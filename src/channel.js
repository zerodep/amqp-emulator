import { constants } from './defs.js';
import { AmqpError, notFound, preconditionFailed, accessRefused, notImplemented, commandInvalid, unexpectedFrame } from './errors.js';
import { decodeProperties } from './codec.js';
import { generateId, kWireProperties, kConsumerEntry, detachConsumerMessages, wireProperties } from './shared.js';

const SUPPORTED_EXCHANGE_TYPES = new Set(['direct', 'topic', 'fanout']);

/**
 * @typedef {(this: AmqpChannel, fields: import('types').MethodFields, method: import('types').MethodDef, properties?: import('types').MessageProperties, content?: Buffer) => void} MethodHandler
 * @typedef {[import('types').AmqpMethodName, import('types').MethodFields, import('types').MessageProperties, Buffer]} ContentFrames
 */

/**
 * One AMQP channel on a connection, translating methods onto the vhost broker
 */
export class AmqpChannel {
  /** @type {import('types').PendingContent | null} basic.publish waiting for header and body frames */
  #pending = null;
  /** @type {ContentFrames[] | null} deliveries buffered until consume-ok is on the wire */
  #deferred = null;
  #torndown = false;

  /**
   * @param {import('./connection.js').AmqpConnection} connection
   * @param {number} id
   */
  constructor(connection, id) {
    this.connection = connection;
    this.id = id;
    this.vhost = connection.vhost;
    this.broker = this.vhost.broker;
    /** @type {import('types').ChannelState} */
    this.state = 'open';
    /** @type {Map<string, import('types').ConsumerEntry>} consumers by client facing consumer tag */
    this.consumers = new Map();
    /** @type {Map<number, import('smqp').Message>} unacknowledged messages by delivery tag */
    this.unacked = new Map();
    this.deliveryTag = 0;
    /** per consumer prefetch, 0 means unlimited */
    this.prefetchCount = 0;
    /** channel wide prefetch, 0 means unlimited */
    this.globalPrefetch = 0;
    this.confirmMode = false;
    this.publishSeq = 0;
    /** name of the most recently declared queue, used when the client passes an empty queue name */
    this.lastQueue = '';
  }

  /**
   * @param {import('types').AmqpMethodName} methodName
   * @param {import('types').MethodFields} [fields]
   */
  send(methodName, fields) {
    this.connection.sendMethod(this.id, methodName, fields);
  }

  /**
   * @param {import('types').AmqpMethodName} methodName
   * @param {import('types').MethodFields} fields
   * @param {import('types').MessageProperties} properties
   * @param {Buffer} content
   */
  sendContent(methodName, fields, properties, content) {
    this.connection.sendContent(this.id, methodName, fields, properties, content);
  }

  /**
   * @param {import('types').MethodDef} method
   * @param {import('types').MethodFields} fields
   */
  handleMethod(method, fields) {
    if (this.state !== 'open') {
      if (method.name === 'channel.close-ok') this.#remove();
      else if (method.name === 'channel.close') {
        this.send('channel.close-ok', {});
        this.#remove();
      }
      return;
    }

    try {
      if (this.#pending) throw unexpectedFrame(`expected content header for ${this.#pending.method.name}, got ${method.name}`, method);
      if (method.classId === 10) throw commandInvalid(`unexpected method ${method.name} on channel ${this.id}`, method);
      if (method.content) {
        this.#pending = { method, fields, received: 0, chunks: [] };
        return;
      }
      const handler = AmqpChannel.#handlers[method.name];
      if (!handler) throw notImplemented(`method ${method.name} is not implemented`, method);
      handler.call(this, fields, method);
    } catch (err) {
      this.#onError(err, method);
    }
  }

  /** @param {Buffer} payload */
  handleHeader(payload) {
    if (this.state !== 'open') return;
    try {
      if (!this.#pending || this.#pending.properties) throw unexpectedFrame('unexpected content header frame');
      const { bodySize, properties } = decodeProperties(payload);
      this.#pending.properties = properties;
      this.#pending.bodySize = bodySize;
      if (bodySize === 0) this.#completeContent();
    } catch (err) {
      // a header that does not decode is a protocol violation, not a broker error
      this.#onError(err instanceof AmqpError ? err : unexpectedFrame(err.message), this.#pending?.method);
    }
  }

  /** @param {Buffer} payload */
  handleBody(payload) {
    if (this.state !== 'open') return;
    try {
      if (!this.#pending?.properties) throw unexpectedFrame('unexpected content body frame');
      this.#pending.chunks.push(payload);
      this.#pending.received += payload.length;
      if (this.#pending.received >= this.#pending.bodySize) this.#completeContent();
    } catch (err) {
      this.#onError(err, this.#pending?.method);
    }
  }

  /**
   * Server initiated close
   * @param {AmqpError} err
   */
  close(err) {
    if (this.state !== 'open') return;
    this.state = 'closing';
    this.teardown();
    this.send('channel.close', {
      replyCode: err.code,
      replyText: err.replyText,
      classId: err.method?.classId ?? 0,
      methodId: err.method?.methodId ?? 0,
    });
    this.connection.emit('channel.error', err, this);
  }

  /** Release consumers and requeue unacked messages */
  teardown() {
    if (this.#torndown) return;
    this.#torndown = true;
    const consumers = [...this.consumers.values()];
    this.consumers.clear();
    for (const entry of consumers) entry.consumer.cancel(true);
    const unacked = [...this.unacked.values()];
    this.unacked.clear();
    for (const message of unacked) message.nack(false, true);
    this.#deferred = null;
  }

  /**
   * Called by the vhost when a queue with one of this channel's consumers was deleted elsewhere
   * @param {import('types').ConsumerEntry} entry
   */
  onConsumerCancelled(entry) {
    this.consumers.delete(entry.tag);
    if (this.state === 'open') this.send('basic.cancel', { consumerTag: entry.tag, nowait: true });
  }

  /**
   * Called by the vhost when a mandatory publish could not be routed
   * @param {import('smqp').Message} event
   * @param {import('types').Publishing} publishing
   */
  onReturn(event, publishing) {
    this.#return(event.fields.exchange, event.fields.routingKey, publishing.properties, event.content);
  }

  #completeContent() {
    const { method, fields, properties, chunks } = this.#pending;
    this.#pending = null;
    const content = chunks.length === 1 ? Buffer.from(chunks[0]) : Buffer.concat(chunks);
    AmqpChannel.#handlers[method.name].call(this, fields, method, properties, content);
  }

  /**
   * @param {any} err
   * @param {import('types').MethodDef} [method]
   */
  #onError(err, method) {
    let amqpErr = err;
    if (!(err instanceof AmqpError)) {
      // every expected broker condition is checked before calling smqp, anything else is a bug worth surfacing
      amqpErr = new AmqpError(constants.INTERNAL_ERROR, `INTERNAL_ERROR - ${err.message}`, { method });
      this.connection.server.emit('error', err, this.connection);
    }
    if (!amqpErr.method) amqpErr.method = method;
    if (amqpErr.killConnection) return this.connection.closeWithError(amqpErr);
    this.close(amqpErr);
  }

  #remove() {
    this.state = 'closed';
    this.teardown();
    this.connection.channels.delete(this.id);
  }

  /**
   * Re-evaluate consumer capacity after acks when a channel-wide prefetch is set
   */
  #poke() {
    if (!this.globalPrefetch) return;
    for (const { consumer } of this.consumers.values()) {
      // smqp only re-evaluates the queue whose message was acked, other queues consumed on this channel need a nudge
      // eslint-disable-next-line dot-notation
      if (consumer.ready && !consumer.queue.stopped) consumer.queue['_consumeNext']();
    }
  }

  /**
   * Clamp a consumer capacity by the channel-wide prefetch
   * @param {number} consumerCapacity
   */
  #capacity(consumerCapacity) {
    if (!this.globalPrefetch) return consumerCapacity;
    const room = this.globalPrefetch - this.unacked.size;
    if (room <= 0) return 0;
    return Math.min(room, consumerCapacity);
  }

  /**
   * @param {string} queueName empty name means the last declared queue
   * @param {import('types').MethodDef} method
   */
  #getQueue(queueName, method) {
    const name = queueName || this.lastQueue;
    const queue = name && this.broker.getQueue(name);
    if (!queue) throw notFound('queue', name, this.vhost.name, method);
    return queue;
  }

  /**
   * @param {string} queueName
   * @param {import('types').MethodDef} method
   */
  #assertExclusiveOwner(queueName, method) {
    const ownerId = this.vhost.exclusiveQueues.get(queueName);
    if (ownerId !== undefined && ownerId !== this.connection.id) {
      throw new AmqpError(
        constants.RESOURCE_LOCKED,
        `RESOURCE_LOCKED - cannot obtain exclusive access to locked queue '${queueName}' in vhost '${this.vhost.name}'`,
        { method },
      );
    }
  }

  /**
   * @param {string} name
   * @param {import('types').MethodDef} method
   * @param {boolean} existing
   */
  #assertExchangeName(name, method, existing) {
    if (name === '') throw accessRefused('operation not permitted on the default exchange', method);
    if (name.startsWith('amq.') && !existing) throw accessRefused(`exchange name '${name}' contains reserved prefix 'amq.'`, method);
  }

  /**
   * Deliver an smqp message to the client
   * @param {import('types').ConsumerEntry} entry
   * @param {import('smqp').Message} message
   */
  #deliver(entry, message) {
    const deliveryTag = ++this.deliveryTag;
    if (!entry.noAck) this.unacked.set(deliveryTag, message);
    const fields = {
      consumerTag: entry.tag,
      deliveryTag,
      redelivered: !!message.fields.redelivered,
      exchange: message.fields.exchange,
      routingKey: message.fields.routingKey,
    };
    /** @type {ContentFrames} */
    const frames = ['basic.deliver', fields, wireProperties(message.properties), toBuffer(message.content)];
    if (this.#deferred) this.#deferred.push(frames);
    else this.sendContent(...frames);
  }

  /**
   * @param {string} exchange
   * @param {string} routingKey
   * @param {import('types').MessageProperties} properties
   * @param {Buffer} content
   */
  #return(exchange, routingKey, properties, content) {
    this.sendContent('basic.return', { replyCode: constants.NO_ROUTE, replyText: 'NO_ROUTE', exchange, routingKey }, properties, content);
  }

  /**
   * Ack, nack or reject unacked messages
   * @param {number} deliveryTag
   * @param {boolean} multiple settle all up to and including delivery tag
   * @param {import('types').MethodDef} method
   * @param {(message: import('smqp').Message) => void} settle
   */
  #settle(deliveryTag, multiple, method, settle) {
    let messages;
    if (deliveryTag === 0 && multiple) {
      messages = [...this.unacked.values()];
      this.unacked.clear();
    } else {
      if (!this.unacked.has(deliveryTag)) throw preconditionFailed(`unknown delivery tag ${deliveryTag}`, method);
      messages = [];
      for (const [tag, message] of [...this.unacked]) {
        if (tag > deliveryTag) continue;
        if (!multiple && tag !== deliveryTag) continue;
        this.unacked.delete(tag);
        messages.push(message);
      }
    }
    for (const message of messages) settle(message);
    this.#poke();
  }

  /** @type {Record<string, MethodHandler>} AMQP method handlers, invoked with the channel as this */
  static #handlers = {
    'channel.open'(_fields, method) {
      throw new AmqpError(constants.CHANNEL_ERROR, `CHANNEL_ERROR - second 'channel.open' seen`, { method, connection: true });
    },
    'channel.flow'({ active }) {
      this.send('channel.flow-ok', { active });
    },
    'channel.close'() {
      this.state = 'closed';
      this.teardown();
      this.send('channel.close-ok', {});
      this.connection.channels.delete(this.id);
    },
    'access.request'() {
      this.send('access.request-ok', { ticket: 1 });
    },

    'exchange.declare'(fields, method) {
      const { exchange, type, passive, durable, autoDelete, internal, nowait } = fields;
      const vhost = this.vhost;
      const existingType = vhost.getExchangeType(exchange);
      this.#assertExchangeName(exchange, method, !!existingType);

      if (existingType) {
        if (!passive) {
          if (type !== existingType) {
            throw preconditionFailed(
              `inequivalent arg 'type' for exchange '${exchange}' in vhost '${vhost.name}': received '${type}' but current is '${existingType}'`,
              method,
            );
          }
          const current = this.broker.getExchange(exchange).options;
          if (!!current.durable !== !!durable) {
            throw preconditionFailed(
              `inequivalent arg 'durable' for exchange '${exchange}' in vhost '${vhost.name}': received '${!!durable}' but current is '${!!current.durable}'`,
              method,
            );
          }
        }
      } else {
        if (passive) throw notFound('exchange', exchange, vhost.name, method);
        if (!SUPPORTED_EXCHANGE_TYPES.has(type)) {
          if (type === 'headers') throw notImplemented(`exchange type 'headers' is not supported by the emulator`, method);
          throw commandInvalid(`unknown exchange type '${type}'`, method);
        }
        vhost.declareExchange(exchange, type, { durable: !!durable, autoDelete: !!autoDelete, internal: !!internal });
      }
      if (!nowait) this.send('exchange.declare-ok', {});
    },
    'exchange.delete'({ exchange, ifUnused, nowait }, method) {
      this.#assertExchangeName(exchange, method, true);
      if (exchange.startsWith('amq.')) throw accessRefused(`exchange name '${exchange}' contains reserved prefix 'amq.'`, method);
      const existing = this.broker.getExchange(exchange);
      if (existing) {
        if (ifUnused && existing.bindingCount)
          throw preconditionFailed(`exchange '${exchange}' in vhost '${this.vhost.name}' in use`, method);
        this.broker.deleteExchange(exchange);
        this.vhost.exchangeTypes.delete(exchange);
      }
      if (!nowait) this.send('exchange.delete-ok', {});
    },
    'exchange.bind'({ destination, source, routingKey, nowait, arguments: args }, method) {
      const vhost = this.vhost;
      if (!vhost.getExchangeType(source)) throw notFound('exchange', source, vhost.name, method);
      if (!vhost.getExchangeType(destination)) throw notFound('exchange', destination, vhost.name, method);
      this.#assertExchangeName(source, method, true);
      this.#assertExchangeName(destination, method, true);
      try {
        this.broker.bindExchange(source, destination, vhost.bindingPattern(source, routingKey), args);
      } catch (/** @type {any} */ err) {
        if (err.code !== 'ERR_SMQP_SHOVEL_NAME_CONFLICT') throw err;
      }
      if (!nowait) this.send('exchange.bind-ok', {});
    },
    'exchange.unbind'({ destination, source, routingKey, nowait }, method) {
      const vhost = this.vhost;
      if (!vhost.getExchangeType(source)) throw notFound('exchange', source, vhost.name, method);
      if (!vhost.getExchangeType(destination)) throw notFound('exchange', destination, vhost.name, method);
      this.broker.unbindExchange(source, destination, vhost.bindingPattern(source, routingKey));
      if (!nowait) this.send('exchange.unbind-ok', {});
    },

    'queue.declare'(fields, method) {
      const { passive, durable, exclusive, autoDelete, nowait, arguments: args = {} } = fields;
      const vhost = this.vhost;
      /** @type {string} */
      let queueName = fields.queue;
      let queue = queueName ? this.broker.getQueue(queueName) : undefined;

      if (queueName.startsWith('amq.') && !queue) throw accessRefused(`queue name '${queueName}' contains reserved prefix 'amq.'`, method);

      if (queue) {
        this.#assertExclusiveOwner(queueName, method);
        queue.evictExpired();
        if (!passive) {
          const current = queue.options;
          for (const [arg, received, existing] of [
            ['durable', !!durable, !!current.durable],
            ['auto_delete', !!autoDelete, !!current.autoDelete],
            ['exclusive', !!exclusive, vhost.exclusiveQueues.has(queueName)],
          ]) {
            if (received !== existing) {
              throw preconditionFailed(
                `inequivalent arg '${arg}' for queue '${queueName}' in vhost '${vhost.name}': received '${received}' but current is '${existing}'`,
                method,
              );
            }
          }
        }
      } else {
        if (passive) throw notFound('queue', queueName, vhost.name, method);
        if (!queueName) queueName = `amq.gen-${generateId()}`;
        /** @type {import('smqp').QueueOptions} */
        const options = {
          durable: !!durable,
          autoDelete: !!autoDelete,
          exclusive: !!exclusive,
          arguments: args,
        };
        if (args['x-message-ttl'] !== undefined) options.messageTtl = args['x-message-ttl'];
        if (args['x-max-length'] !== undefined) options.maxLength = args['x-max-length'];
        if (args['x-dead-letter-exchange'] !== undefined) options.deadLetterExchange = args['x-dead-letter-exchange'];
        if (args['x-dead-letter-routing-key'] !== undefined) options.deadLetterRoutingKey = args['x-dead-letter-routing-key'];
        queue = this.broker.assertQueue(queueName, options);
        if (exclusive) vhost.exclusiveQueues.set(queueName, this.connection.id);
      }

      this.lastQueue = queueName;
      if (!nowait) {
        this.send('queue.declare-ok', {
          queue: queueName,
          messageCount: readyCount(queue),
          consumerCount: queue.consumerCount,
        });
      }
    },
    'queue.bind'({ queue: queueName, exchange, routingKey, nowait, arguments: args }, method) {
      const vhost = this.vhost;
      const queue = this.#getQueue(queueName, method);
      this.#assertExclusiveOwner(queue.name, method);
      if (exchange === '') throw accessRefused('operation not permitted on the default exchange', method);
      if (!vhost.getExchangeType(exchange)) throw notFound('exchange', exchange, vhost.name, method);
      this.broker.bindQueue(queue.name, exchange, vhost.bindingPattern(exchange, routingKey), { arguments: args });
      if (!nowait) this.send('queue.bind-ok', {});
    },
    'queue.unbind'({ queue: queueName, exchange, routingKey }, method) {
      const vhost = this.vhost;
      const queue = this.#getQueue(queueName, method);
      if (!vhost.getExchangeType(exchange)) throw notFound('exchange', exchange, vhost.name, method);
      this.broker.unbindQueue(queue.name, exchange, vhost.bindingPattern(exchange, routingKey));
      this.send('queue.unbind-ok', {});
    },
    'queue.purge'({ queue: queueName, nowait }, method) {
      const queue = this.#getQueue(queueName, method);
      this.#assertExclusiveOwner(queue.name, method);
      const messageCount = queue.purge();
      if (!nowait) this.send('queue.purge-ok', { messageCount });
    },
    'queue.delete'({ queue: queueName, ifUnused, ifEmpty, nowait }, method) {
      const vhost = this.vhost;
      const name = queueName || this.lastQueue;
      const queue = name && this.broker.getQueue(name);
      let messageCount = 0;
      if (queue) {
        this.#assertExclusiveOwner(name, method);
        if (ifUnused && queue.consumerCount) throw preconditionFailed(`queue '${name}' in vhost '${vhost.name}' in use`, method);
        if (ifEmpty && queue.messageCount) throw preconditionFailed(`queue '${name}' in vhost '${vhost.name}' not empty`, method);
        messageCount = vhost.deleteQueue(name).messageCount;
      }
      if (!nowait) this.send('queue.delete-ok', { messageCount });
    },

    'basic.qos'({ prefetchCount, global }) {
      if (global) this.globalPrefetch = prefetchCount;
      else this.prefetchCount = prefetchCount;
      this.send('basic.qos-ok', {});
      this.#poke();
    },
    'basic.consume'(fields, method) {
      const vhost = this.vhost;
      const queue = this.#getQueue(fields.queue, method);
      const queueName = queue.name;
      this.#assertExclusiveOwner(queueName, method);

      const tag = fields.consumerTag || `amq.ctag-${generateId()}`;
      if (this.consumers.has(tag)) {
        throw new AmqpError(constants.NOT_ALLOWED, `NOT_ALLOWED - attempt to reuse consumer tag '${tag}'`, { method });
      }
      if (queue.exclusive || (fields.exclusive && queue.consumerCount)) {
        throw accessRefused(`queue '${queueName}' in vhost '${vhost.name}' in exclusive use`, method);
      }

      /** @type {import('types').ConsumerEntry} */
      const entry = { tag, noAck: !!fields.noAck, channel: this, consumer: null };
      const prefetch = this.prefetchCount || Infinity;
      this.#deferred = [];
      try {
        // smqp grabs messages while creating the consumer, so the initial prefetch must respect channel-wide capacity
        entry.consumer = queue.consume((_routingKey, message) => this.#deliver(entry, message), {
          consumerTag: `${this.connection.id}.${this.id}.${tag}`,
          noAck: entry.noAck,
          exclusive: !!fields.exclusive,
          prefetch: this.#capacity(prefetch),
          priority: fields.arguments?.['x-priority'] || 0,
        });
      } catch (err) {
        this.#deferred = null;
        throw err;
      }

      const consumer = /** @type {any} */ (entry.consumer);
      consumer.prefetch(prefetch);
      consumer[kConsumerEntry] = entry;
      const capacityProp = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(consumer), 'capacity');
      Object.defineProperty(consumer, 'capacity', {
        get: () => this.#capacity(capacityProp.get.call(consumer)),
      });
      this.consumers.set(tag, entry);

      if (!fields.nowait) this.send('basic.consume-ok', { consumerTag: tag });
      const deferred = this.#deferred;
      this.#deferred = null;
      for (const frames of deferred) this.sendContent(...frames);
      this.#poke();
    },
    'basic.cancel'({ consumerTag, nowait }) {
      const entry = this.consumers.get(consumerTag);
      if (entry) {
        this.consumers.delete(consumerTag);
        detachConsumerMessages(entry.consumer);
        entry.consumer.cancel(true);
      }
      if (!nowait) this.send('basic.cancel-ok', { consumerTag });
    },
    'basic.publish'(fields, method, properties, content) {
      if (fields.immediate) throw notImplemented('immediate=true', method);
      const { exchange, routingKey, mandatory } = fields;
      const broker = this.broker;
      const vhost = this.vhost;

      /** @type {any} amqp expiration is a string, smqp wants a number and parses it */
      const smqpProperties = {
        ...properties,
        [kWireProperties]: properties,
        mandatory: !!mandatory,
        timestamp: Date.now(),
      };
      if (properties.messageId === undefined) delete smqpProperties.messageId;

      if (exchange === '') {
        const queue = routingKey && broker.getQueue(routingKey);
        if (queue) queue.queueMessage({ routingKey, exchange: '' }, content, smqpProperties);
        else if (mandatory) this.#return(exchange, routingKey, properties, content);
      } else {
        if (!vhost.getExchangeType(exchange)) throw notFound('exchange', exchange, vhost.name, method);
        vhost.publishing = { channel: this, properties };
        try {
          broker.publish(exchange, routingKey, content, smqpProperties);
        } finally {
          vhost.publishing = null;
        }
      }

      if (this.confirmMode) this.send('basic.ack', { deliveryTag: ++this.publishSeq, multiple: false });
    },
    'basic.get'({ queue: queueName, noAck }, method) {
      const queue = this.#getQueue(queueName, method);
      this.#assertExclusiveOwner(queue.name, method);
      const message = queue.get({ noAck });
      if (!message) return this.send('basic.get-empty', {});
      const deliveryTag = ++this.deliveryTag;
      if (!noAck) this.unacked.set(deliveryTag, message);
      this.sendContent(
        'basic.get-ok',
        {
          deliveryTag,
          redelivered: !!message.fields.redelivered,
          exchange: message.fields.exchange,
          routingKey: message.fields.routingKey,
          messageCount: readyCount(queue),
        },
        wireProperties(message.properties),
        toBuffer(message.content),
      );
    },
    'basic.ack'({ deliveryTag, multiple }, method) {
      this.#settle(deliveryTag, multiple, method, (message) => message.ack());
    },
    'basic.nack'({ deliveryTag, multiple, requeue }, method) {
      this.#settle(deliveryTag, multiple, method, (message) => message.nack(false, requeue));
    },
    'basic.reject'({ deliveryTag, requeue }, method) {
      this.#settle(deliveryTag, false, method, (message) => message.nack(false, requeue));
    },
    'basic.recover'({ requeue }, method) {
      if (!requeue) throw notImplemented('requeue=false', method);
      const unacked = [...this.unacked.values()];
      this.unacked.clear();
      for (const message of unacked) message.nack(false, true);
      this.send('basic.recover-ok', {});
    },
    'basic.recover-async'(_fields, method) {
      throw notImplemented('basic.recover-async', method);
    },

    'confirm.select'({ nowait }) {
      this.confirmMode = true;
      if (!nowait) this.send('confirm.select-ok', {});
    },
    'tx.select'() {
      this.send('tx.select-ok', {});
    },
    'tx.commit'() {
      this.send('tx.commit-ok', {});
    },
    'tx.rollback'() {
      this.send('tx.rollback-ok', {});
    },
  };
}

/**
 * Message content as a buffer; messages seeded through the smqp broker api may carry strings or objects
 * @param {any} content
 */
function toBuffer(content) {
  if (Buffer.isBuffer(content)) return content;
  if (content === undefined || content === null) return Buffer.alloc(0);
  return Buffer.from(typeof content === 'string' ? content : JSON.stringify(content));
}

/**
 * Messages ready for delivery, i.e. not pending acknowledgement
 * @param {import('smqp').Queue} queue
 */
function readyCount(queue) {
  const { messageCount, unackedCount } = queue.getStats();
  return messageCount - unackedCount;
}
