import { randomBytes } from 'node:crypto';

import { BufferWriter } from '../amqp09/codec.js';
import { kWireProperties, wireProperties } from '../shared.js';
import { encodeValue, typed } from '../amqp10/types.js';
import { decodeMessage, encodeMessage, kSections, toSmqp } from '../amqp10/message.js';

/** Symbol under which Service Bus message state (sequence number, delivery count) is kept on smqp message properties */
export const kServiceBus = Symbol.for('amqp-emulator service bus state');

/** message-format of an AMQP batch: the data sections are individually encoded messages */
const BATCH_FORMAT = 0x80013700;
const LOCK_LOST = 'com.microsoft:message-lock-lost';
const NOT_FOUND = 'com.microsoft:message-not-found';

/**
 * Error answered on a management request
 */
export class ServiceBusError extends Error {
  /**
   * @param {number} statusCode
   * @param {string} condition
   * @param {string} description
   */
  constructor(statusCode, condition, description) {
    super(description);
    this.name = 'ServiceBusError';
    this.statusCode = statusCode;
    this.condition = condition;
  }
}

/**
 * @param {Buffer} bytes
 */
export function uuidToString(bytes) {
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Service Bus delivery tags carry the lock token GUID in .NET byte order, i.e. the first three groups are little endian
 * @param {Buffer} bytes
 */
export function reorderUuid(bytes) {
  return Buffer.from([bytes[3], bytes[2], bytes[1], bytes[0], bytes[5], bytes[4], bytes[7], bytes[6], ...bytes.subarray(8)]);
}

/**
 * @typedef {{ sequenceNumber: number, enqueuedTime: number, deliveryCount: number, deadLetterSource?: string, scheduledEnqueueTime?: number }} MessageState
 * @typedef {{ key: string, token: Buffer, message: import('smqp').Message, entity: string, link: import('../amqp10/link.js').Link | null, deliveryId: number | null, expires: number }} Lock
 * @typedef {{ kind: 'cbs' } | { kind: 'management', entity: string } | { kind: 'topic', entity: string } | { kind: 'queue', entity: string, queue: string }} ServiceBusAddress
 * @typedef {{ statusCode: number, statusDescription: string, condition?: string, body?: Record<string, any> }} ManagementResult
 */

/**
 * Azure Service Bus semantics on top of a vhost: entities, peek lock, deferral, dead lettering, scheduling and the
 * `$cbs` and `$management` request response operations the SDK uses
 */
export class ServiceBus {
  /**
   * @param {import('../vhost.js').VHost} vhost
   * @param {import('../server.js').AmqpServer} server
   */
  constructor(vhost, server) {
    this.vhost = vhost;
    this.server = server;
    this.broker = vhost.broker;
    this.sequence = 0;
    /** @type {Map<string, Lock>} active locks by lock token string */
    this.locks = new Map();
    /** @type {Map<string, Map<number, import('smqp').Message>>} deferred messages by entity and sequence number */
    this.deferred = new Map();
    /** @type {Map<number, { entity: string, content: Buffer, properties: any, at: number }>} scheduled messages by sequence number */
    this.scheduled = new Map();
  }

  get lockDuration() {
    return this.server.options.lockDuration;
  }

  /**
   * Create a queue entity
   * @param {string} name
   */
  createQueue(name) {
    if (!this.broker.getQueue(name)) this.broker.assertQueue(name, { durable: true, autoDelete: false });
    return name;
  }

  /**
   * Create a topic entity, a fanout exchange
   * @param {string} name
   */
  createTopic(name) {
    if (!this.vhost.getExchangeType(name)) this.vhost.declareExchange(name, 'fanout', { durable: true, autoDelete: false });
    return name;
  }

  /**
   * Create a subscription, a queue bound to the topic, addressed as `topic/Subscriptions/name`
   * @param {string} topic
   * @param {string} name
   */
  createSubscription(topic, name) {
    this.createTopic(topic);
    const queue = `${topic}/Subscriptions/${name}`;
    if (!this.broker.getQueue(queue)) {
      this.broker.assertQueue(queue, { durable: true, autoDelete: false });
      this.broker.bindQueue(queue, topic, '#');
    }
    return queue;
  }

  /**
   * Resolve a Service Bus address, creating queues and subscriptions on first use
   * @param {string} address
   * @returns {ServiceBusAddress}
   */
  resolve(address) {
    const path = String(address).replace(/^\//, '');
    if (path === '$cbs') return { kind: 'cbs' };
    if (path === '$management') return { kind: 'management', entity: '' };
    let match;
    if ((match = /^(.+)\/\$management$/i.exec(path))) return { kind: 'management', entity: match[1] };
    if (/\/\$DeadLetterQueue$/i.test(path)) return { kind: 'queue', entity: path, queue: this.createQueue(path) };
    if ((match = /^([^/]+)\/Subscriptions\/([^/]+)$/i.exec(path))) {
      return { kind: 'queue', entity: path, queue: this.createSubscription(match[1], match[2]) };
    }
    if (this.vhost.getExchangeType(path)) return { kind: 'topic', entity: path };
    return { kind: 'queue', entity: path, queue: this.createQueue(path) };
  }

  /**
   * Unpack batches and give every message an id, like the service does
   * @param {import('types').Amqp10Message} message
   * @param {number} messageFormat
   * @returns {import('types').Amqp10Message[]}
   */
  prepare(message, messageFormat) {
    const messages = messageFormat === BATCH_FORMAT ? message.body.map((section) => decodeMessage(section.value)) : [message];
    for (const each of messages) {
      each.properties ??= {};
      each.properties.messageId ??= uuidToString(randomBytes(16));
    }
    return messages;
  }

  /**
   * Service Bus state of a message, assigned on first contact
   * @param {import('smqp').Message} message
   * @returns {MessageState}
   */
  stateOf(message) {
    const properties = /** @type {any} */ (message.properties);
    return (properties[kServiceBus] ??= { sequenceNumber: ++this.sequence, enqueuedTime: Date.now(), deliveryCount: 0 });
  }

  /**
   * Encode a message with the Service Bus annotations
   * @param {import('smqp').Message} message
   * @param {number} [lockedUntil]
   */
  encode(message, lockedUntil) {
    const state = this.stateOf(message);
    /** @type {Record<string, any>} */
    const annotations = {
      'x-opt-sequence-number': typed('long', state.sequenceNumber),
      'x-opt-enqueued-time': typed('timestamp', state.enqueuedTime),
    };
    if (lockedUntil !== undefined) annotations['x-opt-locked-until'] = typed('timestamp', lockedUntil);
    if (state.deadLetterSource !== undefined) annotations['x-opt-deadletter-source'] = state.deadLetterSource;
    if (state.scheduledEnqueueTime !== undefined)
      annotations['x-opt-scheduled-enqueue-time'] = typed('timestamp', state.scheduledEnqueueTime);
    return encodeMessage(message, { annotations, deliveryCount: state.deliveryCount });
  }

  /**
   * Lock a message for peek lock delivery
   * @param {import('smqp').Message} message
   * @param {string} entity
   * @param {import('../amqp10/link.js').Link | null} link
   * @param {number | null} deliveryId
   */
  lock(message, entity, link, deliveryId) {
    const token = randomBytes(16);
    /** @type {Lock} */
    const lock = { key: uuidToString(token), token, message, entity, link, deliveryId, expires: Date.now() + this.lockDuration };
    this.locks.set(lock.key, lock);
    return lock;
  }

  /**
   * @param {string} key lock token string
   */
  #lock(key) {
    const lock = this.locks.get(key);
    if (!lock)
      throw new ServiceBusError(
        410,
        LOCK_LOST,
        `The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue. Lock token: ${key}`,
      );
    return lock;
  }

  /**
   * Extend a lock
   * @param {string} key
   * @returns {number} new expiry
   */
  renew(key) {
    const lock = this.#lock(key);
    lock.expires = Date.now() + this.lockDuration;
    return lock.expires;
  }

  /**
   * Settle a locked message
   * @param {string} key lock token string
   * @param {'completed' | 'abandoned' | 'defered' | 'suspended'} status
   * @param {{ deadLetterReason?: string, deadLetterDescription?: string, propertiesToModify?: Record<string, any> }} [options]
   */
  settle(key, status, options = {}) {
    const lock = this.#lock(key);
    this.locks.delete(key);
    lock.link?.forget(lock.deliveryId);
    const { message, entity } = lock;
    const state = this.stateOf(message);
    const deferred = this.#deferred(entity);
    const isDeferred = deferred.get(state.sequenceNumber) === message;
    switch (status) {
      case 'completed':
        if (isDeferred) deferred.delete(state.sequenceNumber);
        else message.ack();
        return;
      case 'abandoned':
        state.deliveryCount++;
        if (!isDeferred) message.nack(false, true);
        return;
      case 'defered':
        if (!isDeferred) {
          message.ack();
          deferred.set(state.sequenceNumber, message);
        }
        return;
      case 'suspended':
        if (isDeferred) deferred.delete(state.sequenceNumber);
        else message.ack();
        this.deadLetter(message, entity, options);
        return;
      default:
        throw new ServiceBusError(400, 'amqp:not-allowed', `unknown disposition status ${status}`);
    }
  }

  /**
   * Keep locks alive when the link that delivered them goes away; the SDK settles them through $management
   * @param {import('../amqp10/link.js').Link} link
   */
  detachLink(link) {
    for (const lock of this.locks.values()) {
      if (lock.link === link) lock.link = null;
    }
  }

  /**
   * Copy a message into the entity's dead letter queue
   * @param {import('smqp').Message} message
   * @param {string} entity
   * @param {{ deadLetterReason?: string, deadLetterDescription?: string, propertiesToModify?: Record<string, any> }} options
   */
  deadLetter(message, entity, options) {
    const dlq = this.createQueue(`${entity}/$DeadLetterQueue`);
    const extra = { ...options.propertiesToModify };
    if (options.deadLetterReason !== undefined) extra.DeadLetterReason = options.deadLetterReason;
    if (options.deadLetterDescription !== undefined) extra.DeadLetterErrorDescription = options.deadLetterDescription;

    const { expiration, ttl, ...rest } = /** @type {any} */ (message.properties);
    /** @type {any} */
    const properties = { ...rest };
    /** @type {MessageState} */
    properties[kServiceBus] = { sequenceNumber: ++this.sequence, enqueuedTime: Date.now(), deliveryCount: 0, deadLetterSource: entity };
    /** @type {import('types').Amqp10Message | undefined} */
    const sections = properties[kSections];
    if (sections) {
      properties[kSections] = { ...sections, applicationProperties: { ...sections.applicationProperties, ...extra } };
    } else {
      const wire = wireProperties(message.properties);
      properties[kWireProperties] = { ...wire, headers: { ...wire.headers, ...extra } };
    }
    delete properties[kWireProperties]?.expiration;
    this.broker.getQueue(dlq).queueMessage({ routingKey: dlq, exchange: '' }, message.content, properties);
  }

  /**
   * @param {string} entity
   */
  #deferred(entity) {
    let map = this.deferred.get(entity);
    if (!map) this.deferred.set(entity, (map = new Map()));
    return map;
  }

  /**
   * Release expired locks and enqueue due scheduled messages; called by the vhost sweeper
   * @param {number} [now]
   */
  sweep(now = Date.now()) {
    for (const lock of [...this.locks.values()]) {
      if (lock.expires > now) continue;
      // the link keeps its unsettled entry so a late disposition is answered with lock lost
      this.locks.delete(lock.key);
      this.stateOf(lock.message).deliveryCount++;
      lock.message.nack(false, true);
    }
    for (const [sequenceNumber, scheduled] of [...this.scheduled]) {
      if (scheduled.at > now) continue;
      this.scheduled.delete(sequenceNumber);
      this.#enqueue(scheduled.entity, scheduled.content, scheduled.properties);
    }
  }

  /**
   * Enqueue into a queue entity or publish to a topic
   * @param {string} entity
   * @param {Buffer} content
   * @param {any} properties
   */
  #enqueue(entity, content, properties) {
    const resolved = this.resolve(entity);
    if (resolved.kind === 'topic') return this.broker.publish(entity, '', content, properties);
    const queue = this.broker.getQueue(/** @type {{ queue: string }} */ (resolved).queue);
    queue.queueMessage({ routingKey: entity, exchange: '' }, content, properties);
  }

  /**
   * Messages visible to peek: everything in the queue plus deferred ones, ordered by sequence number
   * @param {string} entity
   * @param {number} from
   * @param {number} count
   */
  peek(entity, from, count) {
    const queue = this.broker.getQueue(entity);
    const candidates = [...(queue?.messages ?? []), ...this.#deferred(entity).values()];
    return candidates
      .map((message) => ({ message, state: this.stateOf(message) }))
      .filter(({ state }) => state.sequenceNumber >= from)
      .sort((a, b) => a.state.sequenceNumber - b.state.sequenceNumber)
      .slice(0, count)
      .map(({ message }) => message);
  }

  /**
   * Handle a `$cbs` or `$management` request and build the response message bytes
   * @param {'cbs' | 'management'} kind
   * @param {string} entity
   * @param {import('types').Amqp10Message} request
   * @returns {Buffer}
   */
  handleRequest(kind, entity, request) {
    const operation = request.applicationProperties?.operation;
    /** @type {ManagementResult} */
    let result;
    try {
      result = kind === 'cbs' ? this.#cbs(operation) : this.#management(entity, operation, request);
    } catch (err) {
      if (!(err instanceof ServiceBusError)) throw err;
      result = { statusCode: err.statusCode, statusDescription: err.message, condition: err.condition };
    }
    // the SDK reads the dashed keys in its request response layer but the camel cased ones in some operations, so send both
    /** @type {Record<string, any>} */
    const applicationProperties = {
      'status-code': typed('int', result.statusCode),
      statusCode: typed('int', result.statusCode),
      'status-description': result.statusDescription,
      statusDescription: result.statusDescription,
    };
    if (result.condition) {
      applicationProperties['error-condition'] = typed('symbol', result.condition);
      applicationProperties.errorCondition = typed('symbol', result.condition);
    }
    const w = new BufferWriter();
    encodeValue(w, { $type: 'properties', correlationId: request.properties?.messageId }, 'properties');
    encodeValue(w, { $type: 'application-properties', value: applicationProperties });
    if (result.body) encodeValue(w, { $type: 'amqp-value', value: result.body });
    return w.toBuffer();
  }

  /**
   * @param {string} operation
   * @returns {ManagementResult}
   */
  #cbs(operation) {
    if (operation !== 'put-token') throw new ServiceBusError(400, 'amqp:not-implemented', `unsupported $cbs operation ${operation}`);
    return { statusCode: 202, statusDescription: 'Accepted' };
  }

  /**
   * @param {string} entity
   * @param {string} operation
   * @param {import('types').Amqp10Message} request
   * @returns {ManagementResult}
   */
  #management(entity, operation, request) {
    const body = request.body[0]?.value ?? {};
    switch (operation) {
      case 'com.microsoft:peek-message': {
        const messages = this.peek(entity, body['from-sequence-number'] ?? 0, body['message-count'] ?? 1);
        if (!messages.length) return { statusCode: 204, statusDescription: 'No Content' };
        return {
          statusCode: 200,
          statusDescription: 'OK',
          body: { messages: messages.map((message) => ({ message: this.encode(message) })) },
        };
      }
      case 'com.microsoft:renew-lock': {
        const expirations = body['lock-tokens'].map((/** @type {Buffer} */ token) => typed('timestamp', this.renew(uuidToString(token))));
        return { statusCode: 200, statusDescription: 'OK', body: { expirations } };
      }
      case 'com.microsoft:schedule-message': {
        const sequenceNumbers = body.messages.map((/** @type {{ message: Buffer }} */ entry) => this.#schedule(entity, entry.message));
        return {
          statusCode: 200,
          statusDescription: 'OK',
          body: { 'sequence-numbers': sequenceNumbers.map((/** @type {number} */ n) => typed('long', n)) },
        };
      }
      case 'com.microsoft:cancel-scheduled-message':
        for (const sequenceNumber of body['sequence-numbers']) this.scheduled.delete(sequenceNumber);
        return { statusCode: 200, statusDescription: 'OK' };
      case 'com.microsoft:receive-by-sequence-number':
        return this.#receiveDeferred(entity, body['sequence-numbers'], body['receiver-settle-mode'] ?? 1);
      case 'com.microsoft:update-disposition':
        for (const token of body['lock-tokens']) {
          this.settle(uuidToString(token), body['disposition-status'], {
            deadLetterReason: body['deadletter-reason'],
            deadLetterDescription: body['deadletter-description'],
            propertiesToModify: body['properties-to-modify'],
          });
        }
        return { statusCode: 200, statusDescription: 'OK' };
      default:
        throw new ServiceBusError(400, 'amqp:not-implemented', `unsupported management operation ${operation}`);
    }
  }

  /**
   * @param {string} entity
   * @param {Buffer} encoded
   * @returns {number} sequence number
   */
  #schedule(entity, encoded) {
    const decoded = decodeMessage(encoded);
    const { content, properties } = toSmqp(decoded);
    const at = Number(decoded.messageAnnotations?.['x-opt-scheduled-enqueue-time'] ?? Date.now());
    const sequenceNumber = ++this.sequence;
    /** @type {any} */ (properties)[kServiceBus] = { sequenceNumber, enqueuedTime: at, deliveryCount: 0, scheduledEnqueueTime: at };
    this.scheduled.set(sequenceNumber, { entity, content, properties, at });
    return sequenceNumber;
  }

  /**
   * @param {string} entity
   * @param {number[]} sequenceNumbers
   * @param {number} settleMode 0 receive and delete, 1 peek lock
   * @returns {ManagementResult}
   */
  #receiveDeferred(entity, sequenceNumbers, settleMode) {
    const deferred = this.#deferred(entity);
    const messages = [];
    for (const sequenceNumber of sequenceNumbers) {
      const message = deferred.get(sequenceNumber);
      if (!message) continue;
      if (settleMode === 0) {
        deferred.delete(sequenceNumber);
        messages.push({ message: this.encode(message) });
      } else {
        const lock = this.lock(message, entity, null, null);
        messages.push({ message: this.encode(message, lock.expires), 'lock-token': typed('uuid', lock.token) });
      }
    }
    if (!messages.length) throw new ServiceBusError(404, NOT_FOUND, 'The requested message was not found');
    return { statusCode: 200, statusDescription: 'OK', body: { messages } };
  }
}
