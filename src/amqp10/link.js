import { generateId } from '../shared.js';
import { decodeMessage, encodeMessage, toSmqp } from './message.js';
import { reorderUuid } from '../servicebus/servicebus.js';

const INITIAL_CREDIT = 100;
const REFILL_BELOW = INITIAL_CREDIT / 2;

/** @type {Array<[RegExp, 'queue' | 'exchange']>} */
const ADDRESS_PATTERNS = [
  [/^\/?(?:queues?|amq\/queue)\/(.+)$/, 'queue'],
  [/^\/?exchanges?\/([^/]+)(?:\/(.*))?$/, 'exchange'],
];

/**
 * @typedef {{ kind: 'queue', queue: string, entity?: string } | { kind: 'exchange', exchange: string, routingKey: string } | { kind: 'topic', entity: string } | { kind: 'cbs' } | { kind: 'management', entity: string }} ResolvedAddress
 * @typedef {{ message: import('smqp').Message, lockKey?: string }} Unsettled
 */

/**
 * Resolve a plain AMQP 1.0 link address. `/queues/x`, `/queue/x`, `/amq/queue/x` and bare `x` are queues,
 * `/exchanges/x/key` and `/exchange/x/key` are exchanges with an optional routing key.
 * @param {string} address
 * @returns {ResolvedAddress}
 */
export function resolveAddress(address) {
  for (const [pattern, kind] of ADDRESS_PATTERNS) {
    const match = pattern.exec(address);
    if (!match) continue;
    if (kind === 'queue') return { kind, queue: match[1] };
    return { kind, exchange: match[1], routingKey: match[2] ?? '' };
  }
  return { kind: 'queue', queue: address.replace(/^\//, '') };
}

/**
 * Error that detaches a link with an AMQP 1.0 error condition
 */
export class LinkError extends Error {
  /**
   * @param {string} condition e.g. amqp:not-found
   * @param {string} description
   */
  constructor(condition, description) {
    super(`${condition}: ${description}`);
    this.name = 'LinkError';
    this.condition = condition;
    this.description = description;
  }
}

/**
 * One AMQP 1.0 session, multiplexing links on a channel
 */
export class Session {
  /**
   * @param {import('./connection.js').Amqp10Connection} connection
   * @param {number} channel remote channel, reused as our channel
   * @param {Record<string, any>} begin
   */
  constructor(connection, channel, begin) {
    this.connection = connection;
    this.channel = channel;
    this.nextIncomingId = begin.nextOutgoingId;
    this.nextOutgoingId = 1;
    this.incomingWindow = 2048;
    this.outgoingWindow = 2048;
    /** @type {Map<number, Link>} links by remote handle */
    this.links = new Map();
    /** @type {Map<number, Link>} link per unsettled outgoing delivery id */
    this.unsettled = new Map();
    this.send({
      $type: 'begin',
      remoteChannel: channel,
      nextOutgoingId: this.nextOutgoingId,
      incomingWindow: this.incomingWindow,
      outgoingWindow: this.outgoingWindow,
      handleMax: begin.handleMax,
    });
  }

  /**
   * @param {import('types').Amqp10Described} performative
   * @param {Buffer} [payload]
   */
  send(performative, payload) {
    this.connection.send(this.channel, performative, payload);
  }

  /**
   * @param {import('types').Amqp10Frame} frame
   */
  handle(frame) {
    const performative = /** @type {Record<string, any>} */ (frame.performative);
    switch (performative.$type) {
      case 'attach':
        this.links.set(performative.handle, new Link(this, performative));
        return;
      case 'flow': {
        if (performative.handle === undefined) return;
        const link = this.#link(performative.handle);
        return link.onFlow(performative);
      }
      case 'transfer': {
        this.nextIncomingId++;
        const link = this.#link(performative.handle);
        return link.onTransfer(performative, frame.payload);
      }
      case 'disposition':
        return this.#onDisposition(performative);
      default: {
        // detach, the connection only routes attach, flow, transfer, disposition and detach here.
        // A detach for a handle we already detached ourselves is the remote confirming, nothing to do.
        const link = this.links.get(performative.handle);
        if (!link) return;
        this.links.delete(performative.handle);
        return link.onDetach();
      }
    }
  }

  /** @param {number} handle */
  #link(handle) {
    const link = this.links.get(handle);
    if (!link) throw new LinkError('amqp:session:unattached-handle', `no link with handle ${handle}`);
    return link;
  }

  /** @param {Record<string, any>} disposition */
  #onDisposition(disposition) {
    if (!disposition.role) return; // the remote sender settling its own deliveries needs no action
    const last = disposition.last ?? disposition.first;
    for (let id = disposition.first; id <= last; id++) {
      const link = this.unsettled.get(id);
      if (link) link.settle(id, disposition.state, !!disposition.settled);
    }
  }

  nextDeliveryId() {
    return this.nextOutgoingId++;
  }

  /** Release every link */
  teardown() {
    for (const link of this.links.values()) link.teardown();
    this.links.clear();
  }
}

/**
 * One link endpoint. When the remote is a receiver this link sends from a queue, otherwise it receives into the broker.
 * On Service Bus connections queues are peek locked and `$cbs` and `$management` requests are answered.
 */
export class Link {
  /** @type {import('smqp').Consumer | null} */
  #consumer = null;
  /** @type {string | null} server named queue created for an exchange receiver */
  #tempQueue = null;
  /** @type {{ deliveryId: number, settled: boolean, messageFormat: number, chunks: Buffer[] } | null} */
  #incoming = null;
  #tag = 0;
  #closed = false;

  /**
   * @param {Session} session
   * @param {Record<string, any>} attach
   */
  constructor(session, attach) {
    this.session = session;
    this.connection = session.connection;
    this.vhost = session.connection.vhost;
    this.broker = this.vhost.broker;
    this.name = attach.name;
    this.handle = attach.handle;
    /** true when we deliver to the remote receiver */
    this.sending = attach.role === true;
    this.sndSettleMode = attach.sndSettleMode ?? 2;
    this.rcvSettleMode = attach.rcvSettleMode ?? 0;
    this.credit = 0;
    this.deliveryCount = attach.initialDeliveryCount ?? 0;
    /** @type {Map<number, Unsettled>} our unsettled deliveries */
    this.unsettled = new Map();
    /** @type {ResolvedAddress | null} */
    this.address = null;

    const terminus = this.sending ? attach.source : attach.target;
    const address = terminus?.address;
    try {
      this.address = this.#resolve(address, terminus?.dynamic);
    } catch (err) {
      this.#replyAttach(attach, null);
      this.#detach(err instanceof LinkError ? err : new LinkError('amqp:internal-error', /** @type {Error} */ (err).message));
      return;
    }
    const resolved = { ...terminus, $type: this.sending ? 'source' : 'target', address: this.#externalAddress(address) };
    this.#replyAttach(attach, resolved);

    if (this.sending) {
      if (this.address.kind === 'queue' || this.address.kind === 'exchange') this.#consume();
    } else {
      this.credit = INITIAL_CREDIT;
      this.#sendFlow();
    }
  }

  /** Service Bus semantics when the connection speaks that dialect, otherwise null */
  get serviceBus() {
    return this.connection.dialect === 'servicebus' ? this.vhost.serviceBus : null;
  }

  /**
   * The address we report back, differs from the requested one for dynamic and exchange receivers
   * @param {string | undefined} requested
   */
  #externalAddress(requested) {
    if (this.#tempQueue) return this.#tempQueue;
    const address = /** @type {ResolvedAddress} */ (this.address);
    if (address.kind === 'exchange') return `/exchanges/${address.exchange}/${address.routingKey}`;
    return requested;
  }

  /**
   * @param {string | undefined} address
   * @param {boolean} [dynamic]
   * @returns {ResolvedAddress}
   */
  #resolve(address, dynamic) {
    if (dynamic || address === undefined || address === null) {
      if (!this.sending) throw new LinkError('amqp:not-found', 'a target address is required');
      this.#tempQueue = `amq.gen-${generateId()}`;
      this.broker.assertQueue(this.#tempQueue, { durable: false, autoDelete: true });
      return { kind: 'queue', queue: this.#tempQueue };
    }
    const path = String(address);
    if (/^\/?\$cbs$/.test(path) || /\$management$/.test(path)) this.connection.dialect = 'servicebus';
    const serviceBus = this.serviceBus;
    if (serviceBus) {
      const resolved = serviceBus.resolve(path);
      if (resolved.kind === 'topic' && this.sending) {
        throw new LinkError('amqp:not-found', `topic '${resolved.entity}' can only be received through a subscription`);
      }
      return resolved;
    }
    const resolved = resolveAddress(path);
    if (resolved.kind === 'queue') {
      if (!this.broker.getQueue(resolved.queue)) this.broker.assertQueue(resolved.queue, { durable: true, autoDelete: false });
      return resolved;
    }
    const { exchange, routingKey } = /** @type {{ exchange: string, routingKey: string }} */ (resolved);
    if (!this.vhost.ensureExchange(exchange)) {
      throw new LinkError('amqp:not-found', `no exchange '${exchange}' in vhost '${this.vhost.name}'`);
    }
    if (this.sending) {
      this.#tempQueue = `amq.gen-${generateId()}`;
      this.broker.assertQueue(this.#tempQueue, { durable: false, autoDelete: true });
      this.broker.bindQueue(this.#tempQueue, exchange, routingKey);
    }
    return resolved;
  }

  /**
   * @param {Record<string, any>} attach
   * @param {Record<string, any> | null} terminus
   */
  #replyAttach(attach, terminus) {
    this.session.send({
      $type: 'attach',
      name: attach.name,
      handle: this.handle,
      role: !attach.role,
      sndSettleMode: attach.sndSettleMode,
      rcvSettleMode: attach.rcvSettleMode,
      source: this.sending ? terminus : attach.source,
      target: this.sending ? attach.target : terminus,
      initialDeliveryCount: this.sending ? 0 : undefined,
      maxMessageSize: attach.maxMessageSize,
    });
  }

  /** @param {LinkError} err */
  #detach(err) {
    this.teardown();
    this.session.links.delete(this.handle);
    this.session.send({
      $type: 'detach',
      handle: this.handle,
      closed: true,
      error: { $type: 'error', condition: err.condition, description: err.description },
    });
  }

  #consume() {
    const queueName = this.#tempQueue ?? /** @type {{ queue: string }} */ (this.address).queue;
    const queue = this.broker.getQueue(queueName);
    /** @type {import('types').ConsumerEntry} the consumer owner, so vhost.deleteQueue can tell us */
    const entry = {
      tag: this.name,
      noAck: false,
      consumer: null,
      channel: { onConsumerCancelled: () => this.#detach(new LinkError('amqp:resource-deleted', `queue '${queueName}' was deleted`)) },
    };
    // credit starts at zero, so grab nothing while attaching and let flow frames open the tap
    entry.consumer = this.#consumer = queue.consume(
      (_routingKey, message) => this.#deliver(message),
      {
        consumerTag: `amqp10.${this.connection.id}.${this.session.channel}.${this.handle}`,
        prefetch: Infinity,
        capacity: () => this.credit,
      },
      entry,
    );
  }

  /** @param {boolean} [drain] echo the drain flag when answering a drain */
  #sendFlow(drain) {
    const session = this.session;
    session.send({
      $type: 'flow',
      nextIncomingId: session.nextIncomingId,
      incomingWindow: session.incomingWindow,
      nextOutgoingId: session.nextOutgoingId,
      outgoingWindow: session.outgoingWindow,
      handle: this.handle,
      deliveryCount: this.deliveryCount,
      linkCredit: this.credit,
      drain: drain || undefined,
    });
  }

  /** @param {Record<string, any>} flow */
  onFlow(flow) {
    if (!this.sending) return;
    const remoteDeliveryCount = flow.deliveryCount ?? this.deliveryCount;
    this.credit = Math.max(0, remoteDeliveryCount + (flow.linkCredit ?? 0) - this.deliveryCount);
    if (this.#consumer && this.credit) this.#consumer.queue.consumeNext();
    if (flow.drain) {
      this.deliveryCount += this.credit;
      this.credit = 0;
      this.#sendFlow(true);
    } else if (flow.echo) {
      this.#sendFlow();
    }
  }

  /**
   * @param {Record<string, any>} transfer
   * @param {Buffer} payload
   */
  onTransfer(transfer, payload) {
    if (this.sending) throw new LinkError('amqp:not-allowed', 'transfer on a sending link');
    if (!this.#incoming) {
      this.#incoming = {
        deliveryId: transfer.deliveryId,
        settled: !!transfer.settled,
        messageFormat: transfer.messageFormat ?? 0,
        chunks: [],
      };
    }
    this.#incoming.chunks.push(payload);
    if (transfer.more) return;

    const { deliveryId, settled, messageFormat, chunks } = this.#incoming;
    this.#incoming = null;
    /** @type {import('types').Amqp10Message[]} */
    let messages;
    try {
      const message = decodeMessage(chunks.length === 1 ? chunks[0] : Buffer.concat(chunks));
      messages = this.serviceBus ? this.serviceBus.prepare(message, messageFormat) : [message];
    } catch (err) {
      throw new LinkError('amqp:decode-error', /** @type {Error} */ (err).message);
    }
    let state;
    for (const message of messages) state = this.#accept(message);
    if (!settled) this.session.send({ $type: 'disposition', role: true, first: deliveryId, settled: true, state });

    this.deliveryCount++;
    if (--this.credit < REFILL_BELOW) {
      this.credit = INITIAL_CREDIT;
      this.#sendFlow();
    }
  }

  /**
   * Route an incoming message and report the outcome
   * @param {import('types').Amqp10Message} message
   * @returns {import('types').Amqp10Described}
   */
  #accept(message) {
    const address = /** @type {ResolvedAddress} */ (this.address);
    switch (address.kind) {
      case 'cbs':
      case 'management': {
        const entity = address.kind === 'management' ? address.entity : '';
        const response = this.vhost.serviceBus.handleRequest(address.kind, entity, message);
        const replyLink = this.connection.findLink(message.properties?.replyTo);
        if (replyLink) replyLink.transfer(response, { settled: true });
        return { $type: 'accepted' };
      }
      case 'topic': {
        const { content, properties } = toSmqp(message);
        this.broker.publish(address.entity, '', content, properties);
        return { $type: 'accepted' };
      }
      case 'queue': {
        const { content, properties } = toSmqp(message);
        const queue = this.broker.getQueue(address.queue) ?? this.broker.assertQueue(address.queue, { durable: true, autoDelete: false });
        queue.queueMessage({ routingKey: address.queue, exchange: '' }, content, properties);
        return { $type: 'accepted' };
      }
      default: {
        if (!this.vhost.ensureExchange(address.exchange)) {
          return {
            $type: 'rejected',
            error: { $type: 'error', condition: 'amqp:not-found', description: `no exchange '${address.exchange}'` },
          };
        }
        const { content, properties } = toSmqp(message);
        this.broker.publish(address.exchange, address.routingKey, content, properties);
        return { $type: 'accepted' };
      }
    }
  }

  /**
   * Send a message payload to the remote receiver, splitting it by the negotiated frame size
   * @param {Buffer} payload encoded message sections
   * @param {{ settled: boolean, tag?: Buffer }} options
   * @returns {number} delivery id
   */
  transfer(payload, { settled, tag }) {
    const session = this.session;
    const deliveryId = session.nextDeliveryId();
    let deliveryTag = tag;
    if (!deliveryTag) {
      deliveryTag = Buffer.alloc(4);
      deliveryTag.writeUInt32BE(++this.#tag);
    }
    const maxBody = this.connection.maxFrameSize - 64;
    // an encoded message always has at least one section, so there is always one frame
    for (let offset = 0; offset < payload.length; offset += maxBody) {
      const chunk = payload.subarray(offset, offset + maxBody);
      const more = offset + maxBody < payload.length;
      session.send({ $type: 'transfer', handle: this.handle, deliveryId, deliveryTag, messageFormat: 0, settled, more }, chunk);
    }
    this.credit--;
    this.deliveryCount++;
    return deliveryId;
  }

  /**
   * Deliver an smqp message to the remote receiver
   * @param {import('smqp').Message} message
   */
  #deliver(message) {
    const settled = this.sndSettleMode === 1;
    const serviceBus = this.serviceBus;
    if (!serviceBus) {
      const deliveryId = this.transfer(encodeMessage(message), { settled });
      if (settled) return message.ack();
      return this.#hold(deliveryId, { message });
    }
    if (settled) {
      this.transfer(serviceBus.encode(message), { settled });
      return message.ack();
    }
    const entity = /** @type {{ entity: string }} */ (this.address).entity;
    const lock = serviceBus.lock(message, entity, this, this.session.nextOutgoingId);
    const deliveryId = this.transfer(serviceBus.encode(message, lock.expires), { settled, tag: reorderUuid(lock.token) });
    this.#hold(deliveryId, { message, lockKey: lock.key });
  }

  /**
   * @param {number} deliveryId
   * @param {Unsettled} entry
   */
  #hold(deliveryId, entry) {
    this.unsettled.set(deliveryId, entry);
    this.session.unsettled.set(deliveryId, this);
  }

  /**
   * Drop an unsettled delivery without touching the message
   * @param {number} deliveryId
   */
  forget(deliveryId) {
    this.unsettled.delete(deliveryId);
    this.session.unsettled.delete(deliveryId);
  }

  /**
   * Apply a disposition from the remote receiver
   * @param {number} deliveryId
   * @param {Record<string, any> | undefined} state
   * @param {boolean} remoteSettled
   */
  settle(deliveryId, state, remoteSettled) {
    const entry = /** @type {Unsettled} */ (this.unsettled.get(deliveryId));
    this.forget(deliveryId);
    const outcome = state?.$type ?? 'accepted';
    let reply = state;
    if (entry.lockKey) reply = this.#settleServiceBus(entry.lockKey, outcome, state);
    else this.#settlePlain(entry.message, outcome, state);
    if (!remoteSettled) this.session.send({ $type: 'disposition', role: false, first: deliveryId, settled: true, state: reply });
  }

  /**
   * @param {import('smqp').Message} message
   * @param {string} outcome
   * @param {Record<string, any> | undefined} state
   */
  #settlePlain(message, outcome, state) {
    switch (outcome) {
      case 'released':
        return message.nack(false, true);
      case 'modified':
        return message.nack(false, !state.undeliverableHere);
      case 'rejected':
        return message.nack(false, false);
      default:
        return message.ack();
    }
  }

  /**
   * Service Bus reads dispositions as complete, abandon, defer and dead letter. Every successful settlement is
   * confirmed with accepted, a settlement on an expired lock with a rejected lock lost error.
   * @param {string} lockKey
   * @param {string} outcome
   * @param {Record<string, any> | undefined} state
   * @returns {import('types').Amqp10Described} state to reply with
   */
  #settleServiceBus(lockKey, outcome, state) {
    const serviceBus = this.vhost.serviceBus;
    if (!serviceBus.locks.has(lockKey)) {
      const description = `The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue. Lock token: ${lockKey}`;
      return { $type: 'rejected', error: { $type: 'error', condition: 'com.microsoft:message-lock-lost', description } };
    }
    switch (outcome) {
      case 'released':
        serviceBus.settle(lockKey, 'abandoned');
        break;
      case 'modified':
        serviceBus.settle(lockKey, state.undeliverableHere ? 'defered' : 'abandoned', { propertiesToModify: state.messageAnnotations });
        break;
      case 'rejected': {
        const error = state.error ?? {};
        const { DeadLetterReason, DeadLetterErrorDescription, ...propertiesToModify } = error.info ?? {};
        serviceBus.settle(lockKey, 'suspended', {
          deadLetterReason: DeadLetterReason ?? error.condition,
          deadLetterDescription: DeadLetterErrorDescription ?? error.description,
          propertiesToModify,
        });
        break;
      }
      default:
        serviceBus.settle(lockKey, 'completed');
    }
    return { $type: 'accepted' };
  }

  /** Remote detached: release and confirm */
  onDetach() {
    const wasClosed = this.#closed;
    this.teardown();
    if (!wasClosed) this.session.send({ $type: 'detach', handle: this.handle, closed: true });
  }

  /** Cancel the consumer, requeue plain unsettled deliveries and drop any temporary queue */
  teardown() {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#consumer) {
      // Service Bus locks outlive the link, so keep delivered-but-locked messages pending like a 0-9-1 basic.cancel;
      // plain AMQP 1.0 requeues its delivered-unsettled messages
      this.#consumer.cancel(this.serviceBus ? { keepPending: true } : true);
      this.#consumer = null;
    }
    for (const [deliveryId, entry] of this.unsettled) {
      this.session.unsettled.delete(deliveryId);
      // Service Bus locks outlive the link: the client settles them through $management or they expire
      if (!entry.lockKey) entry.message.nack(false, true);
    }
    this.unsettled.clear();
    this.serviceBus?.detachLink(this);
    if (this.#tempQueue) this.vhost.deleteQueue(this.#tempQueue);
  }
}
