import { Broker } from 'smqp';

import { ServiceBus } from './servicebus/servicebus.js';

/** @type {Array<[string, import('smqp').exchangeType]>} */
const DEFAULT_EXCHANGES = [
  ['amq.direct', 'direct'],
  ['amq.fanout', 'fanout'],
  ['amq.topic', 'topic'],
];

/**
 * A virtual host wraps one smqp broker plus the bookkeeping AMQP needs on top of it
 */
export class VHost {
  /**
   * @param {string} name
   * @param {import('./server.js').AmqpServer} server
   */
  constructor(name, server) {
    this.name = name;
    this.server = server;
    /** @type {import('smqp').Broker} */
    this.broker = new Broker(this);
    /** @type {Map<string, number>} exclusive queue name -> owning connection id */
    this.exclusiveQueues = new Map();
    /** @type {import('types').Publishing | null} channel currently publishing, for basic.return correlation */
    this.publishing = null;
    this.broker.on('return', (event) => {
      this.publishing?.channel.onReturn(/** @type {any} */ (event), this.publishing);
    });
    this.#declareDefaults();
    const { evictInterval } = server.options;
    if (evictInterval > 0) {
      // RabbitMQ dead letters expired messages whether or not anyone consumes, smqp only evicts lazily on consume
      this.#sweeper = setInterval(() => this.sweep(), evictInterval);
      this.#sweeper.unref();
    }
  }

  /** @type {NodeJS.Timeout | null} */
  #sweeper = null;
  /** @type {ServiceBus | null} */
  #serviceBus = null;

  /** Azure Service Bus semantics for this vhost, created on first use */
  get serviceBus() {
    return (this.#serviceBus ??= new ServiceBus(this, this.server));
  }

  /** Expire messages, release expired locks and enqueue due scheduled messages */
  sweep() {
    this.broker.evictExpired();
    this.#serviceBus?.sweep();
  }

  #declareDefaults() {
    for (const [name, type] of DEFAULT_EXCHANGES) this.declareExchange(name, type, { durable: true, autoDelete: false });
  }

  /**
   * @param {string} name
   * @returns {string | undefined} AMQP exchange type if the exchange exists
   */
  getExchangeType(name) {
    return this.broker.getExchange(name)?.type;
  }

  /**
   * @param {string} name
   * @param {import('smqp').exchangeType} type AMQP exchange type: direct, topic or fanout
   * @param {import('smqp').ExchangeOptions} [options]
   */
  declareExchange(name, type, options) {
    this.broker.assertExchange(name, type, options);
  }

  /**
   * Declare a queue, mapping the `x-` arguments a client would send to smqp options
   * @param {string} name
   * @param {import('types').QueueDeclareOptions} [options]
   */
  declareQueue(name, { durable = false, autoDelete = false, exclusive = false, arguments: args = {} } = {}) {
    /** @type {import('smqp').QueueOptions} */
    const options = { durable, autoDelete, exclusive, arguments: args };
    if (args['x-message-ttl'] !== undefined) options.messageTtl = args['x-message-ttl'];
    if (args['x-max-length'] !== undefined) options.maxLength = args['x-max-length'];
    if (args['x-dead-letter-exchange'] !== undefined) options.deadLetterExchange = args['x-dead-letter-exchange'];
    if (args['x-dead-letter-routing-key'] !== undefined) options.deadLetterRoutingKey = args['x-dead-letter-routing-key'];
    return this.broker.assertQueue(name, options);
  }

  /**
   * Exchange type of an existing exchange. With the server `autoCreate` option a missing exchange is
   * created durable with the configured default type instead of being reported as missing.
   * @param {string} name
   * @returns {string | undefined}
   */
  ensureExchange(name) {
    const type = this.getExchangeType(name);
    if (type) return type;
    const { autoCreate } = this.server;
    if (!autoCreate) return undefined;
    this.declareExchange(name, autoCreate.exchangeType, { durable: true, autoDelete: false });
    return autoCreate.exchangeType;
  }

  /**
   * An existing queue. With the server `autoCreate` option a missing queue is created durable.
   * @param {string} name
   * @returns {import('smqp').Queue | undefined}
   */
  ensureQueue(name) {
    const queue = this.broker.getQueue(name);
    if (queue || !this.server.autoCreate) return queue;
    return this.declareQueue(name, { durable: true });
  }

  /**
   * Delete a queue and notify channels consuming from it
   * @param {string} queueName
   */
  deleteQueue(queueName) {
    this.exclusiveQueues.delete(queueName);
    const queue = this.broker.getQueue(queueName);
    if (!queue) return undefined;

    const consumers = queue.consumers;
    const result = this.broker.deleteQueue(queueName);
    for (const consumer of consumers) {
      /** @type {import('types').ConsumerEntry | undefined} consume owner, absent for consumers created through the broker api */
      const entry = /** @type {any} */ (consumer).owner;
      if (entry) entry.channel.onConsumerCancelled(entry);
    }
    return result;
  }

  /**
   * Delete exclusive queues owned by a connection
   * @param {number} connectionId
   */
  releaseConnection(connectionId) {
    for (const [queueName, ownerId] of [...this.exclusiveQueues]) {
      if (ownerId === connectionId) this.deleteQueue(queueName);
    }
  }

  /** Stop the expiry sweeper */
  close() {
    if (this.#sweeper) clearInterval(this.#sweeper);
    this.#sweeper = null;
  }

  /** Clear everything and redeclare the default exchanges */
  reset() {
    this.publishing = null;
    this.#serviceBus = null;
    this.broker.reset();
    this.exclusiveQueues.clear();
    this.#declareDefaults();
  }
}
