import { Broker } from 'smqp';

import { kConsumerEntry } from './shared.js';
import { ServiceBus } from './servicebus/servicebus.js';

/** @type {Array<[string, string]>} */
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
    /** @type {Map<string, string>} declared AMQP exchange type by name, smqp only knows topic and direct */
    this.exchangeTypes = new Map();
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
    if (!this.broker.getExchange(name)) {
      this.exchangeTypes.delete(name);
      return undefined;
    }
    return this.exchangeTypes.get(name);
  }

  /**
   * @param {string} name
   * @param {string} type AMQP exchange type: direct, topic or fanout
   * @param {import('smqp').ExchangeOptions} [options]
   */
  declareExchange(name, type, options) {
    this.broker.assertExchange(name, type === 'direct' ? 'direct' : 'topic', options);
    this.exchangeTypes.set(name, type);
  }

  /**
   * Binding pattern for an exchange type; fanout ignores the routing key
   * @param {string} exchangeName
   * @param {string} routingKey
   */
  bindingPattern(exchangeName, routingKey) {
    return this.exchangeTypes.get(exchangeName) === 'fanout' ? '#' : routingKey;
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
      /** @type {import('types').ConsumerEntry | undefined} */
      const entry = /** @type {any} */ (consumer)[kConsumerEntry];
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
    this.exchangeTypes.clear();
    this.exclusiveQueues.clear();
    this.#declareDefaults();
  }
}
