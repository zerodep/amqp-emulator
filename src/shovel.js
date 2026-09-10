import { toBuffer, wireProperties } from './shared.js';
import { AmqpClient } from './amqp09/client.js';

/**
 * @typedef {object} ShovelSource
 * @property {string} queue source queue to consume
 * @property {string} [uri] amqp uri of a remote broker to consume from; omit for this emulator
 * @property {string} [vhost] vhost the source queue lives in when local, defaults to '/'
 */

/**
 * @typedef {object} ShovelDestination
 * @property {string} [exchange] destination exchange to publish to
 * @property {string} [routingKey] routing key for the destination exchange, defaults to the message's own
 * @property {string} [queue] destination queue to publish to, when no exchange is given
 * @property {string} [uri] amqp uri of a remote broker to publish to; omit for this emulator
 * @property {string} [vhost] vhost the destination lives in when local, defaults to '/'
 */

/**
 * @typedef {object} ShovelConfig
 * @property {ShovelSource} source
 * @property {ShovelDestination} destination
 */

/**
 * RabbitMQ style shovel: consume a source queue and republish every message to a destination
 * exchange (with a routing key) or queue. Either side may be this emulator or a remote broker
 * reached over AMQP with a `uri`. On-confirm semantics — the source message is acked once the
 * destination has accepted the message (a remote destination is published in confirm mode).
 */
export class Shovel {
  /** @type {import('smqp').Consumer | null} */
  #consumer = null;
  /** @type {AmqpClient | null} */
  #sourceClient = null;
  /** @type {AmqpClient | null} */
  #destClient = null;
  /** @type {import('./server.js').AmqpServer} */
  #server;

  /**
   * @param {string} name unique shovel name
   * @param {ShovelConfig} config
   * @param {import('./server.js').AmqpServer} server
   */
  constructor(name, config, server) {
    if (!name || typeof name !== 'string') throw new TypeError('Shovel name is required and must be a string');
    if (!config?.source?.queue) throw new TypeError('Shovel source.queue is required');
    const { exchange, queue } = config.destination ?? {};
    if (exchange === undefined && queue === undefined) throw new TypeError('Shovel destination.exchange or destination.queue is required');

    this.name = name;
    this.source = config.source;
    this.destination = config.destination;
    this.#server = server;

    /** Resolves once the shovel is connected and consuming; rejects if a remote leg fails to connect */
    this.ready = this.#start();
    this.ready.catch(() => {});
  }

  #start() {
    // A purely local shovel subscribes synchronously so it is live the moment it is created.
    // Any remote leg is connected first (the destination before the source, so nothing is
    // consumed before it can be delivered) and the shovel is ready once that finishes.
    if (this.source.uri === undefined && this.destination.uri === undefined) {
      this.#subscribeLocalSource();
      return Promise.resolve();
    }
    return this.#startRemote();
  }

  async #startRemote() {
    if (this.destination.uri !== undefined) {
      this.#destClient = new AmqpClient(this.destination.uri);
      await this.#destClient.connect();
      await this.#destClient.selectConfirms();
    }
    if (this.source.uri !== undefined) {
      this.#sourceClient = new AmqpClient(this.source.uri);
      await this.#sourceClient.connect();
      await this.#sourceClient.consume(this.source.queue, (delivery) => this.#forwardRemote(delivery));
    } else {
      this.#subscribeLocalSource();
    }
  }

  #subscribeLocalSource() {
    const broker = this.#server.getBroker(this.source.vhost);
    this.#consumer = broker.consume(this.source.queue, (_routingKey, message) => this.#forwardLocal(message), {
      consumerTag: `shovel.${this.name}`,
    });
  }

  /**
   * A message from a local source queue.
   * @param {import('smqp').Message} message
   */
  #forwardLocal(message) {
    const result = this.#publish(
      toBuffer(message.content),
      message.properties,
      wireProperties(message.properties),
      message.fields.routingKey,
    );
    // a rejected confirm (remote refused or dropped) leaves the source message unacked for retry
    if (result)
      result.then(
        () => message.ack(),
        () => {},
      );
    else message.ack();
  }

  /**
   * A message from a remote source broker.
   * @param {import('./amqp09/client.js').ClientDelivery} delivery
   */
  #forwardRemote(delivery) {
    const result = this.#publish(delivery.content, delivery.properties, delivery.properties, delivery.fields.routingKey);
    const done = () => this.#sourceClient?.ack(delivery.fields.deliveryTag);
    if (result) result.then(done, () => {});
    else done();
  }

  /**
   * Publish to the destination. Returns a promise for a remote destination (resolves on confirm),
   * or undefined for a local one (published synchronously).
   * @param {Buffer} content
   * @param {any} smqpProperties properties for a local destination, carrying wire fidelity symbols
   * @param {any} wireProperties plain wire properties for a remote destination
   * @param {string} sourceRoutingKey the message's own routing key, used when the destination gives none
   * @returns {Promise<void> | undefined}
   */
  #publish(content, smqpProperties, wireProperties, sourceRoutingKey) {
    const { exchange, queue, routingKey } = this.destination;
    if (this.#destClient) {
      if (exchange !== undefined) return this.#destClient.publishConfirm(exchange, routingKey ?? sourceRoutingKey, content, wireProperties);
      return this.#destClient.publishConfirm('', /** @type {string} */ (queue), content, wireProperties);
    }
    const broker = this.#server.getBroker(this.destination.vhost);
    if (exchange !== undefined) broker.publish(exchange, routingKey ?? sourceRoutingKey, content, smqpProperties);
    else broker.sendToQueue(/** @type {string} */ (queue), content, smqpProperties);
    return undefined;
  }

  /** Stop the shovel, cancelling the source consumer and dropping any remote connections */
  close() {
    this.#consumer?.cancel();
    this.#consumer = null;
    this.#sourceClient?.close();
    this.#sourceClient = null;
    this.#destClient?.close();
    this.#destClient = null;
  }
}
