import { EventEmitter } from 'node:events';
import net from 'node:net';

import { AmqpConnection, PROTOCOL_HEADER } from './amqp09/connection.js';
import { Amqp10Connection } from './amqp10/connection.js';
import { AMQP_HEADER, SASL_HEADER } from './amqp10/types.js';
import { VHost } from './vhost.js';
import { Shovel } from './shovel.js';
import { AmqpError } from './amqp09/errors.js';
import { constants } from './amqp09/defs.js';

/** @type {Required<import('types').ServerOptions>} */
const DEFAULT_OPTIONS = {
  heartbeat: 60,
  frameMax: 131072,
  channelMax: 2047,
  closeTimeout: 1000,
  vhosts: null,
  authenticate: null,
  version: '0.1.0',
  evictInterval: 100,
  lockDuration: 60000,
  definitions: null,
  autoCreate: false,
};

/**
 * First defined field of a definitions entry, RabbitMQ spells them snake_case, hand written ones may be camelCase
 * @template T
 * @param {Record<string, any>} entry
 * @param {string[]} names
 * @param {T} fallback
 * @returns {T}
 */
function field(entry, names, fallback) {
  for (const name of names) if (entry[name] !== undefined) return entry[name];
  return fallback;
}

const FORCED_CLOSE_REASON = "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'";

/**
 * @typedef {AmqpConnection | Amqp10Connection} AnyConnection
 */

/**
 * Events emitted by the server: `connection` when a client completed its handshake, `error` for unexpected internal errors.
 * @typedef {{ connection: [connection: AnyConnection], error: [err: Error, connection?: AnyConnection] }} AmqpServerEvents
 */

/**
 * AMQP 0-9-1 and 1.0 broker emulator listening on a TCP port
 * @extends {EventEmitter<AmqpServerEvents>}
 */
export class AmqpServer extends EventEmitter {
  /** @type {Set<import('node:net').Socket>} sockets that have not sent a protocol header yet */
  #pending = new Set();

  /**
   * @param {import('types').ServerOptions} [options]
   */
  constructor(options = {}) {
    super();
    /** @type {Required<import('types').ServerOptions>} */
    this.options = { ...DEFAULT_OPTIONS, ...options };
    /** @type {Map<string, VHost>} */
    this.vhosts = new Map();
    /** @type {Map<string, Shovel>} */
    this.shovels = new Map();
    /** @type {Set<AnyConnection>} */
    this.connections = new Set();
    /** @type {string | null} reason passed to block(), null when not blocked */
    this.blockedReason = null;
    const { autoCreate } = this.options;
    /** @type {Required<import('types').AutoCreateOptions> | null} normalized autoCreate option, null when off */
    this.autoCreate = autoCreate ? { exchangeType: 'topic', ...(typeof autoCreate === 'object' ? autoCreate : {}) } : null;
    /** @type {import('types').Definitions[]} definitions replayed on reset */
    this.definitions = [];
    /** @type {net.Server} */
    this.server = net.createServer((socket) => this.#sniff(socket));
    this.server.on('error', (err) => {
      if (this.listenerCount('error')) this.emit('error', err);
    });
    if (this.options.definitions) this.load(this.options.definitions);
  }

  /**
   * Declare exchanges, queues, bindings and Service Bus topics from a RabbitMQ style definitions
   * object, as if they were provisioned outside the application. Loaded definitions are applied
   * again after `reset()`.
   * @param {import('types').Definitions} definitions
   */
  load(definitions) {
    this.definitions.push(definitions);
    this.#apply(definitions);
    return this;
  }

  /** @param {import('types').Definitions} definitions */
  #apply({ vhosts = [], exchanges = [], queues = [], bindings = [], topics = [] }) {
    for (const { name } of vhosts) this.#definedVHost(name);
    for (const def of exchanges) {
      this.#definedVHost(def.vhost).declareExchange(def.name, def.type, {
        durable: field(def, ['durable'], true),
        autoDelete: field(def, ['auto_delete', 'autoDelete'], false),
        internal: field(def, ['internal'], false),
      });
    }
    for (const def of queues) {
      this.#definedVHost(def.vhost).declareQueue(def.name, {
        durable: field(def, ['durable'], true),
        autoDelete: field(def, ['auto_delete', 'autoDelete'], false),
        arguments: field(def, ['arguments'], {}),
      });
    }
    for (const def of bindings) {
      const { broker } = this.#definedVHost(def.vhost);
      const routingKey = field(def, ['routing_key', 'routingKey'], '');
      const args = field(def, ['arguments'], {});
      const destinationType = /** @type {typeof field<'queue' | 'exchange'>} */ (field)(
        def,
        ['destination_type', 'destinationType'],
        'queue',
      );
      if (destinationType === 'exchange') {
        broker.bindExchange(def.source, def.destination, routingKey, args);
      } else {
        broker.bindQueue(def.destination, def.source, routingKey, { arguments: args });
      }
    }
    for (const def of topics) {
      const { serviceBus } = this.#definedVHost(def.vhost);
      serviceBus.createTopic(def.name);
      for (const subscription of def.subscriptions ?? []) serviceBus.createSubscription(def.name, subscription);
    }
  }

  /**
   * @param {string} [name] defaults to '/'
   */
  #definedVHost(name = '/') {
    const { vhosts } = this.options;
    if (vhosts && !vhosts.includes(name)) throw new Error(`definitions refer to vhost '${name}' which is not an allowed vhost`);
    return this.getVHost(name);
  }

  /**
   * Read the 8 byte protocol header and hand the socket to the matching protocol implementation
   * @param {import('node:net').Socket} socket
   */
  #sniff(socket) {
    this.#pending.add(socket);
    let buffered = Buffer.alloc(0);
    socket.on('error', () => {});
    socket.once('close', () => this.#pending.delete(socket));
    /** @param {Buffer} chunk */
    const onData = (chunk) => {
      buffered = Buffer.concat([buffered, chunk]);
      if (buffered.length < 8) return;
      socket.off('data', onData);
      this.#pending.delete(socket);
      const header = buffered.subarray(0, 8);
      /** @type {AnyConnection} */
      let connection;
      if (header.equals(PROTOCOL_HEADER)) connection = new AmqpConnection(this, socket);
      else if (header.equals(AMQP_HEADER) || header.equals(SASL_HEADER)) connection = new Amqp10Connection(this, socket);
      else return socket.end(PROTOCOL_HEADER);
      this.connections.add(connection);
      connection.feed(buffered);
    };
    socket.on('data', onData);
  }

  /**
   * Start listening
   * @param {number} [port] defaults to 0, i.e. a random free port
   * @param {string} [host] defaults to 127.0.0.1
   * @returns {Promise<import('types').ListenResult>}
   */
  listen(port = 0, host = '127.0.0.1') {
    return new Promise((resolve, reject) => {
      /** @param {Error} err */
      const onError = (err) => reject(err);
      this.server.once('error', onError);
      this.server.listen(port, host, () => {
        this.server.off('error', onError);
        resolve({ port: this.port, host: this.host, url: this.url });
      });
    });
  }

  /** @returns {number | undefined} */
  get port() {
    const address = this.server.address();
    return typeof address === 'object' && address ? address.port : undefined;
  }

  /** Host name usable in a client url */
  get host() {
    const address = this.server.address();
    const ip = typeof address === 'object' && address ? address.address : undefined;
    if (!ip || ip === '::' || ip === '0.0.0.0') return 'localhost';
    return ip.includes(':') ? `[${ip}]` : ip;
  }

  /** amqp:// url for clients */
  get url() {
    return `amqp://${this.host}:${this.port}`;
  }

  /**
   * Get or create a vhost
   * @param {string} [name] defaults to '/'
   */
  getVHost(name = '/') {
    let vhost = this.vhosts.get(name);
    if (!vhost) {
      vhost = new VHost(name, this);
      this.vhosts.set(name, vhost);
    }
    return vhost;
  }

  /**
   * Get the smqp broker behind a vhost, for inspection and seeding
   * @param {string} [vhost] defaults to '/'
   * @returns {import('smqp').Broker}
   */
  getBroker(vhost = '/') {
    return this.getVHost(vhost).broker;
  }

  /**
   * Azure Service Bus entities, locks and management state of a vhost
   * @param {string} [vhost] defaults to '/'
   */
  serviceBus(vhost = '/') {
    return this.getVHost(vhost).serviceBus;
  }

  /**
   * Create a shovel that moves messages from a source queue to a destination exchange or queue,
   * optionally across vhosts. The source message is acked once the destination publish is accepted.
   * @param {string} name unique shovel name
   * @param {import('./shovel.js').ShovelConfig} config
   * @returns {Shovel}
   */
  shovel(name, config) {
    if (this.shovels.has(name)) throw new Error(`Shovel ${name} already exists`);
    const shovel = new Shovel(name, config, this);
    this.shovels.set(name, shovel);
    return shovel;
  }

  /**
   * Close and forget a shovel by name
   * @param {string} name
   */
  closeShovel(name) {
    const shovel = this.shovels.get(name);
    if (!shovel) return;
    shovel.close();
    this.shovels.delete(name);
  }

  /**
   * Message, unacked and consumer counts for a vhost, totals and per queue
   * @param {string} [vhost] defaults to '/'
   * @returns {import('smqp').BrokerStats}
   */
  getStats(vhost = '/') {
    const existing = this.vhosts.get(vhost);
    if (!existing) return { messageCount: 0, unackedCount: 0, consumerCount: 0, queues: [] };
    return existing.broker.getStats();
  }

  /**
   * Close all client connections with connection.close, like a broker restart
   * @param {string} [reason]
   * @param {number} [code] defaults to 320 CONNECTION_FORCED
   */
  disconnect(reason = FORCED_CLOSE_REASON, code = constants.CONNECTION_FORCED) {
    this.#dropPending();
    for (const connection of [...this.connections]) {
      connection.closeWithError(new AmqpError(code, reason, { connection: true }));
    }
  }

  #dropPending() {
    for (const socket of this.#pending) socket.destroy();
    this.#pending.clear();
  }

  /**
   * Drop all connections and clear every vhost
   */
  reset() {
    this.#dropPending();
    for (const connection of [...this.connections]) connection.destroy();
    for (const shovel of this.shovels.values()) shovel.close();
    this.shovels.clear();
    for (const vhost of this.vhosts.values()) vhost.reset();
    for (const definitions of this.definitions) this.#apply(definitions);
    this.blockedReason = null;
  }

  /**
   * Send connection.blocked to all connections that support it
   * @param {string} [reason]
   */
  block(reason = 'low on memory') {
    this.blockedReason = reason;
    for (const connection of this.connections) connection.notifyBlocked();
  }

  /** Send connection.unblocked to all blocked connections */
  unblock() {
    if (this.blockedReason === null) return;
    this.blockedReason = null;
    for (const connection of this.connections) connection.notifyUnblocked();
  }

  /**
   * Stop listening and destroy all connections
   * @returns {Promise<void>}
   */
  close() {
    this.#dropPending();
    for (const connection of [...this.connections]) connection.destroy();
    for (const shovel of this.shovels.values()) shovel.close();
    this.shovels.clear();
    for (const vhost of this.vhosts.values()) vhost.close();
    return new Promise((resolve) => {
      if (!this.server.listening) return resolve();
      this.server.close(() => resolve());
    });
  }
}
