import { EventEmitter } from 'node:events';
import net from 'node:net';

import { AmqpConnection } from './connection.js';
import { VHost } from './vhost.js';
import { AmqpError } from './errors.js';
import { constants } from './defs.js';

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
};

const FORCED_CLOSE_REASON = "CONNECTION_FORCED - broker forced connection closure with reason 'shutdown'";

/**
 * AMQP 0-9-1 broker emulator listening on a TCP port
 * @extends {EventEmitter<import('types').AmqpServerEvents>}
 */
export class AmqpServer extends EventEmitter {
  /**
   * @param {import('types').ServerOptions} [options]
   */
  constructor(options = {}) {
    super();
    /** @type {Required<import('types').ServerOptions>} */
    this.options = { ...DEFAULT_OPTIONS, ...options };
    /** @type {Map<string, VHost>} */
    this.vhosts = new Map();
    /** @type {Set<AmqpConnection>} */
    this.connections = new Set();
    /** @type {string | null} reason passed to block(), null when not blocked */
    this.blockedReason = null;
    /** @type {net.Server} */
    this.server = net.createServer((socket) => {
      const connection = new AmqpConnection(this, socket);
      this.connections.add(connection);
    });
    this.server.on('error', (err) => {
      if (this.listenerCount('error')) this.emit('error', err);
    });
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
    for (const connection of [...this.connections]) {
      connection.closeWithError(new AmqpError(code, reason, { connection: true }));
    }
  }

  /**
   * Drop all connections and clear every vhost
   */
  reset() {
    for (const connection of [...this.connections]) connection.destroy();
    for (const vhost of this.vhosts.values()) vhost.reset();
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
    for (const connection of [...this.connections]) connection.destroy();
    for (const vhost of this.vhosts.values()) vhost.close();
    return new Promise((resolve) => {
      if (!this.server.listening) return resolve();
      this.server.close(() => resolve());
    });
  }
}
