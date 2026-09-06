import { EventEmitter } from 'node:events';

import { constants } from './defs.js';
import { FrameParser, FrameError } from './frame.js';
import { encodeMethod, encodeProperties, encodeBody, decodeMethod, BufferReader, HEARTBEAT_FRAME, PROTOCOL_HEADER } from './codec.js';

export { PROTOCOL_HEADER };
import { AmqpError, channelError, commandInvalid, unexpectedFrame } from './errors.js';
import { AmqpChannel } from './channel.js';

let connectionIds = 0;

const SERVER_CAPABILITIES = {
  publisher_confirms: true,
  exchange_exchange_bindings: true,
  'basic.nack': true,
  consumer_cancel_notify: true,
  'connection.blocked': true,
  consumer_priorities: true,
  authentication_failure_close: true,
  per_consumer_qos: true,
  direct_reply_to: false,
};

/** @type {Record<string, import('types').AmqpMethodName>} method expected from the client per handshake state */
const HANDSHAKE_EXPECTS = { start: 'connection.start-ok', tune: 'connection.tune-ok', open: 'connection.open' };

/**
 * Events emitted by a 0-9-1 connection: `channel` when a channel opens, `channel.error` when the server closes a channel,
 * `error.amqp` when the server closes the connection, `socket.error`, `heartbeat.timeout` and `close`.
 * @typedef {{ channel: [channel: AmqpChannel], 'channel.error': [err: AmqpError, channel: AmqpChannel], 'error.amqp': [err: AmqpError], 'socket.error': [err: Error], 'heartbeat.timeout': [], close: [] }} AmqpConnectionEvents
 */

/**
 * One client connection: handshake, framing, heartbeats and channel multiplexing
 * @extends {EventEmitter<AmqpConnectionEvents>}
 */
export class AmqpConnection extends EventEmitter {
  #parser = new FrameParser();
  #sentSinceLastCheck = false;
  #recvSinceLastCheck = false;
  #missedHeartbeats = 0;
  /** @type {NodeJS.Timeout[]} */
  #timers = [];
  /** @type {NodeJS.Timeout | null} */
  #closeTimer = null;
  #released = false;
  #destroyed = false;

  /**
   * @param {import('../server.js').AmqpServer} server
   * @param {import('node:net').Socket} socket
   */
  constructor(server, socket) {
    super();
    this.id = ++connectionIds;
    this.server = server;
    this.socket = socket;
    /** @type {Map<number, AmqpChannel>} */
    this.channels = new Map();
    /** @type {import('types').ConnectionState} */
    this.state = 'protocol-header';
    /** negotiated max frame size */
    this.frameMax = server.options.frameMax;
    /** negotiated max channel number */
    this.channelMax = server.options.channelMax;
    /** negotiated heartbeat interval in seconds */
    this.heartbeat = server.options.heartbeat;
    /** @type {import('../vhost.js').VHost | null} */
    this.vhost = null;
    /** @type {string | null} authenticated user */
    this.user = null;
    /** @type {Record<string, any>} client properties from connection.start-ok */
    this.clientProperties = {};
    /** @type {import('types').CloseReason | null} */
    this.closeReason = null;

    socket.setNoDelay(true);
    socket.on('data', (chunk) => this.feed(chunk));
    socket.on('error', (err) => this.#onSocketError(err));
    socket.on('close', () => this.destroy());
  }

  /** true if the client announced the connection.blocked capability and the connection is open */
  get supportsBlocked() {
    return this.state === 'running' && !!this.clientProperties.capabilities?.['connection.blocked'];
  }

  /**
   * Write raw bytes to the socket
   * @param {Buffer} buffer
   */
  write(buffer) {
    if (this.state === 'closed') return;
    this.#sentSinceLastCheck = true;
    this.socket.write(buffer);
  }

  /**
   * @param {number} channel
   * @param {import('types').AmqpMethodName} methodName
   * @param {import('types').MethodFields} [fields]
   */
  sendMethod(channel, methodName, fields) {
    this.write(encodeMethod(methodName, channel, fields));
  }

  /**
   * Send a content carrying method: method frame, header frame and body frames split by frameMax
   * @param {number} channel
   * @param {import('types').AmqpMethodName} methodName
   * @param {import('types').MethodFields} fields
   * @param {import('types').MessageProperties} properties
   * @param {Buffer} content
   */
  sendContent(channel, methodName, fields, properties, content) {
    const parts = [encodeMethod(methodName, channel, fields), encodeProperties(channel, content.length, properties)];
    const maxBody = this.frameMax - 8;
    for (let offset = 0; offset < content.length; offset += maxBody) {
      parts.push(encodeBody(channel, content.subarray(offset, offset + maxBody)));
    }
    this.write(Buffer.concat(parts));
  }

  /** Send connection.blocked with the server block reason if the client supports it */
  notifyBlocked() {
    const reason = this.server.blockedReason;
    if (reason !== null && this.supportsBlocked) this.sendMethod(0, 'connection.blocked', { reason });
  }

  /** Send connection.unblocked if the client supports it */
  notifyUnblocked() {
    if (this.supportsBlocked) this.sendMethod(0, 'connection.unblocked', {});
  }

  /**
   * Server initiated connection close
   * @param {AmqpError} err
   */
  closeWithError(err) {
    if (this.state === 'closed' || this.state === 'closing') return;
    this.closeReason = { replyCode: err.code, replyText: err.replyText };
    this.emit('error.amqp', err);
    this.#release();
    this.state = 'closing';
    this.write(
      encodeMethod('connection.close', 0, {
        replyCode: err.code,
        replyText: err.replyText,
        classId: err.method?.classId ?? 0,
        methodId: err.method?.methodId ?? 0,
      }),
    );
    this.#closeTimer = setTimeout(() => this.destroy(), this.server.options.closeTimeout);
    this.#closeTimer.unref();
  }

  /** Drop the socket and release everything */
  destroy() {
    if (this.#destroyed) return;
    this.#destroyed = true;
    this.state = 'closed';
    for (const timer of this.#timers) clearInterval(timer);
    this.#timers = [];
    if (this.#closeTimer) clearTimeout(this.#closeTimer);
    this.#release();
    this.socket.destroy();
    this.server.connections.delete(this);
    this.emit('close');
  }

  /**
   * Feed bytes from the socket. The first call carries the protocol header, already verified by the server.
   * @param {Buffer} chunk
   */
  feed(chunk) {
    this.#recvSinceLastCheck = true;
    this.#missedHeartbeats = 0;
    try {
      if (this.state === 'protocol-header') {
        chunk = chunk.subarray(8);
        this.state = 'start';
        this.#sendStart();
        if (!chunk.length) return;
      }
      for (const frame of this.#parser.feed(chunk)) {
        if (this.state === 'closed') return;
        this.#handleFrame(frame);
      }
    } catch (err) {
      this.#onError(err);
    }
  }

  /** @param {any} err */
  #onError(err) {
    if (err instanceof AmqpError) return this.closeWithError(err);
    if (err instanceof FrameError) {
      return this.closeWithError(new AmqpError(constants.FRAME_ERROR, `FRAME_ERROR - ${err.message}`, { connection: true }));
    }
    this.server.emit('error', err, this);
    this.closeWithError(new AmqpError(constants.INTERNAL_ERROR, `INTERNAL_ERROR - ${err.message}`, { connection: true }));
  }

  /** @param {NodeJS.ErrnoException} err */
  #onSocketError(err) {
    if (err.code !== 'ECONNRESET' && err.code !== 'EPIPE') this.emit('socket.error', err);
    this.destroy();
  }

  /** @param {import('types').Frame} frame */
  #handleFrame({ type, channel, payload }) {
    if (type === constants.FRAME_HEARTBEAT) return;

    if (this.state === 'closing') {
      if (type !== constants.FRAME_METHOD || channel !== 0) return;
      return this.#handleConnectionMethod(decodeMethod(payload));
    }
    if (this.state !== 'running') return this.#handleHandshakeFrame(type, channel, payload);

    if (channel === 0) {
      if (type !== constants.FRAME_METHOD) throw unexpectedFrame(`unexpected frame type ${type} on channel 0`);
      return this.#handleConnectionMethod(decodeMethod(payload));
    }

    const amqpChannel = this.channels.get(channel);
    switch (type) {
      case constants.FRAME_METHOD: {
        const { method, fields } = decodeMethod(payload);
        if (amqpChannel) return amqpChannel.handleMethod(method, fields);
        if (method.name !== 'channel.open') {
          if (method.name === 'channel.close-ok') return;
          throw channelError(`expected 'channel.open', got ${method.name}`, method);
        }
        if (this.channelMax && channel > this.channelMax) {
          throw channelError(`channel ${channel} exceeds channel-max ${this.channelMax}`, method);
        }
        const created = new AmqpChannel(this, channel);
        this.channels.set(channel, created);
        created.send('channel.open-ok', { channelId: '' });
        this.emit('channel', created);
        return;
      }
      case constants.FRAME_HEADER:
        if (!amqpChannel) throw channelError(`content header on unopened channel ${channel}`);
        return amqpChannel.handleHeader(payload);
      case constants.FRAME_BODY:
        if (!amqpChannel) throw channelError(`content body on unopened channel ${channel}`);
        return amqpChannel.handleBody(payload);
      default:
        throw unexpectedFrame(`unknown frame type ${type}`);
    }
  }

  /**
   * @param {number} type
   * @param {number} channel
   * @param {Buffer} payload
   */
  #handleHandshakeFrame(type, channel, payload) {
    if (type !== constants.FRAME_METHOD || channel !== 0) {
      throw new AmqpError(constants.UNEXPECTED_FRAME, 'UNEXPECTED_FRAME - expected connection method on channel 0 during handshake', {
        connection: true,
      });
    }
    const decoded = decodeMethod(payload);
    const { method, fields } = decoded;
    const expected = HANDSHAKE_EXPECTS[this.state];
    if (method.name !== expected) {
      if (method.name === 'connection.close') return this.#handleConnectionMethod(decoded);
      throw new AmqpError(constants.COMMAND_INVALID, `COMMAND_INVALID - expected ${expected}, got ${method.name}`, {
        connection: true,
        method,
      });
    }

    switch (this.state) {
      case 'start':
        return this.#onStartOk(fields, method);
      case 'tune':
        return this.#onTuneOk(fields);
      case 'open':
        return this.#onOpen(fields, method);
    }
  }

  #sendStart() {
    this.sendMethod(0, 'connection.start', {
      versionMajor: 0,
      versionMinor: 9,
      serverProperties: {
        product: 'amqp-emulator',
        version: this.server.options.version,
        platform: `Node.js ${process.version}`,
        capabilities: SERVER_CAPABILITIES,
        cluster_name: 'amqp-emulator@localhost',
      },
      mechanisms: 'PLAIN AMQPLAIN',
      locales: 'en_US',
    });
  }

  /**
   * @param {import('types').MethodFields} fields
   * @param {import('types').MethodDef} method
   */
  #onStartOk(fields, method) {
    this.clientProperties = fields.clientProperties;
    const { mechanism, response } = fields;
    let user;
    let password;
    if (mechanism === 'PLAIN') {
      const parts = response.toString('utf8').split(String.fromCharCode(0));
      user = parts[1];
      password = parts[2];
    } else if (mechanism === 'AMQPLAIN') {
      // AMQPLAIN response is a field table without the length prefix
      const size = Buffer.alloc(4);
      size.writeUInt32BE(response.length);
      const table = new BufferReader(Buffer.concat([size, response])).table();
      user = table.LOGIN;
      password = table.PASSWORD;
    } else {
      throw new AmqpError(constants.NOT_ALLOWED, `NOT_ALLOWED - unsupported authentication mechanism ${mechanism}`, {
        connection: true,
        method,
      });
    }

    const { authenticate } = this.server.options;
    if (authenticate && !authenticate(user, password, this)) {
      throw new AmqpError(
        constants.ACCESS_REFUSED,
        `ACCESS_REFUSED - Login was refused using authentication mechanism ${mechanism}. For details see the broker logfile.`,
        { connection: true, method },
      );
    }
    this.user = user;
    this.state = 'tune';
    this.sendMethod(0, 'connection.tune', { channelMax: this.channelMax, frameMax: this.frameMax, heartbeat: this.heartbeat });
  }

  /** @param {import('types').MethodFields} fields */
  #onTuneOk(fields) {
    if (fields.channelMax) this.channelMax = this.channelMax ? Math.min(this.channelMax, fields.channelMax) : fields.channelMax;
    if (fields.frameMax) this.frameMax = Math.max(constants.FRAME_MIN_SIZE, Math.min(this.frameMax, fields.frameMax));
    this.heartbeat = fields.heartbeat;
    this.state = 'open';
  }

  /**
   * @param {import('types').MethodFields} fields
   * @param {import('types').MethodDef} method
   */
  #onOpen(fields, method) {
    const vhostName = fields.virtualHost || '/';
    const { vhosts } = this.server.options;
    if (vhosts && !vhosts.includes(vhostName)) {
      throw new AmqpError(constants.NOT_ALLOWED, `NOT_ALLOWED - vhost ${vhostName} not found`, { connection: true, method });
    }
    this.vhost = this.server.getVHost(vhostName);
    this.state = 'running';
    this.sendMethod(0, 'connection.open-ok', { knownHosts: '' });
    this.#startHeartbeat();
    this.notifyBlocked();
    this.server.emit('connection', this);
  }

  /** @param {import('types').DecodedMethod} decoded */
  #handleConnectionMethod({ method, fields }) {
    switch (method.name) {
      case 'connection.close':
        this.closeReason = /** @type {import('types').CloseReason} */ (fields);
        this.#release();
        this.state = 'closed';
        this.socket.write(encodeMethod('connection.close-ok', 0, {}));
        this.socket.end();
        return;
      case 'connection.close-ok':
        if (this.state === 'closing') this.destroy();
        return;
      case 'connection.update-secret':
        return this.sendMethod(0, 'connection.update-secret-ok', {});
      default:
        if (this.state === 'closing') return;
        throw commandInvalid(`unexpected method ${method.name} on channel 0`, method);
    }
  }

  #startHeartbeat() {
    const heartbeat = this.heartbeat;
    if (!heartbeat) return;
    const sender = setInterval(
      () => {
        if (!this.#sentSinceLastCheck) this.write(HEARTBEAT_FRAME);
        this.#sentSinceLastCheck = false;
      },
      (heartbeat * 1000) / 2,
    );
    const monitor = setInterval(() => {
      if (this.#recvSinceLastCheck) {
        this.#recvSinceLastCheck = false;
        return;
      }
      if (++this.#missedHeartbeats >= 2) {
        this.emit('heartbeat.timeout');
        this.destroy();
      }
    }, heartbeat * 1000);
    sender.unref();
    monitor.unref();
    this.#timers.push(sender, monitor);
  }

  /** Release broker resources held by this connection */
  #release() {
    if (this.#released) return;
    this.#released = true;
    for (const channel of this.channels.values()) channel.teardown();
    this.channels.clear();
    this.vhost?.releaseConnection(this.id);
  }
}
