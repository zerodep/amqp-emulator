import { EventEmitter } from 'node:events';

import { AMQP_HEADER, SASL_HEADER, FRAME_TYPE_AMQP, FRAME_TYPE_SASL, Amqp10FrameParser, encodeFrame } from './types.js';
import { Session, LinkError } from './link.js';

let connectionIds = 0;

const SASL_OK = 0;
const SASL_AUTH_FAILED = 1;

/**
 * Events emitted by a 1.0 connection: `error.amqp` when the server closes it with an error condition, and `close`.
 * @typedef {{ 'error.amqp': [err: { condition: string, description?: string }], close: [] }} Amqp10ConnectionEvents
 */

/**
 * One AMQP 1.0 client connection: SASL, open, sessions and links
 * @extends {EventEmitter<Amqp10ConnectionEvents>}
 */
export class Amqp10Connection extends EventEmitter {
  #parser = new Amqp10FrameParser();
  /** @type {Buffer} bytes of a protocol header not yet complete */
  #header = Buffer.alloc(0);
  /** @type {NodeJS.Timeout | null} */
  #closeTimer = null;
  /** @type {NodeJS.Timeout | null} */
  #heartbeat = null;
  #released = false;
  #destroyed = false;

  /**
   * @param {import('../server.js').AmqpServer} server
   * @param {import('node:net').Socket} socket
   */
  constructor(server, socket) {
    super();
    this.id = ++connectionIds;
    /** @type {'1.0'} */
    this.protocol = '1.0';
    this.server = server;
    this.socket = socket;
    /** @type {import('types').Amqp10ConnectionState} */
    this.state = 'header';
    /** @type {string | null} remote container id from open */
    this.containerId = null;
    /** @type {string | null} authenticated user, anonymous when SASL ANONYMOUS was used */
    this.user = null;
    /** @type {import('../vhost.js').VHost | null} */
    this.vhost = null;
    /** @type {Record<string, any>} connection properties from open */
    this.clientProperties = {};
    /** negotiated max frame size */
    this.maxFrameSize = server.options.frameMax;
    this.channelMax = Math.min(server.options.channelMax || 0xffff, 0xffff);
    /** @type {Map<number, Session>} sessions by remote channel */
    this.sessions = new Map();
    /** @type {Record<string, any> | null} error from close, ours or theirs */
    this.closeReason = null;
    /** @type {'amqp' | 'servicebus'} plain AMQP 1.0, or the Azure Service Bus dialect once the client used $cbs or $management */
    this.dialect = 'amqp';

    socket.setNoDelay(true);
    socket.on('data', (chunk) => this.feed(chunk));
    socket.on('error', () => this.destroy());
    socket.on('close', () => this.destroy());
  }

  /** 1.0 has no connection.blocked, so never */
  get supportsBlocked() {
    return false;
  }

  /**
   * Find a link we send on by name, used to answer request response links
   * @param {string} name
   */
  findLink(name) {
    for (const session of this.sessions.values()) {
      for (const link of session.links.values()) {
        if (link.sending && link.name === name) return link;
      }
    }
    return undefined;
  }

  /** @param {Buffer} buffer */
  write(buffer) {
    if (this.state === 'closed') return;
    this.socket.write(buffer);
  }

  /**
   * @param {number} channel
   * @param {import('types').Amqp10Described} performative
   * @param {Buffer} [payload]
   */
  send(channel, performative, payload) {
    this.write(encodeFrame(FRAME_TYPE_AMQP, channel, performative, payload));
  }

  /**
   * Feed bytes from the socket, or the bytes the server already read while sniffing the protocol
   * @param {Buffer} chunk
   */
  feed(chunk) {
    try {
      if (this.state === 'header' || this.state === 'sasl-done') {
        this.#header = Buffer.concat([this.#header, chunk]);
        if (this.#header.length < 8) return;
        const header = this.#header.subarray(0, 8);
        chunk = this.#header.subarray(8);
        this.#header = Buffer.alloc(0);
        if (!this.#onHeader(header)) return;
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

  /**
   * @param {Buffer} header
   * @returns {boolean} whether to keep reading frames
   */
  #onHeader(header) {
    if (this.state === 'header' && header.equals(SASL_HEADER)) {
      this.write(SASL_HEADER);
      this.write(encodeFrame(FRAME_TYPE_SASL, 0, { $type: 'sasl-mechanisms', saslServerMechanisms: ['ANONYMOUS', 'PLAIN'] }));
      this.state = 'sasl';
      return true;
    }
    if (header.equals(AMQP_HEADER) && (this.state === 'sasl-done' || !this.server.options.authenticate)) {
      this.write(AMQP_HEADER);
      this.state = 'open';
      return true;
    }
    // not a header we accept: answer with the one we want and hang up
    this.socket.end(this.state === 'header' ? SASL_HEADER : AMQP_HEADER);
    this.state = 'closed';
    return false;
  }

  /** @param {any} err */
  #onError(err) {
    if (err instanceof LinkError) return this.closeWithError(err);
    this.server.emit('error', err, this);
    this.closeWithError({ condition: 'amqp:internal-error', description: err.message });
  }

  /** @param {import('types').Amqp10Frame} frame */
  #handleFrame(frame) {
    if (frame.type === FRAME_TYPE_SASL) return this.#handleSasl(frame);
    const performative = /** @type {Record<string, any> | null} */ (frame.performative);
    if (!performative) return; // empty frame, heartbeat

    if (this.state === 'open') {
      if (performative.$type !== 'open') throw new LinkError('amqp:illegal-state', `expected open, got ${performative.$type}`);
      return this.#onOpen(performative);
    }
    if (this.state !== 'running') {
      if (performative.$type === 'close' && this.state === 'closing') this.destroy();
      return;
    }

    switch (performative.$type) {
      case 'begin':
        this.sessions.set(frame.channel, new Session(this, frame.channel, performative));
        return;
      case 'end': {
        const session = this.sessions.get(frame.channel);
        if (!session) throw new LinkError('amqp:not-found', `no session on channel ${frame.channel}`);
        this.sessions.delete(frame.channel);
        session.teardown();
        this.send(frame.channel, { $type: 'end' });
        return;
      }
      case 'close':
        this.closeReason = performative.error ?? null;
        this.#release();
        this.state = 'closed';
        this.socket.write(encodeFrame(FRAME_TYPE_AMQP, 0, { $type: 'close' }));
        this.socket.end();
        return;
      case 'attach':
      case 'flow':
      case 'transfer':
      case 'disposition':
      case 'detach': {
        const session = this.sessions.get(frame.channel);
        if (!session) throw new LinkError('amqp:not-found', `no session on channel ${frame.channel}`);
        return session.handle(frame);
      }
      default:
        throw new LinkError('amqp:not-implemented', `${performative.$type} is not implemented`);
    }
  }

  /** @param {import('types').Amqp10Frame} frame */
  #handleSasl(frame) {
    const performative = /** @type {Record<string, any> | null} */ (frame.performative);
    if (this.state !== 'sasl' || performative?.$type !== 'sasl-init') {
      throw new LinkError('amqp:illegal-state', `unexpected ${performative?.$type ?? 'empty'} sasl frame`);
    }
    const { mechanism, initialResponse } = performative;
    let user = 'anonymous';
    let password;
    if (mechanism === 'PLAIN') {
      const parts = (initialResponse ?? Buffer.alloc(0)).toString('utf8').split(String.fromCharCode(0));
      user = parts[1] ?? null;
      password = parts[2];
    } else if (mechanism !== 'ANONYMOUS') {
      return this.#saslFail();
    }
    const { authenticate } = this.server.options;
    if (authenticate && !authenticate(user, password, /** @type {any} */ (this))) return this.#saslFail();
    this.user = user;
    this.write(encodeFrame(FRAME_TYPE_SASL, 0, { $type: 'sasl-outcome', code: SASL_OK }));
    this.state = 'sasl-done';
  }

  #saslFail() {
    this.socket.end(encodeFrame(FRAME_TYPE_SASL, 0, { $type: 'sasl-outcome', code: SASL_AUTH_FAILED }));
    this.state = 'closed';
  }

  /** @param {Record<string, any>} open */
  #onOpen(open) {
    this.containerId = open.containerId;
    this.clientProperties = open.properties ?? {};
    if (open.maxFrameSize) this.maxFrameSize = Math.min(this.maxFrameSize, open.maxFrameSize);
    if (open.channelMax !== undefined) this.channelMax = Math.min(this.channelMax, open.channelMax);
    this.vhost = this.server.getVHost('/');
    if (this.clientProperties.product === 'MSJSClient') this.dialect = 'servicebus';
    this.state = 'running';
    if (open.idleTimeOut) {
      // the peer drops us after twice its idle timeout without frames, so keep empty frames flowing
      this.#heartbeat = setInterval(() => this.write(encodeFrame(FRAME_TYPE_AMQP, 0, null)), open.idleTimeOut / 2);
      this.#heartbeat.unref();
    }
    this.send(0, {
      $type: 'open',
      containerId: 'amqp-emulator',
      maxFrameSize: this.maxFrameSize,
      channelMax: this.channelMax,
      properties: { product: 'amqp-emulator', version: this.server.options.version, platform: `Node.js ${process.version}` },
    });
    this.server.emit('connection', /** @type {any} */ (this));
  }

  /** No-op, AMQP 1.0 has no blocked notification */
  notifyBlocked() {}

  /** No-op, AMQP 1.0 has no blocked notification */
  notifyUnblocked() {}

  /**
   * Server initiated close. Accepts a 0-9-1 style AmqpError (code 320 becomes amqp:connection:forced)
   * or an object with a 1.0 condition and description.
   * @param {{ condition?: string, description?: string, code?: number, replyText?: string }} err
   */
  closeWithError(err) {
    if (this.state === 'closed' || this.state === 'closing') return;
    const condition = err.condition ?? (err.code === 320 ? 'amqp:connection:forced' : 'amqp:internal-error');
    const description = err.description ?? err.replyText;
    this.closeReason = { condition, description };
    this.emit('error.amqp', { condition, description });
    this.#release();
    if (this.state === 'header' || this.state === 'sasl' || this.state === 'sasl-done') return this.destroy();
    this.state = 'closing';
    this.write(encodeFrame(FRAME_TYPE_AMQP, 0, { $type: 'close', error: { $type: 'error', condition, description } }));
    this.#closeTimer = setTimeout(() => this.destroy(), this.server.options.closeTimeout);
    this.#closeTimer.unref();
  }

  #release() {
    if (this.#released) return;
    this.#released = true;
    for (const session of this.sessions.values()) session.teardown();
    this.sessions.clear();
    this.vhost?.releaseConnection(this.id);
  }

  /** Drop the socket and release everything */
  destroy() {
    if (this.#destroyed) return;
    this.#destroyed = true;
    this.state = 'closed';
    if (this.#closeTimer) clearTimeout(this.#closeTimer);
    if (this.#heartbeat) clearInterval(this.#heartbeat);
    this.#release();
    this.socket.destroy();
    this.server.connections.delete(/** @type {any} */ (this));
    this.emit('close');
  }
}
