import net from 'node:net';

import { FrameParser } from './frame.js';
import { encodeMethod, decodeMethod, encodeProperties, decodeProperties } from './codec.js';
import { constants } from './defs.js';

const PROTOCOL_HEADER = Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary');
const CHANNEL = 1;

/**
 * @typedef {object} ClientDelivery
 * @property {import('types').MethodFields} fields basic.deliver fields (deliveryTag, exchange, routingKey)
 * @property {import('types').MessageProperties} properties
 * @property {Buffer} content
 */

/**
 * Minimal outbound AMQP 0-9-1 client used by the shovel to reach a remote broker. It speaks
 * just enough of the protocol to consume a queue and confirm-publish, built on this package's
 * own codec so the shovel needs no third party client at runtime.
 */
export class AmqpClient {
  /** @type {net.Socket | null} */
  #socket = null;
  #parser = new FrameParser();
  /** @type {Array<{ name: string, resolve: (frame: any) => void, reject: (err: Error) => void }>} */
  #waiters = [];
  #publishSeq = 0;
  /** @type {Map<number, { resolve: () => void, reject: (err: Error) => void }>} */
  #confirms = new Map();
  /** @type {(delivery: ClientDelivery) => void} set by consume() before any delivery can arrive */
  #onDelivery;
  /** @type {{ fields: any, properties: any, chunks: Buffer[], received: number, bodySize: number } | null} */
  #incoming = null;
  #host;
  #port;
  #vhost;
  #user;
  #pass;

  /** @param {string} uri e.g. amqp://user:pass@host:5672/vhost */
  constructor(uri) {
    const url = new URL(uri);
    this.#host = url.hostname;
    this.#port = Number(url.port);
    this.#user = decodeURIComponent(url.username) || 'guest';
    this.#pass = decodeURIComponent(url.password) || 'guest';
    this.#vhost = decodeURIComponent(url.pathname.replace(/^\//, '')) || '/';
  }

  /** Connect, run the handshake and open a channel */
  async connect() {
    await new Promise((resolve, reject) => {
      this.#socket = net.connect(this.#port, this.#host, () => resolve(undefined));
      this.#socket.once('error', reject);
      this.#socket.on('data', (chunk) => this.#feed(chunk));
      this.#socket.on('close', () => this.#fail(new Error('shovel client connection closed')));
    });
    this.#socket.on('error', (err) => this.#fail(err));

    this.#write(PROTOCOL_HEADER);
    await this.#waitFor('connection.start');
    this.#write(
      encodeMethod('connection.start-ok', 0, {
        clientProperties: { product: 'amqp-emulator shovel' },
        mechanism: 'PLAIN',
        response: Buffer.from(`\0${this.#user}\0${this.#pass}`),
        locale: 'en_US',
      }),
    );
    const tune = await this.#waitFor('connection.tune');
    this.#write(
      encodeMethod('connection.tune-ok', 0, {
        channelMax: tune.fields.channelMax,
        frameMax: tune.fields.frameMax,
        heartbeat: 0,
      }),
    );
    this.#write(encodeMethod('connection.open', 0, { virtualHost: this.#vhost }));
    await this.#waitFor('connection.open-ok');
    this.#write(encodeMethod('channel.open', CHANNEL));
    await this.#waitFor('channel.open-ok');
  }

  /** Put the channel into publisher confirm mode */
  async selectConfirms() {
    this.#write(encodeMethod('confirm.select', CHANNEL, { nowait: false }));
    await this.#waitFor('confirm.select-ok');
  }

  /**
   * Publish and resolve when the broker confirms it (basic.ack).
   * @param {string} exchange
   * @param {string} routingKey
   * @param {Buffer} content
   * @param {import('types').MessageProperties} [properties]
   * @returns {Promise<void>}
   */
  publishConfirm(exchange, routingKey, content, properties = {}) {
    const tag = ++this.#publishSeq;
    const confirmed = new Promise((resolve, reject) => this.#confirms.set(tag, { resolve, reject }));
    this.#write(encodeMethod('basic.publish', CHANNEL, { exchange, routingKey, mandatory: false, immediate: false }));
    this.#write(encodeProperties(CHANNEL, content.length, properties));
    if (content.length) this.#write(this.#bodyFrame(content));
    return confirmed;
  }

  /**
   * Consume a queue; `onDelivery` is called for every message with its content and properties.
   * @param {string} queue
   * @param {(delivery: ClientDelivery) => void} onDelivery
   */
  async consume(queue, onDelivery) {
    this.#onDelivery = onDelivery;
    this.#write(
      encodeMethod('basic.consume', CHANNEL, { queue, consumerTag: '', noLocal: false, noAck: false, exclusive: false, arguments: {} }),
    );
    await this.#waitFor('basic.consume-ok');
  }

  /** @param {number} deliveryTag */
  ack(deliveryTag) {
    this.#write(encodeMethod('basic.ack', CHANNEL, { deliveryTag, multiple: false }));
  }

  /** Drop the connection */
  close() {
    this.#socket?.destroy();
    this.#socket = null;
    this.#fail(new Error('shovel client closed'));
  }

  /** @param {Buffer} chunk */
  #feed(chunk) {
    for (const frame of this.#parser.feed(chunk)) {
      switch (frame.type) {
        case constants.FRAME_METHOD:
          this.#onMethod(frame.payload);
          break;
        case constants.FRAME_HEADER: {
          const { bodySize, properties } = decodeProperties(frame.payload);
          if (this.#incoming) {
            this.#incoming.properties = properties;
            this.#incoming.bodySize = bodySize;
            if (bodySize === 0) this.#emitDelivery();
          }
          break;
        }
        case constants.FRAME_BODY:
          if (this.#incoming) {
            this.#incoming.chunks.push(frame.payload);
            this.#incoming.received += frame.payload.length;
            if (this.#incoming.received >= this.#incoming.bodySize) this.#emitDelivery();
          }
          break;
      }
    }
  }

  /** @param {Buffer} payload */
  #onMethod(payload) {
    const { method, fields } = decodeMethod(payload);
    const name = method.name;
    if (name === 'basic.deliver') {
      this.#incoming = { fields, properties: {}, chunks: [], received: 0, bodySize: 0 };
      return;
    }
    if (name === 'basic.ack') {
      const pending = this.#confirms.get(fields.deliveryTag);
      if (pending) {
        this.#confirms.delete(fields.deliveryTag);
        pending.resolve();
      }
      return;
    }
    if (name === 'connection.close' || name === 'channel.close') {
      this.#fail(new Error(`remote ${name}`));
      return;
    }
    const idx = this.#waiters.findIndex((w) => w.name === name);
    if (idx > -1) this.#waiters.splice(idx, 1)[0].resolve({ fields });
  }

  #emitDelivery() {
    const { fields, properties, chunks } = /** @type {NonNullable<typeof this.incoming>} */ (this.#incoming);
    this.#incoming = null;
    this.#onDelivery({ fields, properties, content: Buffer.concat(chunks) });
  }

  /** @param {string} name */
  #waitFor(name) {
    return new Promise((resolve, reject) => this.#waiters.push({ name, resolve, reject }));
  }

  /** Reject every outstanding wait and confirm; called when the connection goes away */
  #fail(err) {
    for (const w of this.#waiters.splice(0)) w.reject(err);
    for (const [, pending] of this.#confirms) pending.reject(err);
    this.#confirms.clear();
  }

  /** @param {Buffer} content */
  #bodyFrame(content) {
    const frame = Buffer.allocUnsafe(8 + content.length);
    frame[0] = constants.FRAME_BODY;
    frame.writeUInt16BE(CHANNEL, 1);
    frame.writeUInt32BE(content.length, 3);
    content.copy(frame, 7);
    frame[frame.length - 1] = constants.FRAME_END;
    return frame;
  }

  /** @param {Buffer} buffer */
  #write(buffer) {
    this.#socket?.write(buffer);
  }
}
