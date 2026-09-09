import net from 'node:net';

import { Amqp10FrameParser, encodeAmqp10Frame } from 'amqp-emulator';

export const AMQP_HEADER = Buffer.from([65, 77, 81, 80, 0, 1, 0, 0]);
export const SASL_HEADER = Buffer.from([65, 77, 81, 80, 3, 1, 0, 0]);

/**
 * Minimal AMQP 1.0 client speaking raw frames, for protocol edge cases rhea never produces
 */
export class Raw10Client {
  /**
   * @param {number} port
   * @param {string} [host]
   */
  constructor(port, host = '127.0.0.1') {
    this.port = port;
    this.host = host;
    this.parser = new Amqp10FrameParser();
    /** @type {import('amqp-emulator').Amqp10Frame[]} */
    this.frames = [];
    /** @type {Array<(frame: import('amqp-emulator').Amqp10Frame) => void>} */
    this.waiters = [];
    /** @type {Buffer[]} protocol headers received from the server */
    this.headers = [];
    /** number of 8 byte protocol headers still expected before frames */
    this.expectHeaders = 0;
    /** @type {net.Socket} */
    this.socket = null;
    this.closed = new Promise((resolve) => (this.resolveClosed = resolve));
    /** @type {Buffer[]} */
    this.raw = [];
    this.pendingHeader = Buffer.alloc(0);
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.socket = net.connect(this.port, this.host, () => resolve(this));
      this.socket.once('error', reject);
      this.socket.on('data', (chunk) => {
        this.raw.push(chunk);
        while (this.expectHeaders > 0 && chunk.length) {
          this.pendingHeader = Buffer.concat([this.pendingHeader, chunk]);
          if (this.pendingHeader.length < 8) return;
          this.headers.push(this.pendingHeader.subarray(0, 8));
          chunk = this.pendingHeader.subarray(8);
          this.pendingHeader = Buffer.alloc(0);
          this.expectHeaders--;
        }
        if (!chunk.length) return;
        for (const frame of this.parser.feed(chunk)) {
          const waiter = this.waiters.shift();
          if (waiter) waiter(frame);
          else this.frames.push(frame);
        }
      });
      this.socket.on('close', () => this.resolveClosed(true));
    });
  }

  /** Next frame from the server */
  nextFrame() {
    if (this.frames.length) return Promise.resolve(this.frames.shift());
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  /**
   * Skip frames until a performative of the given type arrives
   * @param {string} type
   */
  async waitFor(type) {
    for (;;) {
      const frame = await this.nextFrame();
      if (/** @type {any} */ (frame.performative)?.$type === type) return frame;
    }
  }

  /** @param {Buffer} buffer */
  write(buffer) {
    this.socket.write(buffer);
  }

  /**
   * Send an AMQP frame
   * @param {number} channel
   * @param {Record<string, any> | null} performative
   * @param {Buffer} [payload]
   */
  send(channel, performative, payload) {
    this.write(encodeAmqp10Frame(0, channel, /** @type {any} */ (performative), payload));
  }

  /**
   * Send a SASL frame
   * @param {Record<string, any>} performative
   */
  sendSasl(performative) {
    this.write(encodeAmqp10Frame(1, 0, /** @type {any} */ (performative)));
  }

  /**
   * Send a protocol header and expect it echoed
   * @param {Buffer} header
   */
  sendHeader(header) {
    this.expectHeaders++;
    this.write(header);
  }

  /**
   * Complete SASL and the open handshake
   * @param {object} [options]
   * @param {boolean} [options.sasl] defaults to true
   * @param {string} [options.mechanism] defaults to ANONYMOUS
   * @param {string} [options.user]
   * @param {string} [options.password]
   * @param {Record<string, any>} [options.open] extra open fields
   */
  async handshake({ sasl = true, mechanism = 'ANONYMOUS', user, password, open = {} } = {}) {
    if (sasl) {
      this.sendHeader(SASL_HEADER);
      await this.waitFor('sasl-mechanisms');
      const initialResponse = mechanism === 'PLAIN' ? Buffer.from(['', user, password].join(String.fromCharCode(0))) : undefined;
      this.sendSasl({ $type: 'sasl-init', mechanism, initialResponse });
      const outcome = await this.waitFor('sasl-outcome');
      if (outcome.performative.code !== 0) return outcome;
    }
    this.sendHeader(AMQP_HEADER);
    this.send(0, { $type: 'open', containerId: 'raw10', ...open });
    return this.waitFor('open');
  }

  /**
   * Begin a session
   * @param {number} [channel]
   */
  begin(channel = 0) {
    this.send(channel, { $type: 'begin', nextOutgoingId: 0, incomingWindow: 2048, outgoingWindow: 2048 });
    return this.waitFor('begin');
  }

  /**
   * Attach a link
   * @param {number} channel
   * @param {Record<string, any>} attach
   */
  attach(channel, attach) {
    this.send(channel, { $type: 'attach', handle: 0, ...attach });
    return this.waitFor('attach');
  }

  /** Proper close handshake */
  async close() {
    this.send(0, { $type: 'close' });
    await this.waitFor('close');
    this.socket.end();
    await this.closed;
  }

  destroy() {
    this.socket.destroy();
    return this.closed;
  }
}
