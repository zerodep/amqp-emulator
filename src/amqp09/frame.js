import { constants } from './defs.js';

const HEADER_SIZE = 7;

/**
 * Incremental AMQP frame parser. Feed it socket chunks, get complete frames back.
 */
export class FrameParser {
  constructor() {
    /** @type {Buffer} */
    this.pending = Buffer.alloc(0);
  }
  /**
   * @param {Buffer} chunk
   * @returns {import('types').Frame[]}
   */
  feed(chunk) {
    let buf = this.pending.length ? Buffer.concat([this.pending, chunk]) : chunk;
    /** @type {import('types').Frame[]} */
    const frames = [];
    while (buf.length >= HEADER_SIZE) {
      const size = buf.readUInt32BE(3);
      const total = HEADER_SIZE + size + 1;
      if (buf.length < total) break;
      const end = buf[total - 1];
      if (end !== constants.FRAME_END) {
        throw new FrameError(`Invalid frame end marker 0x${end.toString(16)}, expected 0xce`);
      }
      frames.push({
        type: buf[0],
        channel: buf.readUInt16BE(1),
        payload: buf.subarray(HEADER_SIZE, HEADER_SIZE + size),
      });
      buf = buf.subarray(total);
    }
    this.pending = buf.length ? Buffer.from(buf) : Buffer.alloc(0);
    return frames;
  }
}

export class FrameError extends Error {
  /**
   * @param {string} message
   */
  constructor(message) {
    super(message);
    this.name = 'FrameError';
    this.code = constants.FRAME_ERROR;
  }
}
