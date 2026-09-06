import { BufferWriter, BufferReader } from '../amqp09/codec.js';
import { composites, compositesByDescriptor, restricted, restrictedByDescriptor } from './defs.js';

/** @type {Map<string, import('types').Amqp10Composite>} */
const compositesBySymbol = new Map(Object.values(composites).map((c) => [c.symbol, c]));
/** @type {Map<string, import('types').Amqp10Restricted>} */
const restrictedBySymbol = new Map([...restrictedByDescriptor.values()].map((r) => [r.symbol, r]));

/** restricted map types keyed by symbols rather than strings */
const SYMBOL_KEYED = new Set([
  'fields',
  'annotations',
  'filter-set',
  'node-properties',
  'delivery-annotations',
  'message-annotations',
  'footer',
]);

export const AMQP_HEADER = Buffer.from([65, 77, 81, 80, 0, 1, 0, 0]);
export const SASL_HEADER = Buffer.from([65, 77, 81, 80, 3, 1, 0, 0]);
export const FRAME_TYPE_AMQP = 0;
export const FRAME_TYPE_SASL = 1;

/**
 * Force an encoding type for a value in a `*` position, e.g. `typed('ulong', 5)`
 */
export class Typed {
  /**
   * @param {string} type
   * @param {any} value
   */
  constructor(type, value) {
    this.type = type;
    this.value = value;
  }
}

/**
 * @param {string} type
 * @param {any} value
 */
export function typed(type, value) {
  return new Typed(type, value);
}

/**
 * Resolve restricted type names to their primitive source
 * @param {string} type
 */
function resolve(type) {
  let name = type;
  while (restricted[name]) name = restricted[name].source;
  return name;
}

/**
 * @param {BufferWriter} w
 * @param {number | string | Typed} descriptor numeric, or a symbol given as string or typed value
 */
function writeDescriptor(w, descriptor) {
  w.octet(0x00);
  if (typeof descriptor === 'number') return writeUlong(w, descriptor);
  encodeValue(w, descriptor, typeof descriptor === 'string' ? 'symbol' : '*');
}

/**
 * @param {BufferWriter} w
 * @param {number | bigint} value
 */
function writeUlong(w, value) {
  if (value === 0 || value === 0n) return w.octet(0x44);
  if (value < 256) {
    w.octet(0x53);
    return w.octet(Number(value));
  }
  w.octet(0x80);
  w.longlong(value);
}

/**
 * Write a size prefixed compound (list, map or array) and patch the size afterwards
 * @param {BufferWriter} w
 * @param {number} constructor 0xd0 list32, 0xd1 map32 or 0xf0 array32
 * @param {number} count
 * @param {() => void} body
 */
function writeCompound(w, constructor, count, body) {
  w.octet(constructor);
  const start = w.offset;
  w.long(0);
  w.long(count);
  body();
  w.buf.writeUInt32BE(w.offset - start - 4, start);
}

/**
 * Infer an encoding type for a value in a `*` position
 * @param {any} value
 * @returns {string}
 */
function inferType(value) {
  if (value === null || value === undefined) return 'null';
  switch (typeof value) {
    case 'boolean':
      return 'boolean';
    case 'number':
      if (!Number.isInteger(value)) return 'double';
      return value >= -0x80000000 && value < 0x80000000 ? 'int' : 'long';
    case 'bigint':
      return 'long';
    case 'string':
      return 'string';
    case 'object':
      if (Buffer.isBuffer(value)) return 'binary';
      if (Array.isArray(value)) return 'list';
      if (value instanceof Date) return 'timestamp';
      if (value.$type) return value.$type;
      if ('$descriptor' in value) return 'described';
      return 'map';
    default:
      throw new TypeError(`Cannot encode ${typeof value} as an AMQP 1.0 value`);
  }
}

/**
 * Encode a value with the given spec type: a primitive, a restricted type, a composite, a section or `*`
 * @param {BufferWriter} w
 * @param {any} value
 * @param {string} [type]
 * @returns {void}
 */
export function encodeValue(w, value, type = '*') {
  if (value instanceof Typed) {
    type = value.type;
    value = value.value;
  }
  if (type === '*') type = inferType(value);
  if (value === null || value === undefined) return w.octet(0x40);

  const composite = composites[type];
  if (composite) return encodeComposite(w, composite, value);
  const keyType = SYMBOL_KEYED.has(type) ? 'symbol' : 'string';
  const section = restricted[type]?.descriptor !== undefined ? restricted[type] : undefined;
  if (section) {
    writeDescriptor(w, section.descriptor);
    return encodePrimitive(w, value.$type ? value.value : value, resolve(type), keyType);
  }
  if (type === 'described') {
    writeDescriptor(w, value.$descriptor);
    return encodeValue(w, value.value);
  }
  return encodePrimitive(w, value, resolve(type), keyType);
}

/**
 * @param {BufferWriter} w
 * @param {any} value
 * @param {string} type primitive type name or `*`
 * @param {'string' | 'symbol'} keyType map key type
 * @returns {void}
 */
function encodePrimitive(w, value, type, keyType) {
  if (type === '*') return encodeValue(w, value);
  if (value === null || value === undefined) return w.octet(0x40);
  switch (type) {
    case 'null':
      return w.octet(0x40);
    case 'boolean':
      return w.octet(value ? 0x41 : 0x42);
    case 'ubyte':
      w.octet(0x50);
      return w.octet(value);
    case 'ushort':
      w.octet(0x60);
      return w.short(value);
    case 'uint':
      if (value === 0) return w.octet(0x43);
      if (value < 256) {
        w.octet(0x52);
        return w.octet(value);
      }
      w.octet(0x70);
      return w.long(value);
    case 'ulong':
      return writeUlong(w, value);
    case 'byte':
      w.octet(0x51);
      w.ensure(1);
      w.buf.writeInt8(value, w.offset);
      w.offset += 1;
      return;
    case 'short':
      w.octet(0x61);
      w.ensure(2);
      w.buf.writeInt16BE(value, w.offset);
      w.offset += 2;
      return;
    case 'int':
      if (value >= -128 && value < 128) {
        w.octet(0x54);
        w.ensure(1);
        w.buf.writeInt8(value, w.offset);
        w.offset += 1;
        return;
      }
      w.octet(0x71);
      w.ensure(4);
      w.buf.writeInt32BE(value, w.offset);
      w.offset += 4;
      return;
    case 'long':
      if (typeof value === 'number' && value >= -128 && value < 128) {
        w.octet(0x55);
        w.ensure(1);
        w.buf.writeInt8(value, w.offset);
        w.offset += 1;
        return;
      }
      w.octet(0x81);
      w.ensure(8);
      w.buf.writeBigInt64BE(BigInt(value), w.offset);
      w.offset += 8;
      return;
    case 'float':
      w.octet(0x72);
      w.ensure(4);
      w.buf.writeFloatBE(value, w.offset);
      w.offset += 4;
      return;
    case 'double':
      w.octet(0x82);
      w.ensure(8);
      w.buf.writeDoubleBE(value, w.offset);
      w.offset += 8;
      return;
    case 'char':
      w.octet(0x73);
      return w.long(typeof value === 'string' ? value.codePointAt(0) : value);
    case 'timestamp':
      w.octet(0x83);
      w.ensure(8);
      w.buf.writeBigInt64BE(BigInt(value instanceof Date ? value.getTime() : value), w.offset);
      w.offset += 8;
      return;
    case 'uuid':
      w.octet(0x98);
      return w.raw(value);
    case 'binary':
      return writeVariable(w, 0xa0, 0xb0, Buffer.isBuffer(value) ? value : Buffer.from(value));
    case 'string':
      return writeVariable(w, 0xa1, 0xb1, Buffer.from(String(value), 'utf8'));
    case 'symbol':
      return writeVariable(w, 0xa3, 0xb3, Buffer.from(String(value), 'ascii'));
    case 'list':
      if (!value.length) return w.octet(0x45);
      return writeCompound(w, 0xd0, value.length, () => {
        for (const item of value) encodeValue(w, item);
      });
    case 'map': {
      const entries = value instanceof Map ? [...value] : Object.entries(value).filter(([, v]) => v !== undefined);
      return writeCompound(w, 0xd1, entries.length * 2, () => {
        for (const [key, val] of entries) {
          encodeValue(w, key, value instanceof Map ? '*' : keyType);
          encodeValue(w, val);
        }
      });
    }
    default:
      throw new TypeError(`Unknown AMQP 1.0 type ${type}`);
  }
}

/**
 * @param {BufferWriter} w
 * @param {number} smallCode
 * @param {number} largeCode
 * @param {Buffer} bytes
 */
function writeVariable(w, smallCode, largeCode, bytes) {
  if (bytes.length < 256) {
    w.octet(smallCode);
    w.octet(bytes.length);
  } else {
    w.octet(largeCode);
    w.long(bytes.length);
  }
  w.raw(bytes);
}

/**
 * Encode an array of symbols, the only array the spec uses (`multiple` fields are all symbols)
 * @param {BufferWriter} w
 * @param {any[]} values
 */
function encodeArray(w, values) {
  writeCompound(w, 0xf0, values.length, () => {
    w.octet(0xb3);
    for (const value of values) {
      const bytes = Buffer.from(String(value), 'ascii');
      w.long(bytes.length);
      w.raw(bytes);
    }
  });
}

/**
 * Encode a composite (described list) from an object with the composite field names
 * @param {BufferWriter} w
 * @param {import('types').Amqp10Composite} composite
 * @param {Record<string, any>} value
 */
function encodeComposite(w, composite, value) {
  writeDescriptor(w, composite.descriptor);
  const fields = composite.fields;
  let count = fields.length;
  while (count > 0 && (value[fields[count - 1].name] === undefined || value[fields[count - 1].name] === null)) count--;
  if (!count) return w.octet(0x45);
  writeCompound(w, 0xd0, count, () => {
    for (let i = 0; i < count; i++) {
      const field = fields[i];
      const fieldValue = value[field.name];
      if (field.multiple && Array.isArray(fieldValue)) {
        if (fieldValue.length === 1) encodeValue(w, fieldValue[0], field.type);
        else encodeArray(w, fieldValue);
      } else {
        encodeValue(w, fieldValue, field.type);
      }
    }
  });
}

/**
 * Encode a value to a buffer
 * @param {any} value
 * @param {string} [type]
 */
export function encode(value, type) {
  const w = new BufferWriter();
  encodeValue(w, value, type);
  return w.toBuffer();
}

/**
 * Decode the next value
 * @param {BufferReader} r
 * @returns {any}
 */
export function decodeValue(r) {
  const constructor = r.octet();
  if (constructor === 0x00) {
    const descriptor = decodeValue(r);
    const value = decodeValue(r);
    return describe(descriptor, value);
  }
  return decodeBody(r, constructor);
}

/**
 * Turn a described value into an object with `$type` when the descriptor is known
 * @param {number | string} descriptor
 * @param {any} value
 * @returns {import('types').Amqp10Described}
 */
function describe(descriptor, value) {
  const composite = typeof descriptor === 'number' ? compositesByDescriptor.get(descriptor) : compositesBySymbol.get(descriptor);
  if (composite) {
    /** @type {Record<string, any>} */
    const result = { $type: composite.name };
    const list = Array.isArray(value) ? value : [];
    composite.fields.forEach((field, idx) => {
      if (list[idx] !== undefined && list[idx] !== null) result[field.name] = list[idx];
    });
    return /** @type {import('types').Amqp10Described} */ (result);
  }
  const section = typeof descriptor === 'number' ? restrictedByDescriptor.get(descriptor) : restrictedBySymbol.get(descriptor);
  if (section) return { $type: section.name, value };
  return { $descriptor: descriptor, value };
}

/**
 * @param {BufferReader} r
 * @param {number} constructor
 * @returns {any}
 */
function decodeBody(r, constructor) {
  let v;
  switch (constructor) {
    case 0x40:
      return null;
    case 0x41:
      return true;
    case 0x42:
      return false;
    case 0x56:
      return r.octet() !== 0;
    case 0x50:
      return r.octet();
    case 0x60:
      return r.short();
    case 0x70:
      return r.long();
    case 0x52:
      return r.octet();
    case 0x43:
      return 0;
    case 0x80:
      return r.longlong();
    case 0x53:
      return r.octet();
    case 0x44:
      return 0;
    case 0x51:
      v = r.buf.readInt8(r.offset);
      r.offset += 1;
      return v;
    case 0x61:
      v = r.buf.readInt16BE(r.offset);
      r.offset += 2;
      return v;
    case 0x71:
      v = r.buf.readInt32BE(r.offset);
      r.offset += 4;
      return v;
    case 0x54:
      v = r.buf.readInt8(r.offset);
      r.offset += 1;
      return v;
    case 0x81:
      v = Number(r.buf.readBigInt64BE(r.offset));
      r.offset += 8;
      return v;
    case 0x55:
      v = r.buf.readInt8(r.offset);
      r.offset += 1;
      return v;
    case 0x72:
      v = r.buf.readFloatBE(r.offset);
      r.offset += 4;
      return v;
    case 0x82:
      v = r.buf.readDoubleBE(r.offset);
      r.offset += 8;
      return v;
    case 0x74:
      r.offset += 4;
      return null;
    case 0x84:
      r.offset += 8;
      return null;
    case 0x94:
      r.offset += 16;
      return null;
    case 0x73:
      return String.fromCodePoint(r.long());
    case 0x83:
      v = Number(r.buf.readBigInt64BE(r.offset));
      r.offset += 8;
      return v;
    case 0x98:
      v = Buffer.from(r.buf.subarray(r.offset, r.offset + 16));
      r.offset += 16;
      return v;
    case 0xa0:
      return Buffer.from(readBytes(r, r.octet()));
    case 0xb0:
      return Buffer.from(readBytes(r, r.long()));
    case 0xa1:
      return readBytes(r, r.octet()).toString('utf8');
    case 0xb1:
      return readBytes(r, r.long()).toString('utf8');
    case 0xa3:
      return readBytes(r, r.octet()).toString('ascii');
    case 0xb3:
      return readBytes(r, r.long()).toString('ascii');
    case 0x45:
      return [];
    case 0xc0:
      return decodeList(r, r.octet(), r.octet(), 1);
    case 0xd0:
      return decodeList(r, r.long(), r.long(), 4);
    case 0xc1:
      return decodeMap(r, r.octet(), r.octet(), 1);
    case 0xd1:
      return decodeMap(r, r.long(), r.long(), 4);
    case 0xe0:
      return decodeArray(r, r.octet(), r.octet(), 1);
    case 0xf0:
      return decodeArray(r, r.long(), r.long(), 4);
    default:
      throw new TypeError(`Unknown AMQP 1.0 type constructor 0x${constructor.toString(16)}`);
  }
}

/**
 * @param {BufferReader} r
 * @param {number} length
 */
function readBytes(r, length) {
  const bytes = r.buf.subarray(r.offset, r.offset + length);
  r.offset += length;
  return bytes;
}

/**
 * @param {BufferReader} r
 * @param {number} size bytes following the size field, i.e. count and elements
 * @param {number} count
 * @param {number} width byte width of the count field
 */
function decodeList(r, size, count, width) {
  const end = r.offset + size - width;
  const list = [];
  for (let i = 0; i < count; i++) list.push(decodeValue(r));
  r.offset = end;
  return list;
}

/**
 * @param {BufferReader} r
 * @param {number} size
 * @param {number} count
 * @param {number} width byte width of the count field
 * @returns {Record<string, any>}
 */
function decodeMap(r, size, count, width) {
  const end = r.offset + size - width;
  /** @type {Record<string, any>} */
  const map = {};
  for (let i = 0; i < count; i += 2) {
    const key = decodeValue(r);
    map[typeof key === 'string' ? key : String(key)] = decodeValue(r);
  }
  r.offset = end;
  return map;
}

/**
 * @param {BufferReader} r
 * @param {number} size
 * @param {number} count
 * @param {number} width byte width of the count field
 */
function decodeArray(r, size, count, width) {
  const end = r.offset + size - width;
  let constructor = r.octet();
  let descriptor;
  if (constructor === 0x00) {
    descriptor = decodeValue(r);
    constructor = r.octet();
  }
  const values = [];
  for (let i = 0; i < count; i++) {
    const value = decodeBody(r, constructor);
    values.push(descriptor === undefined ? value : describe(descriptor, value));
  }
  r.offset = end;
  return values;
}

/**
 * Decode a single value from a buffer
 * @param {Buffer} buffer
 */
export function decode(buffer) {
  return decodeValue(new BufferReader(buffer));
}

/**
 * Encode a frame: 8 byte header, performative and payload. A null performative gives an empty (heartbeat) frame.
 * @param {number} type 0 amqp, 1 sasl
 * @param {number} channel
 * @param {import('types').Amqp10Described | null} performative
 * @param {Buffer} [payload]
 */
export function encodeFrame(type, channel, performative, payload) {
  const w = new BufferWriter();
  w.long(0);
  w.octet(2);
  w.octet(type);
  w.short(channel);
  if (performative) encodeValue(w, performative);
  if (payload) w.raw(payload);
  w.buf.writeUInt32BE(w.offset, 0);
  return w.toBuffer();
}

/**
 * Incremental AMQP 1.0 frame parser
 */
export class Amqp10FrameParser {
  constructor() {
    this.pending = Buffer.alloc(0);
  }
  /**
   * @param {Buffer} chunk
   * @returns {import('types').Amqp10Frame[]}
   */
  feed(chunk) {
    let buf = this.pending.length ? Buffer.concat([this.pending, chunk]) : chunk;
    /** @type {import('types').Amqp10Frame[]} */
    const frames = [];
    while (buf.length >= 8) {
      const size = buf.readUInt32BE(0);
      if (size < 8) throw new TypeError(`Invalid AMQP 1.0 frame size ${size}`);
      if (buf.length < size) break;
      const doff = buf[4] * 4;
      const body = buf.subarray(doff, size);
      let performative = null;
      let payload = body;
      if (body.length) {
        const r = new BufferReader(body);
        performative = decodeValue(r);
        payload = body.subarray(r.offset);
      }
      frames.push({ type: buf[5], channel: buf.readUInt16BE(6), performative, payload: Buffer.from(payload) });
      buf = buf.subarray(size);
    }
    this.pending = buf.length ? Buffer.from(buf) : Buffer.alloc(0);
    return frames;
  }
}
