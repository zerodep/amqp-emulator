import { methods, methodsById, properties, constants } from './defs.js';

const FRAME_END = constants.FRAME_END;

/** Growable big-endian buffer writer */
export class BufferWriter {
  /**
   * @param {number} [size] initial size in bytes
   */
  constructor(size = 256) {
    /** @type {Buffer} */
    this.buf = Buffer.allocUnsafe(size);
    this.offset = 0;
  }
  /**
   * Make room for n more bytes
   * @param {number} n
   */
  ensure(n) {
    const need = this.offset + n;
    if (need <= this.buf.length) return;
    let size = this.buf.length * 2;
    while (size < need) size *= 2;
    const nb = Buffer.allocUnsafe(size);
    this.buf.copy(nb, 0, 0, this.offset);
    this.buf = nb;
  }
  /** @param {number} v */
  octet(v) {
    this.ensure(1);
    this.buf.writeUInt8(v, this.offset);
    this.offset += 1;
  }
  /** @param {number} v */
  short(v) {
    this.ensure(2);
    this.buf.writeUInt16BE(v, this.offset);
    this.offset += 2;
  }
  /** @param {number} v */
  long(v) {
    this.ensure(4);
    this.buf.writeUInt32BE(v, this.offset);
    this.offset += 4;
  }
  /** @param {number | bigint} v */
  longlong(v) {
    this.ensure(8);
    this.buf.writeBigUInt64BE(BigInt(v), this.offset);
    this.offset += 8;
  }
  /** @param {string} [v] */
  shortstr(v) {
    if (v === undefined || v === null) v = '';
    v = String(v);
    const len = Buffer.byteLength(v, 'utf8');
    if (len > 255) throw new TypeError(`shortstr too long (${len} bytes): ${v.slice(0, 40)}...`);
    this.ensure(1 + len);
    this.buf.writeUInt8(len, this.offset);
    this.buf.write(v, this.offset + 1, 'utf8');
    this.offset += 1 + len;
  }
  /** @param {string | Buffer} [v] */
  longstr(v) {
    if (v === undefined || v === null) v = '';
    const b = Buffer.isBuffer(v) ? v : Buffer.from(String(v), 'utf8');
    this.ensure(4 + b.length);
    this.buf.writeUInt32BE(b.length, this.offset);
    b.copy(this.buf, this.offset + 4);
    this.offset += 4 + b.length;
  }
  /** @param {Buffer} b */
  raw(b) {
    this.ensure(b.length);
    b.copy(this.buf, this.offset);
    this.offset += b.length;
  }
  /** @param {Record<string, any>} [v] */
  table(v) {
    const start = this.offset;
    this.long(0);
    if (v) {
      for (const key of Object.keys(v)) {
        const val = v[key];
        if (val === undefined) continue;
        this.shortstr(key);
        this.fieldValue(val);
      }
    }
    this.buf.writeUInt32BE(this.offset - start - 4, start);
  }
  /** @param {any[]} v */
  array(v) {
    const start = this.offset;
    this.long(0);
    for (const val of v) this.fieldValue(val);
    this.buf.writeUInt32BE(this.offset - start - 4, start);
  }
  /** @param {string} t single char type tag */
  tag(t) {
    this.octet(t.charCodeAt(0));
  }
  /**
   * Write a tagged field table value. Numbers are encoded as the smallest signed integer that fits,
   * or as a double when fractional. Use `{ '!': type, value }` to force a type, like amqplib.
   * @param {any} value
   */
  fieldValue(value) {
    /** @type {string} */
    let type = typeof value;
    let val = value;
    if (value && type === 'object' && Object.hasOwn(value, '!')) {
      type = value['!'];
      val = value.value;
    }
    if (type === 'number') {
      if (val >= 0x8000000000000000 || (Math.abs(val) < 0x4000000000000 && Math.floor(val) !== val)) type = 'double';
      else if (val >= -128 && val < 128) type = 'byte';
      else if (val >= -0x8000 && val < 0x8000) type = 'short';
      else if (val >= -0x80000000 && val < 0x80000000) type = 'int';
      else type = 'long';
    }
    switch (type) {
      case 'string':
        this.tag('S');
        this.longstr(val);
        break;
      case 'bigint':
        this.tag('l');
        this.ensure(8);
        this.buf.writeBigInt64BE(val, this.offset);
        this.offset += 8;
        break;
      case 'object':
        if (val === null) this.tag('V');
        else if (Array.isArray(val)) {
          this.tag('A');
          this.array(val);
        } else if (Buffer.isBuffer(val)) {
          this.tag('x');
          this.longstr(val);
        } else if (val instanceof Date) {
          this.tag('T');
          this.longlong(Math.floor(val.getTime() / 1000));
        } else {
          this.tag('F');
          this.table(val);
        }
        break;
      case 'boolean':
        this.tag('t');
        this.octet(val ? 1 : 0);
        break;
      case 'double':
      case 'float64':
        this.tag('d');
        this.ensure(8);
        this.buf.writeDoubleBE(val, this.offset);
        this.offset += 8;
        break;
      case 'float':
        this.tag('f');
        this.ensure(4);
        this.buf.writeFloatBE(val, this.offset);
        this.offset += 4;
        break;
      case 'byte':
      case 'int8':
        this.tag('b');
        this.ensure(1);
        this.buf.writeInt8(val, this.offset);
        this.offset += 1;
        break;
      case 'unsignedbyte':
      case 'uint8':
        this.tag('B');
        this.octet(val);
        break;
      case 'short':
      case 'int16':
        this.tag('s');
        this.ensure(2);
        this.buf.writeInt16BE(val, this.offset);
        this.offset += 2;
        break;
      case 'unsignedshort':
      case 'uint16':
        this.tag('u');
        this.short(val);
        break;
      case 'int':
      case 'int32':
        this.tag('I');
        this.ensure(4);
        this.buf.writeInt32BE(val, this.offset);
        this.offset += 4;
        break;
      case 'unsignedint':
      case 'uint32':
        this.tag('i');
        this.long(val);
        break;
      case 'long':
      case 'int64':
        this.tag('l');
        this.ensure(8);
        this.buf.writeBigInt64BE(BigInt(val), this.offset);
        this.offset += 8;
        break;
      case 'timestamp':
        this.tag('T');
        this.longlong(val);
        break;
      case 'decimal':
        this.tag('D');
        this.octet(val.places);
        this.long(val.digits);
        break;
      default:
        throw new TypeError(`Unknown field value type to encode: ${type}`);
    }
  }
  /** The written bytes */
  toBuffer() {
    return this.buf.subarray(0, this.offset);
  }
}

/** Sequential big-endian reader */
export class BufferReader {
  /**
   * @param {Buffer} buf
   * @param {number} [offset]
   */
  constructor(buf, offset = 0) {
    this.buf = buf;
    this.offset = offset;
  }
  octet() {
    const v = this.buf.readUInt8(this.offset);
    this.offset += 1;
    return v;
  }
  short() {
    const v = this.buf.readUInt16BE(this.offset);
    this.offset += 2;
    return v;
  }
  long() {
    const v = this.buf.readUInt32BE(this.offset);
    this.offset += 4;
    return v;
  }
  longlong() {
    const v = Number(this.buf.readBigUInt64BE(this.offset));
    this.offset += 8;
    return v;
  }
  shortstr() {
    const len = this.octet();
    const v = this.buf.toString('utf8', this.offset, this.offset + len);
    this.offset += len;
    return v;
  }
  longstr() {
    const len = this.long();
    const v = this.buf.subarray(this.offset, this.offset + len);
    this.offset += len;
    return v;
  }
  /** @returns {Record<string, any>} */
  table() {
    const len = this.long();
    const end = this.offset + len;
    /** @type {Record<string, any>} */
    const result = {};
    while (this.offset < end) {
      const key = this.shortstr();
      result[key] = this.fieldValue();
    }
    this.offset = end;
    return result;
  }
  /** @returns {any[]} */
  array() {
    const len = this.long();
    const end = this.offset + len;
    const result = [];
    while (this.offset < end) result.push(this.fieldValue());
    this.offset = end;
    return result;
  }
  /**
   * Read a tagged field table value. Timestamps and decimals come back as `{ '!': type, value }`, like amqplib.
   * @returns {any}
   */
  fieldValue() {
    const tag = String.fromCharCode(this.octet());
    let v;
    switch (tag) {
      case 't':
        return this.octet() !== 0;
      case 'b':
        v = this.buf.readInt8(this.offset);
        this.offset += 1;
        return v;
      case 'B':
        return this.octet();
      case 's':
        v = this.buf.readInt16BE(this.offset);
        this.offset += 2;
        return v;
      case 'u':
        return this.short();
      case 'I':
        v = this.buf.readInt32BE(this.offset);
        this.offset += 4;
        return v;
      case 'i':
        return this.long();
      case 'l':
        v = Number(this.buf.readBigInt64BE(this.offset));
        this.offset += 8;
        return v;
      case 'f':
        v = this.buf.readFloatBE(this.offset);
        this.offset += 4;
        return v;
      case 'd':
        v = this.buf.readDoubleBE(this.offset);
        this.offset += 8;
        return v;
      case 'D': {
        const places = this.octet();
        const digits = this.long();
        return { '!': 'decimal', value: { places, digits } };
      }
      case 'S':
        return this.longstr().toString('utf8');
      case 'x':
        return Buffer.from(this.longstr());
      case 'A':
        return this.array();
      case 'T':
        return { '!': 'timestamp', value: this.longlong() };
      case 'F':
        return this.table();
      case 'V':
        return null;
      default:
        throw new TypeError(`Unexpected field value type tag "${tag}"`);
    }
  }
}

/**
 * @param {BufferWriter} w
 * @param {import('types').MethodDef['args']} args
 * @param {import('types').MethodFields} fields
 */
function writeArgs(w, args, fields) {
  let bits = 0;
  let bitCount = 0;
  for (const [name, type, def] of args) {
    let val = fields[name];
    if (val === undefined || val === null) val = def;
    if (type === 'bit') {
      // no AMQP 0-9-1 method has more than 8 consecutive bits, the generator asserts this
      if (val) bits |= 1 << bitCount;
      bitCount++;
      continue;
    }
    if (bitCount) {
      w.octet(bits);
      bits = 0;
      bitCount = 0;
    }
    switch (type) {
      case 'octet':
        w.octet(val);
        break;
      case 'short':
        w.short(val ?? 0);
        break;
      case 'long':
        w.long(val ?? 0);
        break;
      case 'longlong':
      case 'timestamp':
        w.longlong(val ?? 0);
        break;
      case 'shortstr':
        w.shortstr(val);
        break;
      case 'longstr':
        w.longstr(val);
        break;
      case 'table':
        w.table(val);
        break;
    }
  }
  if (bitCount) w.octet(bits);
}

/**
 * @param {BufferReader} r
 * @param {import('types').MethodDef['args']} args
 * @returns {import('types').MethodFields}
 */
function readArgs(r, args) {
  /** @type {import('types').MethodFields} */
  const fields = {};
  let bits = 0;
  let bitCount = 0;
  for (const [name, type] of args) {
    if (type === 'bit') {
      if (bitCount === 0) bits = r.octet();
      fields[name] = (bits & (1 << bitCount)) !== 0;
      bitCount++;
      continue;
    }
    bitCount = 0;
    switch (type) {
      case 'octet':
        fields[name] = r.octet();
        break;
      case 'short':
        fields[name] = r.short();
        break;
      case 'long':
        fields[name] = r.long();
        break;
      case 'longlong':
      case 'timestamp':
        fields[name] = r.longlong();
        break;
      case 'shortstr':
        fields[name] = r.shortstr();
        break;
      case 'longstr':
        fields[name] = r.longstr();
        break;
      case 'table':
        fields[name] = r.table();
        break;
    }
  }
  return fields;
}

/**
 * @param {BufferWriter} w
 * @param {number} type
 * @param {number} channel
 */
function frameHeader(w, type, channel) {
  w.octet(type);
  w.short(channel);
  w.long(0); // size placeholder
}

/** @param {BufferWriter} w */
function frameEnd(w) {
  w.buf.writeUInt32BE(w.offset - 7, 3);
  w.octet(FRAME_END);
}

/**
 * Encode a complete method frame
 * @param {import('types').AmqpMethodName} methodName e.g. 'basic.deliver'
 * @param {number} channel
 * @param {import('types').MethodFields} [fields]
 */
export function encodeMethod(methodName, channel, fields = {}) {
  const def = methods[methodName];
  if (!def) throw new TypeError(`Unknown AMQP method ${methodName}`);
  const w = new BufferWriter();
  frameHeader(w, constants.FRAME_METHOD, channel);
  w.short(def.classId);
  w.short(def.methodId);
  writeArgs(w, def.args, fields);
  frameEnd(w);
  return w.toBuffer();
}

/**
 * Decode a method frame payload
 * @param {Buffer} payload
 * @returns {import('types').DecodedMethod}
 */
export function decodeMethod(payload) {
  const r = new BufferReader(payload);
  const classId = r.short();
  const methodId = r.short();
  const method = methodsById.get((classId << 16) | methodId);
  if (!method) throw new TypeError(`Unknown AMQP method class ${classId} method ${methodId}`);
  return { method, fields: readArgs(r, method.args) };
}

/**
 * Encode a content header frame
 * @param {number} channel
 * @param {number} bodySize
 * @param {import('types').MessageProperties} [props] basic properties, unknown keys are ignored
 * @param {number} [classId] defaults to basic (60)
 */
export function encodeProperties(channel, bodySize, props = {}, classId = 60) {
  const def = properties[classId];
  const w = new BufferWriter();
  frameHeader(w, constants.FRAME_HEADER, channel);
  w.short(classId);
  w.short(0); // weight
  w.longlong(bodySize);
  const flagsOffset = w.offset;
  w.short(0);
  let flags = 0;
  def.fields.forEach(([name, type], idx) => {
    const val = props[name];
    if (val === undefined || val === null) return;
    flags |= 1 << (15 - idx);
    switch (type) {
      case 'octet':
        w.octet(val);
        break;
      case 'shortstr':
        w.shortstr(val);
        break;
      case 'table':
        w.table(val);
        break;
      case 'timestamp':
        w.longlong(val);
        break;
    }
  });
  w.buf.writeUInt16BE(flags, flagsOffset);
  frameEnd(w);
  return w.toBuffer();
}

/**
 * Decode a content header frame payload
 * @param {Buffer} payload
 * @returns {import('types').DecodedProperties}
 */
export function decodeProperties(payload) {
  const r = new BufferReader(payload);
  const classId = r.short();
  r.short(); // weight
  const bodySize = r.longlong();
  const flags = r.short();
  const def = properties[classId];
  if (!def) throw new TypeError(`Unknown content class ${classId}`);
  /** @type {import('types').MessageProperties} */
  const props = {};
  def.fields.forEach(([name, type], idx) => {
    if (!(flags & (1 << (15 - idx)))) return;
    switch (type) {
      case 'octet':
        props[name] = r.octet();
        break;
      case 'shortstr':
        props[name] = r.shortstr();
        break;
      case 'table':
        props[name] = r.table();
        break;
      case 'timestamp':
        props[name] = r.longlong();
        break;
    }
  });
  return { classId, bodySize, properties: props };
}

/**
 * Encode a content body frame
 * @param {number} channel
 * @param {Buffer} content
 */
export function encodeBody(channel, content) {
  const w = new BufferWriter(content.length + 8);
  frameHeader(w, constants.FRAME_BODY, channel);
  w.raw(content);
  frameEnd(w);
  return w.toBuffer();
}

export const HEARTBEAT_FRAME = Buffer.from([constants.FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END]);

export const PROTOCOL_HEADER = Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary');
