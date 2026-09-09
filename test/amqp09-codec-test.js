import {
  encodeMethod,
  decodeMethod,
  encodeProperties,
  decodeProperties,
  BufferWriter,
  BufferReader,
  FrameParser,
  constants,
  methods,
} from 'amqp-emulator';

describe('codec', () => {
  describe('field tables', () => {
    it('round trips every value type', () => {
      const table = {
        str: 'text',
        bool: true,
        nil: null,
        byte: 5,
        neg: -5,
        short: 300,
        int: 70000,
        long: 3000000000,
        bigint: 2n ** 40n,
        float: 1.25,
        buf: Buffer.from('bytes'),
        list: [1, 'two', [3], { four: 4 }],
        nested: { deep: { deeper: 'yes' } },
        timestamp: { '!': 'timestamp', value: 1700000000 },
        decimal: { '!': 'decimal', value: { places: 2, digits: 314 } },
        date: new Date(1700000000000),
        uint: { '!': 'unsignedint', value: 4000000000 },
        ushort: { '!': 'unsignedshort', value: 65000 },
        ubyte: { '!': 'unsignedbyte', value: 250 },
        f32: { '!': 'float', value: 0.5 },
        skipped: undefined,
      };
      const writer = new BufferWriter(16);
      writer.table(table);
      const decoded = new BufferReader(writer.toBuffer()).table();
      expect(decoded).to.deep.equal({
        str: 'text',
        bool: true,
        nil: null,
        byte: 5,
        neg: -5,
        short: 300,
        int: 70000,
        long: 3000000000,
        bigint: 2 ** 40,
        float: 1.25,
        buf: Buffer.from('bytes'),
        list: [1, 'two', [3], { four: 4 }],
        nested: { deep: { deeper: 'yes' } },
        timestamp: { '!': 'timestamp', value: 1700000000 },
        decimal: { '!': 'decimal', value: { places: 2, digits: 314 } },
        date: { '!': 'timestamp', value: 1700000000 },
        uint: 4000000000,
        ushort: 65000,
        ubyte: 250,
        f32: 0.5,
      });
    });

    it('grows the buffer as needed', () => {
      const writer = new BufferWriter(4);
      writer.longstr('x'.repeat(100));
      writer.shortstr(null);
      writer.longstr(null);
      expect(writer.toBuffer().length).to.equal(4 + 100 + 1 + 4);
    });

    it('rejects unknown explicit types', () => {
      const writer = new BufferWriter();
      expect(() => writer.table({ x: { '!': 'nope', value: 1 } })).to.throw(TypeError, /Unknown field value type/);
    });

    it('rejects unknown type tags when decoding', () => {
      const reader = new BufferReader(Buffer.from([0, 0, 0, 3, 1, 0x61, 0x5a]));
      expect(() => reader.table()).to.throw(TypeError, /Unexpected field value type tag "Z"/);
    });

    it('rejects too long short strings', () => {
      const writer = new BufferWriter();
      expect(() => writer.shortstr('x'.repeat(256))).to.throw(TypeError, /shortstr too long/);
    });
  });

  describe('methods', () => {
    it('packs consecutive bits into one octet', () => {
      const frame = encodeMethod('queue.declare', 1, {
        queue: 'q',
        passive: false,
        durable: true,
        exclusive: false,
        autoDelete: true,
        nowait: false,
        arguments: {},
      });
      // frame header (7) + class (2) + method (2) + ticket (2) + queue shortstr (2) + bits (1) + table (4) + end (1)
      expect(frame.length).to.equal(21);
      const { method, fields } = decodeMethod(frame.subarray(7, -1));
      expect(method).to.equal(methods['queue.declare']);
      expect(fields).to.deep.equal({
        ticket: 0,
        queue: 'q',
        passive: false,
        durable: true,
        exclusive: false,
        autoDelete: true,
        nowait: false,
        arguments: {},
      });
    });

    it('applies defaults for missing fields', () => {
      const frame = encodeMethod('basic.consume', 2, { queue: 'q' });
      const { fields } = decodeMethod(frame.subarray(7, -1));
      expect(fields.consumerTag).to.equal('');
      expect(fields.noAck).to.equal(false);
      expect(fields.arguments).to.deep.equal({});
    });

    it('encodes zero values for fields without a spec default', () => {
      const deliver = decodeMethod(encodeMethod('basic.deliver', 1).subarray(7, -1)).fields;
      expect(deliver).to.deep.equal({ consumerTag: '', deliveryTag: 0, redelivered: false, exchange: '', routingKey: '' });
      const getOk = decodeMethod(encodeMethod('basic.get-ok', 1).subarray(7, -1)).fields;
      expect(getOk.messageCount).to.equal(0);
      const close = decodeMethod(encodeMethod('connection.close', 0).subarray(7, -1)).fields;
      expect(close).to.deep.equal({ replyCode: 0, replyText: '', classId: 0, methodId: 0 });
      const start = decodeMethod(encodeMethod('connection.start', 0).subarray(7, -1)).fields;
      expect(start.versionMajor).to.equal(0);
      expect(start.versionMinor).to.equal(9);
    });

    it('decodes octet fields', () => {
      const frame = encodeMethod('connection.start', 0, { versionMajor: 0, versionMinor: 9, mechanisms: 'PLAIN', locales: 'en_US' });
      const { fields } = decodeMethod(frame.subarray(7, -1));
      expect(fields.versionMajor).to.equal(0);
      expect(fields.versionMinor).to.equal(9);
      expect(fields.mechanisms.toString()).to.equal('PLAIN');
    });

    it('encodes longlong fields', () => {
      const frame = encodeMethod('basic.ack', 1, { deliveryTag: 2 ** 40, multiple: true });
      const { fields } = decodeMethod(frame.subarray(7, -1));
      expect(fields).to.deep.equal({ deliveryTag: 2 ** 40, multiple: true });
    });

    it('throws on unknown method names and ids', () => {
      expect(() => encodeMethod(/** @type {any} */ ('basic.nope'), 1)).to.throw(TypeError, /Unknown AMQP method basic.nope/);
      expect(() => decodeMethod(Buffer.from([0, 99, 0, 99]))).to.throw(TypeError, /Unknown AMQP method class 99 method 99/);
    });
  });

  describe('properties', () => {
    it('round trips basic properties and ignores unknown keys', () => {
      const frame = encodeProperties(3, 42, {
        contentType: 'text/plain',
        headers: { h: 'v' },
        deliveryMode: 2,
        timestamp: 1700000000,
        clusterId: 'c',
        mandatory: true,
        smqpOnly: 'ignored',
      });
      const decoded = decodeProperties(frame.subarray(7, -1));
      expect(decoded).to.deep.equal({
        classId: 60,
        bodySize: 42,
        properties: { contentType: 'text/plain', headers: { h: 'v' }, deliveryMode: 2, timestamp: 1700000000, clusterId: 'c' },
      });
    });

    it('encodes no flags when there are no properties', () => {
      const frame = encodeProperties(3, 0, {});
      expect(frame.length).to.equal(7 + 2 + 2 + 8 + 2 + 1);
      expect(decodeProperties(frame.subarray(7, -1)).properties).to.deep.equal({});
    });

    it('throws on unknown content class', () => {
      expect(() => decodeProperties(Buffer.from([0, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))).to.throw(
        TypeError,
        /Unknown content class 99/,
      );
    });
  });

  describe('frame parser', () => {
    it('parses frames arriving one byte at a time', () => {
      const frame = encodeMethod('channel.open', 5, {});
      const parser = new FrameParser();
      const frames = [];
      for (const byte of frame) frames.push(...parser.feed(Buffer.from([byte])));
      expect(frames.length).to.equal(1);
      expect(frames[0]).to.include({ type: constants.FRAME_METHOD, channel: 5 });
      expect(decodeMethod(frames[0].payload).method.name).to.equal('channel.open');
    });

    it('parses several frames in one chunk and keeps the remainder', () => {
      const a = encodeMethod('channel.open', 1, {});
      const b = encodeMethod('channel.flow', 2, { active: true });
      const parser = new FrameParser();
      const chunk = Buffer.concat([a, b.subarray(0, 4)]);
      expect(parser.feed(chunk).length).to.equal(1);
      const rest = parser.feed(b.subarray(4));
      expect(rest.length).to.equal(1);
      expect(decodeMethod(rest[0].payload).fields).to.deep.equal({ active: true });
    });

    it('throws a frame error on a bad frame end marker', () => {
      const frame = encodeMethod('channel.open', 1, {});
      frame[frame.length - 1] = 0;
      const parser = new FrameParser();
      expect(() => parser.feed(frame)).to.throw(/Invalid frame end marker/);
    });
  });
});
