import {
  encodeAmqp10 as encode,
  decodeAmqp10 as decode,
  amqp10Typed as typed,
  Amqp10FrameParser,
  encodeAmqp10Frame,
  resolveAmqp10Address,
} from '@0dep/amqp-emulator';

/** encode then decode */
function roundTrip(value, type) {
  return decode(encode(value, type));
}

describe('AMQP 1.0 codec', () => {
  describe('primitives', () => {
    it('round trips inferred values', () => {
      expect(roundTrip(null)).to.equal(null);
      expect(roundTrip(true)).to.equal(true);
      expect(roundTrip(false)).to.equal(false);
      expect(roundTrip(5)).to.equal(5);
      expect(roundTrip(-5)).to.equal(-5);
      expect(roundTrip(70000)).to.equal(70000);
      expect(roundTrip(2 ** 40)).to.equal(2 ** 40);
      expect(roundTrip(2n ** 40n)).to.equal(2 ** 40);
      expect(roundTrip(1.5)).to.equal(1.5);
      expect(roundTrip('text')).to.equal('text');
      expect(roundTrip('x'.repeat(300))).to.equal('x'.repeat(300));
      expect(roundTrip(Buffer.from('bytes'))).to.deep.equal(Buffer.from('bytes'));
      expect(roundTrip(Buffer.alloc(300, 1))).to.deep.equal(Buffer.alloc(300, 1));
      expect(roundTrip([1, 'two', [3]])).to.deep.equal([1, 'two', [3]]);
      expect(roundTrip([])).to.deep.equal([]);
      expect(roundTrip({ a: 1, b: 'x', skipped: undefined })).to.deep.equal({ a: 1, b: 'x' });
      expect(roundTrip(new Date(1700000000000))).to.equal(1700000000000);
    });

    it('round trips explicitly typed values', () => {
      expect(roundTrip(typed('ubyte', 200))).to.equal(200);
      expect(roundTrip(typed('ushort', 60000))).to.equal(60000);
      expect(roundTrip(typed('uint', 0))).to.equal(0);
      expect(roundTrip(typed('uint', 200))).to.equal(200);
      expect(roundTrip(typed('uint', 70000))).to.equal(70000);
      expect(roundTrip(typed('ulong', 0))).to.equal(0);
      expect(roundTrip(typed('ulong', 200))).to.equal(200);
      expect(roundTrip(typed('ulong', 2 ** 40))).to.equal(2 ** 40);
      expect(roundTrip(typed('byte', -100))).to.equal(-100);
      expect(roundTrip(typed('short', -30000))).to.equal(-30000);
      expect(roundTrip(typed('int', -100))).to.equal(-100);
      expect(roundTrip(typed('int', -70000))).to.equal(-70000);
      expect(roundTrip(typed('long', -100))).to.equal(-100);
      expect(roundTrip(typed('long', -(2 ** 40)))).to.equal(-(2 ** 40));
      expect(roundTrip(typed('long', 7n))).to.equal(7);
      expect(roundTrip(typed('float', 0.5))).to.equal(0.5);
      expect(roundTrip(typed('double', 0.1))).to.equal(0.1);
      expect(roundTrip(typed('char', 'A'))).to.equal('A');
      expect(roundTrip(typed('char', 66))).to.equal('B');
      expect(roundTrip(typed('timestamp', 1700000000000))).to.equal(1700000000000);
      expect(roundTrip(typed('uuid', Buffer.alloc(16, 7)))).to.deep.equal(Buffer.alloc(16, 7));
      expect(roundTrip(typed('symbol', 'sym'))).to.equal('sym');
      expect(roundTrip(typed('symbol', 's'.repeat(300)))).to.equal('s'.repeat(300));
      expect(roundTrip(typed('binary', 'as bytes'))).to.deep.equal(Buffer.from('as bytes'));
      expect(roundTrip(typed('boolean', 1))).to.equal(true);
      expect(roundTrip(typed('null', 'ignored'))).to.equal(null);
      expect(roundTrip(typed('string', null))).to.equal(null);
      expect(roundTrip(typed('seconds', 30))).to.equal(30);
    });

    it('encodes Map instances with typed keys', () => {
      const map = new Map(
        /** @type {Array<[any, any]>} */ ([
          [typed('symbol', 'x-opt'), 1],
          ['plain', true],
        ]),
      );
      expect(roundTrip(map)).to.deep.equal({ 'x-opt': 1, plain: true });
    });

    it('decodes compact encodings it does not produce itself', () => {
      expect(decode(Buffer.from([0x56, 0x01]))).to.equal(true);
      expect(decode(Buffer.from([0x44]))).to.equal(0);
      expect(decode(Buffer.from([0xc0, 0x05, 0x02, 0x54, 0x01, 0x54, 0x02]))).to.deep.equal([1, 2]);
      expect(decode(Buffer.from([0xc1, 0x06, 0x02, 0xa1, 0x01, 0x6b, 0x41]))).to.deep.equal({ k: true });
      expect(decode(Buffer.from([0xc1, 0x05, 0x02, 0x54, 0x01, 0x41]))).to.deep.equal({ 1: true });
      expect(decode(Buffer.from([0xe0, 0x04, 0x02, 0x50, 0x07, 0x08]))).to.deep.equal([7, 8]);
    });

    it('decodes decimals as null, arrays of described values and symbol descriptors', () => {
      expect(decode(Buffer.from([0x74, 0, 0, 0, 0]))).to.equal(null);
      expect(decode(Buffer.from([0x84, 0, 0, 0, 0, 0, 0, 0, 0]))).to.equal(null);
      expect(decode(Buffer.from([0x94, ...Buffer.alloc(16)]))).to.equal(null);
      // array32 of two described (accepted) empty lists
      const described = Buffer.from([0xf0, 0, 0, 0, 9, 0, 0, 0, 2, 0x00, 0x53, 0x24, 0x45, 0x45]);
      expect(decode(described)).to.deep.equal([{ $type: 'accepted' }, { $type: 'accepted' }]);
      // composite and section addressed by symbolic descriptor
      const symbolic = encode({ $descriptor: typed('symbol', 'amqp:accepted:list'), value: [] });
      expect(decode(symbolic)).to.deep.equal({ $type: 'accepted' });
      const section = encode({ $descriptor: typed('symbol', 'amqp:data:binary'), value: Buffer.from('x') });
      expect(decode(section)).to.deep.equal({ $type: 'data', value: Buffer.from('x') });
      const unknown = encode({ $descriptor: 0x1234, value: 'custom' });
      expect(decode(unknown)).to.deep.equal({ $descriptor: 0x1234, value: 'custom' });
      expect(decode(encode({ $descriptor: 'amqp:released:list', value: [] }))).to.deep.equal({ $type: 'released' });
    });

    it('encodes sections from bare values and tolerates malformed composites', () => {
      expect(decode(encode(Buffer.from('x'), 'data'))).to.deep.equal({ $type: 'data', value: Buffer.from('x') });
      expect(decode(encode({ $type: 'amqp-value', value: null }))).to.deep.equal({ $type: 'amqp-value', value: null });
      expect(decode(encode({ $type: 'amqp-value' }))).to.deep.equal({ $type: 'amqp-value', value: null });
      expect(decode(encode({ $type: 'data', value: null }))).to.deep.equal({ $type: 'data', value: null });
      expect(decode(encode({ $type: 'data' }))).to.deep.equal({ $type: 'data', value: null });
      expect(decode(Buffer.from([0x00, 0x53, 0x24, 0x40]))).to.deep.equal({ $type: 'accepted' });
    });
  });

  describe('composites', () => {
    it('drops trailing null fields and keeps arrays of symbols', () => {
      const open = roundTrip({ $type: 'open', containerId: 'c', offeredCapabilities: ['a', 'b'], desiredCapabilities: ['single'] });
      expect(open).to.deep.equal({ $type: 'open', containerId: 'c', offeredCapabilities: ['a', 'b'], desiredCapabilities: 'single' });
    });

    it('encodes an empty composite as an empty list', () => {
      expect(encode({ $type: 'accepted' })).to.deep.equal(Buffer.from([0x00, 0x53, 0x24, 0x45]));
      expect(roundTrip({ $type: 'accepted' })).to.deep.equal({ $type: 'accepted' });
    });
  });

  describe('errors', () => {
    it('rejects values it cannot encode', () => {
      expect(() => encode(() => {})).to.throw(TypeError, /Cannot encode function/);
      expect(() => encode(1, 'nope')).to.throw(TypeError, /Unknown AMQP 1.0 type nope/);
    });

    it('rejects unknown constructors and bad frame sizes', () => {
      expect(() => decode(Buffer.from([0x3f]))).to.throw(TypeError, /Unknown AMQP 1.0 type constructor 0x3f/);
      expect(() => new Amqp10FrameParser().feed(Buffer.from([0, 0, 0, 4, 2, 0, 0, 0]))).to.throw(
        TypeError,
        /Invalid AMQP 1.0 frame size 4/,
      );
    });
  });

  describe('addresses', () => {
    it('resolves queue and exchange addresses', () => {
      expect(resolveAmqp10Address('/queues/q')).to.deep.equal({ kind: 'queue', queue: 'q' });
      expect(resolveAmqp10Address('/queue/q')).to.deep.equal({ kind: 'queue', queue: 'q' });
      expect(resolveAmqp10Address('/amq/queue/q')).to.deep.equal({ kind: 'queue', queue: 'q' });
      expect(resolveAmqp10Address('/q')).to.deep.equal({ kind: 'queue', queue: 'q' });
      expect(resolveAmqp10Address('q')).to.deep.equal({ kind: 'queue', queue: 'q' });
      expect(resolveAmqp10Address('/exchanges/x/a.b')).to.deep.equal({ kind: 'exchange', exchange: 'x', routingKey: 'a.b' });
      expect(resolveAmqp10Address('/exchange/x')).to.deep.equal({ kind: 'exchange', exchange: 'x', routingKey: '' });
    });
  });

  describe('frames', () => {
    it('parses frames arriving one byte at a time and empty frames', () => {
      const frame = encodeAmqp10Frame(0, 3, { $type: 'flow', nextOutgoingId: 1, incomingWindow: 2, outgoingWindow: 3 });
      const parser = new Amqp10FrameParser();
      const frames = [];
      for (const byte of frame) frames.push(...parser.feed(Buffer.from([byte])));
      frames.push(...parser.feed(encodeAmqp10Frame(0, 0, null)));
      expect(frames).to.have.length(2);
      expect(frames[0].channel).to.equal(3);
      expect(frames[0].performative).to.deep.equal({ $type: 'flow', nextOutgoingId: 1, incomingWindow: 2, outgoingWindow: 3 });
      expect(frames[1].performative).to.equal(null);
    });
  });
});
