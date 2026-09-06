import { BufferReader, BufferWriter } from '../amqp09/codec.js';
import { kWireProperties, wireProperties } from '../shared.js';
import { decodeValue, encodeValue } from './types.js';

/** Symbol under which the original AMQP 1.0 sections are kept on smqp message properties */
export const kSections = Symbol.for('amqp-emulator amqp10 sections');

/**
 * Decode the message sections carried by transfer frames
 * @param {Buffer} payload
 * @returns {import('types').Amqp10Message}
 */
export function decodeMessage(payload) {
  /** @type {import('types').Amqp10Message} */
  const message = { body: [] };
  const r = new BufferReader(payload);
  while (r.offset < payload.length) {
    const section = /** @type {any} */ (decodeValue(r));
    switch (section?.$type) {
      case 'header':
      case 'properties': {
        const { $type, ...fields } = section;
        message[$type === 'header' ? 'header' : 'properties'] = fields;
        break;
      }
      case 'delivery-annotations':
        message.deliveryAnnotations = section.value;
        break;
      case 'message-annotations':
        message.messageAnnotations = section.value;
        break;
      case 'application-properties':
        message.applicationProperties = section.value;
        break;
      case 'footer':
        message.footer = section.value;
        break;
      case 'data':
      case 'amqp-sequence':
      case 'amqp-value':
        message.body.push(section);
        break;
      default:
        throw new TypeError(`Unexpected message section ${JSON.stringify(section)}`);
    }
  }
  return message;
}

/**
 * Message body as a buffer, for 0-9-1 consumers and inspection
 * @param {import('types').Amqp10Message} message
 */
function bodyToBuffer(message) {
  const { body } = message;
  if (!body.length) return Buffer.alloc(0);
  if (body.every((s) => s.$type === 'data')) return Buffer.concat(body.map((s) => s.value));
  if (body.length === 1 && body[0].$type === 'amqp-value') {
    const value = body[0].value;
    if (Buffer.isBuffer(value)) return value;
    if (typeof value === 'string') return Buffer.from(value, 'utf8');
    return Buffer.from(JSON.stringify(value));
  }
  return Buffer.from(JSON.stringify(body.map((s) => s.value)));
}

/**
 * Map a decoded 1.0 message onto smqp content and properties, keeping the original sections for 1.0 consumers
 * @param {import('types').Amqp10Message} message
 * @returns {{ content: Buffer, properties: import('smqp').MessageProperties }}
 */
export function toSmqp(message) {
  const header = message.header ?? {};
  const props = message.properties ?? {};
  /** @type {import('types').MessageProperties} */
  const wire = {};
  if (props.contentType !== undefined) wire.contentType = String(props.contentType);
  if (props.contentEncoding !== undefined) wire.contentEncoding = String(props.contentEncoding);
  if (props.messageId !== undefined) wire.messageId = String(props.messageId);
  if (props.correlationId !== undefined) wire.correlationId = String(props.correlationId);
  if (props.replyTo !== undefined) wire.replyTo = props.replyTo;
  if (props.subject !== undefined) wire.type = props.subject;
  if (props.userId !== undefined) wire.userId = Buffer.isBuffer(props.userId) ? props.userId.toString('utf8') : String(props.userId);
  if (props.creationTime !== undefined) wire.timestamp = Math.floor(props.creationTime / 1000);
  if (header.durable) wire.deliveryMode = 2;
  if (header.priority !== undefined) wire.priority = header.priority;
  if (header.ttl !== undefined) wire.expiration = String(header.ttl);
  else if (props.absoluteExpiryTime !== undefined) wire.expiration = String(Math.max(0, props.absoluteExpiryTime - Date.now()));
  if (message.applicationProperties) wire.headers = { ...message.applicationProperties };

  /** @type {any} */
  const properties = { ...wire, [kWireProperties]: wire, [kSections]: message, timestamp: Date.now() };
  if (wire.messageId === undefined) delete properties.messageId;
  return { content: bodyToBuffer(message), properties };
}

/**
 * Encode an smqp message as 1.0 sections. Messages that arrived over 1.0 keep their sections,
 * messages from 0-9-1 clients or the broker api are mapped from their properties.
 * @param {import('smqp').Message} message
 * @param {{ annotations?: Record<string, any>, deliveryCount?: number }} [extra] message annotations to merge in and a delivery count for the header
 */
export function encodeMessage(message, extra = {}) {
  const w = new BufferWriter();
  const deliveryCount = extra.deliveryCount ?? (message.fields.redelivered ? 1 : undefined);
  /** @type {import('types').Amqp10Message | undefined} */
  const sections = /** @type {any} */ (message.properties)[kSections];
  if (sections) {
    if (sections.header || deliveryCount !== undefined) encodeValue(w, { $type: 'header', ...sections.header, deliveryCount }, 'header');
    if (sections.messageAnnotations || extra.annotations) {
      encodeValue(w, { $type: 'message-annotations', value: { ...sections.messageAnnotations, ...extra.annotations } });
    }
    if (sections.properties) encodeValue(w, { $type: 'properties', ...sections.properties }, 'properties');
    if (sections.applicationProperties) encodeValue(w, { $type: 'application-properties', value: sections.applicationProperties });
    for (const section of sections.body) encodeValue(w, section);
    if (sections.footer) encodeValue(w, { $type: 'footer', value: sections.footer });
    return w.toBuffer();
  }

  const wire = wireProperties(message.properties);
  const header = {
    $type: 'header',
    durable: wire.deliveryMode === 2 ? true : undefined,
    priority: wire.priority,
    ttl: wire.expiration !== undefined ? Number(wire.expiration) : undefined,
    deliveryCount,
  };
  if (Object.values(header).some((v, i) => i > 0 && v !== undefined)) encodeValue(w, header, 'header');
  if (extra.annotations) encodeValue(w, { $type: 'message-annotations', value: extra.annotations });
  const properties = {
    $type: 'properties',
    messageId: wire.messageId,
    userId: wire.userId !== undefined ? Buffer.from(String(wire.userId)) : undefined,
    subject: wire.type,
    replyTo: wire.replyTo,
    correlationId: wire.correlationId,
    contentType: wire.contentType,
    contentEncoding: wire.contentEncoding,
    creationTime: wire.timestamp !== undefined ? wire.timestamp * 1000 : undefined,
  };
  if (Object.values(properties).some((v, i) => i > 0 && v !== undefined)) encodeValue(w, properties, 'properties');
  if (wire.headers && Object.keys(wire.headers).length) encodeValue(w, { $type: 'application-properties', value: wire.headers });
  const content = message.content;
  const data = Buffer.isBuffer(content) ? content : Buffer.from(typeof content === 'string' ? content : JSON.stringify(content ?? ''));
  encodeValue(w, { $type: 'data', value: data });
  return w.toBuffer();
}
