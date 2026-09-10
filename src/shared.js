import { randomBytes } from 'node:crypto';

/** 22 char url-safe random id, like RabbitMQ server-generated names */
export function generateId() {
  return randomBytes(16).toString('base64url').slice(0, 22);
}

/** Symbol under which the original wire properties are kept on smqp message properties */
export const kWireProperties = Symbol.for('amqp-emulator wire properties');

/**
 * Pick the properties that go on the wire from smqp message properties
 * @param {import('smqp').MessageProperties} properties
 * @returns {import('types').MessageProperties}
 */
export function wireProperties(properties) {
  const wire = /** @type {any} */ (properties)[kWireProperties];
  if (wire) return wire;
  const { mandatory, confirm, ttl, persistent, messageId, timestamp, ...rest } = properties;
  if (messageId && !String(messageId).startsWith('smq.mid-')) rest.messageId = messageId;
  return /** @type {any} */ (rest);
}

/**
 * Message content as a buffer; messages seeded through the smqp broker api may carry strings or objects
 * @param {any} content
 */
export function toBuffer(content) {
  if (Buffer.isBuffer(content)) return content;
  if (content === undefined || content === null) return Buffer.alloc(0);
  return Buffer.from(typeof content === 'string' ? content : JSON.stringify(content));
}
