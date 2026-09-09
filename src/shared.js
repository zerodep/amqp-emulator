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
