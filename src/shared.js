import { randomBytes } from 'node:crypto';

/** 22 char url-safe random id, like RabbitMQ server-generated names */
export function generateId() {
  return randomBytes(16).toString('base64url').slice(0, 22);
}

/** Symbol under which the original wire properties are kept on smqp message properties */
export const kWireProperties = Symbol.for('amqp-emulator wire properties');

/** Symbol under which the owning channel consumer entry is kept on an smqp consumer */
export const kConsumerEntry = Symbol.for('amqp-emulator consumer entry');

const SMQP_INTERNAL_QUEUE = Symbol.for('internalQueue');

/**
 * Detach messages held by an smqp consumer so a subsequent cancel does not requeue them.
 * AMQP basic.cancel leaves already delivered messages unacked on the channel; smqp cancel would requeue them.
 * @param {import('smqp').Consumer} consumer
 */
export function detachConsumerMessages(consumer) {
  /** @type {import('smqp').Queue | undefined} */
  const internalQueue = /** @type {any} */ (consumer)[SMQP_INTERNAL_QUEUE];
  if (internalQueue) internalQueue.messages.splice(0);
}

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
