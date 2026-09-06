import { constants, constantNames } from './defs.js';

/**
 * Protocol-level error that maps to a channel.close or connection.close
 */
export class AmqpError extends Error {
  /**
   * @param {number} code reply code, e.g. 404
   * @param {string} text reply text
   * @param {object} [options]
   * @param {boolean} [options.connection] close the connection rather than the channel
   * @param {import('types').MethodDef} [options.method] offending method
   */
  constructor(code, text, options = {}) {
    super(`${code} (${constantNames[code] || 'UNKNOWN'}) with message "${text}"`);
    this.name = 'AmqpError';
    /** AMQP reply code */
    this.code = code;
    /** AMQP reply text */
    this.replyText = text;
    /** true if the error closes the connection rather than the channel */
    this.killConnection = !!options.connection;
    /** offending method, if known */
    this.method = options.method;
  }
}

/**
 * @param {string} kind exchange or queue
 * @param {string} name
 * @param {string} vhost
 * @param {import('types').MethodDef} [method]
 */
export function notFound(kind, name, vhost, method) {
  return new AmqpError(constants.NOT_FOUND, `NOT_FOUND - no ${kind} '${name}' in vhost '${vhost}'`, { method });
}

/**
 * @param {string} text
 * @param {import('types').MethodDef} [method]
 */
export function preconditionFailed(text, method) {
  return new AmqpError(constants.PRECONDITION_FAILED, `PRECONDITION_FAILED - ${text}`, { method });
}

/**
 * @param {string} text
 * @param {import('types').MethodDef} [method]
 */
export function accessRefused(text, method) {
  return new AmqpError(constants.ACCESS_REFUSED, `ACCESS_REFUSED - ${text}`, { method });
}

/**
 * @param {string} text
 * @param {import('types').MethodDef} [method]
 */
export function notImplemented(text, method) {
  return new AmqpError(constants.NOT_IMPLEMENTED, `NOT_IMPLEMENTED - ${text}`, { method });
}

/**
 * Connection level error
 * @param {string} text
 * @param {import('types').MethodDef} [method]
 */
export function commandInvalid(text, method) {
  return new AmqpError(constants.COMMAND_INVALID, `COMMAND_INVALID - ${text}`, { method, connection: true });
}

/**
 * Connection level error
 * @param {string} text
 * @param {import('types').MethodDef} [method]
 */
export function channelError(text, method) {
  return new AmqpError(constants.CHANNEL_ERROR, `CHANNEL_ERROR - ${text}`, { method, connection: true });
}

/**
 * Connection level error
 * @param {string} text
 * @param {import('types').MethodDef} [method]
 */
export function unexpectedFrame(text, method) {
  return new AmqpError(constants.UNEXPECTED_FRAME, `UNEXPECTED_FRAME - ${text}`, { method, connection: true });
}
