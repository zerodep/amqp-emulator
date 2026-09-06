export { AmqpServer } from './server.js';
export { AmqpConnection } from './amqp09/connection.js';
export { Amqp10Connection } from './amqp10/connection.js';
export {
  Amqp10FrameParser,
  encode as encodeAmqp10,
  decode as decodeAmqp10,
  encodeFrame as encodeAmqp10Frame,
  typed as amqp10Typed,
} from './amqp10/types.js';
export { decodeMessage as decodeAmqp10Message, encodeMessage as encodeAmqp10Message } from './amqp10/message.js';
export { resolveAddress as resolveAmqp10Address } from './amqp10/link.js';
export { ServiceBus, ServiceBusError } from './servicebus/servicebus.js';
export { AmqpChannel } from './amqp09/channel.js';
export { VHost } from './vhost.js';
export { AmqpError } from './amqp09/errors.js';
export { constants, methods } from './amqp09/defs.js';
export { encodeMethod, decodeMethod, encodeProperties, decodeProperties, BufferReader, BufferWriter } from './amqp09/codec.js';
export { FrameParser } from './amqp09/frame.js';

/**
 * @typedef {import('types').ServerOptions} ServerOptions
 * @typedef {import('types').ListenResult} ListenResult
 * @typedef {import('types').AmqpMethodName} AmqpMethodName
 * @typedef {import('types').WireType} WireType
 * @typedef {import('types').MethodDef} MethodDef
 * @typedef {import('types').PropertiesDef} PropertiesDef
 * @typedef {import('types').MethodFields} MethodFields
 * @typedef {import('types').MessageProperties} MessageProperties
 * @typedef {import('types').Frame} Frame
 * @typedef {import('types').DecodedMethod} DecodedMethod
 * @typedef {import('types').DecodedProperties} DecodedProperties
 * @typedef {import('types').ConsumerEntry} ConsumerEntry
 * @typedef {import('types').CloseReason} CloseReason
 * @typedef {import('types').PendingContent} PendingContent
 * @typedef {import('types').Publishing} Publishing
 * @typedef {import('types').ChannelState} ChannelState
 * @typedef {import('types').ConnectionState} ConnectionState
 * @typedef {import('./amqp09/connection.js').AmqpConnectionEvents} AmqpConnectionEvents
 * @typedef {import('./amqp10/connection.js').Amqp10ConnectionEvents} Amqp10ConnectionEvents
 * @typedef {import('./server.js').AmqpServerEvents} AmqpServerEvents
 * @typedef {import('./server.js').AnyConnection} AnyConnection
 * @typedef {import('types').AuthenticatingConnection} AuthenticatingConnection
 * @typedef {import('types').Amqp10Composite} Amqp10Composite
 * @typedef {import('types').Amqp10Restricted} Amqp10Restricted
 * @typedef {import('types').Amqp10Described} Amqp10Described
 * @typedef {import('types').Amqp10Frame} Amqp10Frame
 * @typedef {import('types').Amqp10Message} Amqp10Message
 */
