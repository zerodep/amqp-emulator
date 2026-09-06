import type { Consumer } from 'smqp';

export interface ServerOptions {
  /** suggested heartbeat interval in seconds, defaults to 60, 0 disables heartbeats */
  heartbeat?: number;
  /** max frame size in bytes, defaults to 131072 */
  frameMax?: number;
  /** max channels per connection, defaults to 2047 */
  channelMax?: number;
  /** milliseconds to wait for connection.close-ok before dropping the socket, defaults to 1000 */
  closeTimeout?: number;
  /** allowed vhosts, defaults to any (created on demand) */
  vhosts?: string[] | null;
  /** credential check, defaults to accept everyone */
  authenticate?: ((user: string, password: string, connection: AuthenticatingConnection) => boolean) | null;
  /** version string reported in server properties */
  version?: string;
  /** how often expired messages are swept from queues in ms, dead lettering them when configured, defaults to 100, 0 disables */
  evictInterval?: number;
  /** Service Bus peek lock duration in ms, defaults to 60000 */
  lockDuration?: number;
}

/** the connection being authenticated, either protocol */
export interface AuthenticatingConnection {
  id: number;
  clientProperties: Record<string, any>;
  socket: import('node:net').Socket;
}

export interface ListenResult {
  port: number;
  host: string;
  url: string;
}

export type AmqpMethodName =
  | 'connection.start'
  | 'connection.start-ok'
  | 'connection.secure'
  | 'connection.secure-ok'
  | 'connection.tune'
  | 'connection.tune-ok'
  | 'connection.open'
  | 'connection.open-ok'
  | 'connection.close'
  | 'connection.close-ok'
  | 'connection.blocked'
  | 'connection.unblocked'
  | 'connection.update-secret'
  | 'connection.update-secret-ok'
  | 'channel.open'
  | 'channel.open-ok'
  | 'channel.flow'
  | 'channel.flow-ok'
  | 'channel.close'
  | 'channel.close-ok'
  | 'access.request'
  | 'access.request-ok'
  | 'exchange.declare'
  | 'exchange.declare-ok'
  | 'exchange.delete'
  | 'exchange.delete-ok'
  | 'exchange.bind'
  | 'exchange.bind-ok'
  | 'exchange.unbind'
  | 'exchange.unbind-ok'
  | 'queue.declare'
  | 'queue.declare-ok'
  | 'queue.bind'
  | 'queue.bind-ok'
  | 'queue.purge'
  | 'queue.purge-ok'
  | 'queue.delete'
  | 'queue.delete-ok'
  | 'queue.unbind'
  | 'queue.unbind-ok'
  | 'basic.qos'
  | 'basic.qos-ok'
  | 'basic.consume'
  | 'basic.consume-ok'
  | 'basic.cancel'
  | 'basic.cancel-ok'
  | 'basic.publish'
  | 'basic.return'
  | 'basic.deliver'
  | 'basic.get'
  | 'basic.get-ok'
  | 'basic.get-empty'
  | 'basic.ack'
  | 'basic.reject'
  | 'basic.recover-async'
  | 'basic.recover'
  | 'basic.recover-ok'
  | 'basic.nack'
  | 'tx.select'
  | 'tx.select-ok'
  | 'tx.commit'
  | 'tx.commit-ok'
  | 'tx.rollback'
  | 'tx.rollback-ok'
  | 'confirm.select'
  | 'confirm.select-ok';

/** AMQP wire types used in method arguments and content properties */
export type WireType = 'bit' | 'octet' | 'short' | 'long' | 'longlong' | 'shortstr' | 'longstr' | 'table' | 'timestamp';

export interface MethodDef {
  name: AmqpMethodName;
  /** class id shifted 16 bits or:ed with method id */
  id: number;
  classId: number;
  methodId: number;
  synchronous: boolean;
  /** method carries content, i.e. is followed by a header frame and body frames */
  content: boolean;
  /** field name, wire type, default value */
  args: Array<[string, WireType, unknown]>;
}

export interface PropertiesDef {
  name: string;
  classId: number;
  /** field name, wire type */
  fields: Array<[string, WireType]>;
}

export type MethodFields = Record<string, any>;

/** AMQP basic properties as amqplib names them */
export interface MessageProperties {
  contentType?: string;
  contentEncoding?: string;
  headers?: Record<string, any>;
  deliveryMode?: number;
  priority?: number;
  correlationId?: string;
  replyTo?: string;
  expiration?: string;
  messageId?: string;
  timestamp?: number;
  type?: string;
  userId?: string;
  appId?: string;
  clusterId?: string;
  [x: string]: any;
}

export interface Frame {
  type: number;
  channel: number;
  payload: Buffer;
}

export interface DecodedMethod {
  method: MethodDef;
  fields: MethodFields;
}

export interface DecodedProperties {
  classId: number;
  bodySize: number;
  properties: MessageProperties;
}

/** whoever owns a consumer and wants to know when its queue disappears */
export interface ConsumerOwner {
  onConsumerCancelled(entry: ConsumerEntry): void;
}

export interface ConsumerEntry {
  /** client facing consumer tag */
  tag: string;
  noAck: boolean;
  channel: ConsumerOwner;
  consumer: Consumer;
}

export interface CloseReason {
  replyCode: number;
  replyText: string;
  classId?: number;
  methodId?: number;
}

/** basic.publish in flight, kept until the header and body frames have arrived */
export interface PendingContent {
  method: MethodDef;
  fields: MethodFields;
  properties?: MessageProperties;
  bodySize?: number;
  received: number;
  chunks: Buffer[];
}

/** channel currently publishing, used to correlate basic.return */
export interface Publishing {
  channel: { onReturn(event: any, publishing: Publishing): void };
  properties: MessageProperties;
}

export type ChannelState = 'open' | 'closing' | 'closed';

export type ConnectionState = 'protocol-header' | 'start' | 'tune' | 'open' | 'running' | 'closing' | 'closed';

export interface Amqp10Field {
  /** camel cased field name */
  name: string;
  /** spec type name: a primitive, a restricted type, a composite or `*` */
  type: string;
  mandatory: boolean;
  /** may be encoded as an array of the type */
  multiple: boolean;
  /** archetype the value must provide when type is `*` */
  requires?: string;
}

/** AMQP 1.0 described list type, e.g. a performative, a terminus or a message header */
export interface Amqp10Composite {
  name: string;
  /** numeric descriptor, domain shifted 32 bits plus id */
  descriptor: number;
  symbol: string;
  source: string;
  provides: string[];
  fields: Amqp10Field[];
}

/** AMQP 1.0 restricted type, a primitive with a name, choices and sometimes a descriptor (message sections) */
export interface Amqp10Restricted {
  name: string;
  source: string;
  choices: Array<[string, string]>;
  descriptor?: number;
  symbol?: string;
}

export interface Amqp10Encoding {
  name: string;
  code: number;
  category: string;
  width: number;
}

export interface Amqp10Primitive {
  name: string;
  encodings: Amqp10Encoding[];
}

/** decoded described value: known composites and sections come back with `$type`, unknown descriptors with `$descriptor` */
export type Amqp10Described = { $type: string; [field: string]: any } | { $descriptor: number | string; value: any };

export interface Amqp10Frame {
  /** 0 amqp, 1 sasl */
  type: number;
  channel: number;
  /** null for empty (heartbeat) frames */
  performative: Amqp10Described | null;
  payload: Buffer;
}

export type Amqp10ConnectionState = 'header' | 'sasl' | 'sasl-done' | 'open' | 'running' | 'closing' | 'closed';

/** decoded AMQP 1.0 message, sections by name plus the body sections in order */
export interface Amqp10Message {
  header?: Record<string, any>;
  deliveryAnnotations?: Record<string, any>;
  messageAnnotations?: Record<string, any>;
  properties?: Record<string, any>;
  applicationProperties?: Record<string, any>;
  body: Array<{ $type: 'data' | 'amqp-sequence' | 'amqp-value'; value: any }>;
  footer?: Record<string, any>;
}
