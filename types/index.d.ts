declare module '@0dep/amqp-emulator' {
	import type { EventEmitter } from 'node:events';
	import type { default as net } from 'node:net';
	import type { Consumer } from 'smqp';
	export type ServerOptions = ServerOptions_1;
	export type ListenResult = ListenResult_1;
	export type AmqpMethodName = AmqpMethodName_1;
	export type WireType = WireType_1;
	export type MethodDef = MethodDef_1;
	export type PropertiesDef = PropertiesDef_1;
	export type MethodFields = MethodFields_1;
	export type MessageProperties = MessageProperties_1;
	export type Frame = Frame_1;
	export type DecodedMethod = DecodedMethod_1;
	export type DecodedProperties = DecodedProperties_1;
	export type ConsumerEntry = ConsumerEntry_1;
	export type CloseReason = CloseReason_1;
	export type PendingContent = PendingContent_1;
	export type Publishing = Publishing_1;
	export type ChannelState = ChannelState_1;
	export type ConnectionState = ConnectionState_1;
	export type AmqpConnectionEvents = AmqpConnectionEvents_1;
	export type AmqpServerEvents = AmqpServerEvents_1;
	/**
	 * AMQP 0-9-1 broker emulator listening on a TCP port
	 * 
	 */
	export class AmqpServer extends EventEmitter<AmqpServerEvents_1> {
		
		constructor(options?: ServerOptions_1);
		
		options: Required<ServerOptions_1>;
		
		vhosts: Map<string, VHost>;
		
		connections: Set<AmqpConnection>;
		/** @type {string | null} reason passed to block(), null when not blocked */
		blockedReason: string | null;
		
		server: net.Server;
		/**
		 * Start listening
		 * @param port defaults to 0, i.e. a random free port
		 * @param host defaults to 127.0.0.1
		 * */
		listen(port?: number, host?: string): Promise<ListenResult_1>;
		
		get port(): number | undefined;
		/** Host name usable in a client url */
		get host(): string;
		/** amqp:// url for clients */
		get url(): string;
		/**
		 * Get or create a vhost
		 * @param name defaults to '/'
		 */
		getVHost(name?: string): VHost;
		/**
		 * Get the smqp broker behind a vhost, for inspection and seeding
		 * @param vhost defaults to '/'
		 * */
		getBroker(vhost?: string): import("smqp").Broker;
		/**
		 * Message, unacked and consumer counts for a vhost, totals and per queue
		 * @param vhost defaults to '/'
		 * */
		getStats(vhost?: string): import("smqp").BrokerStats;
		/**
		 * Close all client connections with connection.close, like a broker restart
		 * @param code defaults to 320 CONNECTION_FORCED
		 */
		disconnect(reason?: string, code?: number): void;
		/**
		 * Drop all connections and clear every vhost
		 */
		reset(): void;
		/**
		 * Send connection.blocked to all connections that support it
		 * 
		 */
		block(reason?: string): void;
		/** Send connection.unblocked to all blocked connections */
		unblock(): void;
		/**
		 * Stop listening and destroy all connections
		 * */
		close(): Promise<void>;
	}
	/**
	 * One client connection: handshake, framing, heartbeats and channel multiplexing
	 * 
	 */
	export class AmqpConnection extends EventEmitter<AmqpConnectionEvents_1> {
		
		constructor(server: AmqpServer, socket: import("node:net").Socket);
		id: number;
		server: AmqpServer;
		socket: import("net").Socket;
		
		channels: Map<number, AmqpChannel>;
		
		state: ConnectionState_1;
		/** negotiated max frame size */
		frameMax: number;
		/** negotiated max channel number */
		channelMax: number;
		/** negotiated heartbeat interval in seconds */
		heartbeat: number;
		
		vhost: VHost | null;
		/** @type {string | null} authenticated user */
		user: string | null;
		/** @type {Record<string, any>} client properties from connection.start-ok */
		clientProperties: Record<string, any>;
		
		closeReason: CloseReason_1 | null;
		/** true if the client announced the connection.blocked capability and the connection is open */
		get supportsBlocked(): boolean;
		/**
		 * Write raw bytes to the socket
		 * */
		write(buffer: Buffer): void;
		
		sendMethod(channel: number, methodName: AmqpMethodName_1, fields?: MethodFields_1): void;
		/**
		 * Send a content carrying method: method frame, header frame and body frames split by frameMax
		 * */
		sendContent(channel: number, methodName: AmqpMethodName_1, fields: MethodFields_1, properties: MessageProperties_1, content: Buffer): void;
		/** Send connection.blocked with the server block reason if the client supports it */
		notifyBlocked(): void;
		/** Send connection.unblocked if the client supports it */
		notifyUnblocked(): void;
		/**
		 * Server initiated connection close
		 * */
		closeWithError(err: AmqpError): void;
		/** Drop the socket and release everything */
		destroy(): void;
		#private;
	}
	/**
	 * One AMQP channel on a connection, translating methods onto the vhost broker
	 */
	export class AmqpChannel {
		/** @type {Record<string, MethodHandler>} AMQP method handlers, invoked with the channel as this */
		static #handlers: Record<string, MethodHandler>;
		
		constructor(connection: AmqpConnection, id: number);
		connection: AmqpConnection;
		id: number;
		vhost: VHost;
		broker: import("smqp").Broker;
		
		state: ChannelState_1;
		/** @type {Map<string, import('../types/interfaces.d.ts').ConsumerEntry>} consumers by client facing consumer tag */
		consumers: Map<string, ConsumerEntry_1>;
		/** @type {Map<number, import('smqp').Message>} unacknowledged messages by delivery tag */
		unacked: Map<number, import("smqp").Message>;
		deliveryTag: number;
		/** per consumer prefetch, 0 means unlimited */
		prefetchCount: number;
		/** channel wide prefetch, 0 means unlimited */
		globalPrefetch: number;
		confirmMode: boolean;
		publishSeq: number;
		/** name of the most recently declared queue, used when the client passes an empty queue name */
		lastQueue: string;
		
		send(methodName: AmqpMethodName_1, fields?: MethodFields_1): void;
		
		sendContent(methodName: AmqpMethodName_1, fields: MethodFields_1, properties: MessageProperties_1, content: Buffer): void;
		
		handleMethod(method: MethodDef_1, fields: MethodFields_1): void;
		
		handleHeader(payload: Buffer): void;
		
		handleBody(payload: Buffer): void;
		/**
		 * Server initiated close
		 * */
		close(err: AmqpError): void;
		/** Release consumers and requeue unacked messages */
		teardown(): void;
		/**
		 * Called by the vhost when a queue with one of this channel's consumers was deleted elsewhere
		 * */
		onConsumerCancelled(entry: ConsumerEntry_1): void;
		/**
		 * Called by the vhost when a mandatory publish could not be routed
		 * */
		onReturn(event: import("smqp").Message, publishing: Publishing_1): void;
		#private;
	}
	type MethodHandler = (this: AmqpChannel, fields: MethodFields_1, method: MethodDef_1, properties?: MessageProperties_1, content?: Buffer) => void;
	/**
	 * A virtual host wraps one smqp broker plus the bookkeeping AMQP needs on top of it
	 */
	export class VHost {
		
		constructor(name: string, server: AmqpServer);
		name: string;
		server: AmqpServer;
		
		broker: import("smqp").Broker;
		/** @type {Map<string, string>} declared AMQP exchange type by name, smqp only knows topic and direct */
		exchangeTypes: Map<string, string>;
		/** @type {Map<string, number>} exclusive queue name -> owning connection id */
		exclusiveQueues: Map<string, number>;
		/** @type {import('../types/interfaces.d.ts').Publishing | null} channel currently publishing, for basic.return correlation */
		publishing: Publishing_1 | null;
		/**
		 * @returns AMQP exchange type if the exchange exists
		 */
		getExchangeType(name: string): string | undefined;
		/**
		 * @param type AMQP exchange type: direct, topic or fanout
		 * 
		 */
		declareExchange(name: string, type: string, options?: import("smqp").ExchangeOptions): void;
		/**
		 * Binding pattern for an exchange type; fanout ignores the routing key
		 * */
		bindingPattern(exchangeName: string, routingKey: string): string;
		/**
		 * Delete a queue and notify channels consuming from it
		 * */
		deleteQueue(queueName: string): {
			messageCount: number;
		};
		/**
		 * Delete exclusive queues owned by a connection
		 * */
		releaseConnection(connectionId: number): void;
		/** Stop the expiry sweeper */
		close(): void;
		/** Clear everything and redeclare the default exchanges */
		reset(): void;
		#private;
	}
	/**
	 * Protocol-level error that maps to a channel.close or connection.close
	 */
	export class AmqpError extends Error {
		/**
		 * @param code reply code, e.g. 404
		 * @param text reply text
		 * */
		constructor(code: number, text: string, options?: {
			connection?: boolean;
			method?: MethodDef_1;
		});
		/** AMQP reply code */
		code: number;
		/** AMQP reply text */
		replyText: string;
		/** true if the error closes the connection rather than the channel */
		killConnection: boolean;
		/** offending method, if known */
		method: MethodDef_1;
	}
	/**
	 * Incremental AMQP frame parser. Feed it socket chunks, get complete frames back.
	 */
	export class FrameParser {
		
		pending: Buffer;
		
		feed(chunk: Buffer): Frame_1[];
	}
  interface ServerOptions_1 {
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
	authenticate?: ((user: string, password: string, connection: import('@0dep/amqp-emulator').AmqpConnection) => boolean) | null;
	/** version string reported in server properties */
	version?: string;
	/** how often expired messages are swept from queues in ms, dead lettering them when configured, defaults to 100, 0 disables */
	evictInterval?: number;
  }

  interface ListenResult_1 {
	port: number;
	host: string;
	url: string;
  }

  type AmqpMethodName_1 =
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
  type WireType_1 = 'bit' | 'octet' | 'short' | 'long' | 'longlong' | 'shortstr' | 'longstr' | 'table' | 'timestamp';

  interface MethodDef_1 {
	name: AmqpMethodName_1;
	/** class id shifted 16 bits or:ed with method id */
	id: number;
	classId: number;
	methodId: number;
	synchronous: boolean;
	/** method carries content, i.e. is followed by a header frame and body frames */
	content: boolean;
	/** field name, wire type, default value */
	args: Array<[string, WireType_1, unknown]>;
  }

  interface PropertiesDef_1 {
	name: string;
	classId: number;
	/** field name, wire type */
	fields: Array<[string, WireType_1]>;
  }

  type MethodFields_1 = Record<string, any>;

  /** AMQP basic properties as amqplib names them */
  interface MessageProperties_1 {
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

  interface Frame_1 {
	type: number;
	channel: number;
	payload: Buffer;
  }

  interface DecodedMethod_1 {
	method: MethodDef_1;
	fields: MethodFields_1;
  }

  interface DecodedProperties_1 {
	classId: number;
	bodySize: number;
	properties: MessageProperties_1;
  }

  interface ConsumerEntry_1 {
	/** client facing consumer tag */
	tag: string;
	noAck: boolean;
	channel: import('@0dep/amqp-emulator').AmqpChannel;
	consumer: Consumer;
  }

  interface CloseReason_1 {
	replyCode: number;
	replyText: string;
	classId?: number;
	methodId?: number;
  }

  /** basic.publish in flight, kept until the header and body frames have arrived */
  interface PendingContent_1 {
	method: MethodDef_1;
	fields: MethodFields_1;
	properties?: MessageProperties_1;
	bodySize?: number;
	received: number;
	chunks: Buffer[];
  }

  /** channel currently publishing, used to correlate basic.return */
  interface Publishing_1 {
	channel: import('@0dep/amqp-emulator').AmqpChannel;
	properties: MessageProperties_1;
  }

  type ChannelState_1 = 'open' | 'closing' | 'closed';

  type ConnectionState_1 = 'protocol-header' | 'start' | 'tune' | 'open' | 'running' | 'closing' | 'closed';

  interface AmqpConnectionEvents_1 {
	channel: [channel: import('@0dep/amqp-emulator').AmqpChannel];
	'channel.error': [err: import('@0dep/amqp-emulator').AmqpError, channel: import('@0dep/amqp-emulator').AmqpChannel];
	'error.amqp': [err: import('@0dep/amqp-emulator').AmqpError];
	'socket.error': [err: Error];
	'heartbeat.timeout': [];
	close: [];
  }

  interface AmqpServerEvents_1 {
	connection: [connection: import('@0dep/amqp-emulator').AmqpConnection];
	error: [err: Error, connection?: import('@0dep/amqp-emulator').AmqpConnection];
  }
	export namespace constants {
		let FRAME_METHOD: number;
		let FRAME_HEADER: number;
		let FRAME_BODY: number;
		let FRAME_HEARTBEAT: number;
		let FRAME_MIN_SIZE: number;
		let FRAME_END: number;
		let REPLY_SUCCESS: number;
		let CONTENT_TOO_LARGE: number;
		let NO_ROUTE: number;
		let NO_CONSUMERS: number;
		let ACCESS_REFUSED: number;
		let NOT_FOUND: number;
		let RESOURCE_LOCKED: number;
		let PRECONDITION_FAILED: number;
		let CONNECTION_FORCED: number;
		let INVALID_PATH: number;
		let FRAME_ERROR: number;
		let SYNTAX_ERROR: number;
		let COMMAND_INVALID: number;
		let CHANNEL_ERROR: number;
		let UNEXPECTED_FRAME: number;
		let RESOURCE_ERROR: number;
		let NOT_ALLOWED: number;
		let NOT_IMPLEMENTED: number;
		let INTERNAL_ERROR: number;
	}

	export const methods: Record<AmqpMethodName_1, MethodDef_1>;
	/**
	 * Encode a complete method frame
	 * @param methodName e.g. 'basic.deliver'
	 * 
	 */
	export function encodeMethod(methodName: AmqpMethodName_1, channel: number, fields?: MethodFields_1): Buffer<ArrayBufferLike>;
	/**
	 * Decode a method frame payload
	 * */
	export function decodeMethod(payload: Buffer): DecodedMethod_1;
	/**
	 * Encode a content header frame
	 * @param props basic properties, unknown keys are ignored
	 * @param classId defaults to basic (60)
	 */
	export function encodeProperties(channel: number, bodySize: number, props?: MessageProperties_1, classId?: number): Buffer<ArrayBufferLike>;
	/**
	 * Decode a content header frame payload
	 * */
	export function decodeProperties(payload: Buffer): DecodedProperties_1;
	/** Growable big-endian buffer writer */
	export class BufferWriter {
		/**
		 * @param size initial size in bytes
		 */
		constructor(size?: number);
		
		buf: Buffer;
		offset: number;
		/**
		 * Make room for n more bytes
		 * */
		ensure(n: number): void;
		
		octet(v: number): void;
		
		short(v: number): void;
		
		long(v: number): void;
		
		longlong(v: number | bigint): void;
		
		shortstr(v?: string): void;
		
		longstr(v?: string | Buffer): void;
		
		raw(b: Buffer): void;
		
		table(v?: Record<string, any>): void;
		
		array(v: any[]): void;
		/** @param t single char type tag */
		tag(t: string): void;
		/**
		 * Write a tagged field table value. Numbers are encoded as the smallest signed integer that fits,
		 * or as a double when fractional. Use `{ '!': type, value }` to force a type, like amqplib.
		 * */
		fieldValue(value: any): void;
		/** The written bytes */
		toBuffer(): Buffer<ArrayBufferLike>;
	}
	/** Sequential big-endian reader */
	export class BufferReader {
		
		constructor(buf: Buffer, offset?: number);
		buf: Buffer<ArrayBufferLike>;
		offset: number;
		octet(): number;
		short(): number;
		long(): number;
		longlong(): number;
		shortstr(): string;
		longstr(): Buffer<ArrayBufferLike>;
		
		table(): Record<string, any>;
		
		array(): any[];
		/**
		 * Read a tagged field table value. Timestamps and decimals come back as `{ '!': type, value }`, like amqplib.
		 * */
		fieldValue(): any;
	}

	export {};
}

//# sourceMappingURL=index.d.ts.map