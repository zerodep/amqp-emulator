declare module 'amqp-emulator' {
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
	export type Amqp10ConnectionEvents = Amqp10ConnectionEvents_1;
	export type AmqpServerEvents = AmqpServerEvents_1;
	export type AnyConnection = AnyConnection_1;
	export type AuthenticatingConnection = AuthenticatingConnection_1;
	export type Amqp10Composite = Amqp10Composite_1;
	export type Amqp10Restricted = Amqp10Restricted_1;
	export type Amqp10Described = Amqp10Described_1;
	export type Amqp10Frame = Amqp10Frame_1;
	export type Amqp10Message = Amqp10Message_1;
	/**
	 * Events emitted by the server: `connection` when a client completed its handshake, `error` for unexpected internal errors.
	 * 
	 */
	/**
	 * AMQP 0-9-1 and 1.0 broker emulator listening on a TCP port
	 * 
	 */
	export class AmqpServer extends EventEmitter<AmqpServerEvents_1> {
		
		constructor(options?: ServerOptions_1);
		
		options: Required<ServerOptions_1>;
		
		vhosts: Map<string, VHost>;
		
		shovels: Map<string, Shovel>;
		
		connections: Set<AnyConnection_1>;
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
		 * Azure Service Bus entities, locks and management state of a vhost
		 * @param vhost defaults to '/'
		 */
		serviceBus(vhost?: string): ServiceBus;
		/**
		 * Create a shovel that moves messages from a source queue to a destination exchange or queue,
		 * optionally across vhosts. The source message is acked once the destination publish is accepted.
		 * @param name unique shovel name
		 * */
		shovel(name: string, config: ShovelConfig): Shovel;
		/**
		 * Close and forget a shovel by name
		 * */
		closeShovel(name: string): void;
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
		#private;
	}
	type AnyConnection_1 = AmqpConnection | Amqp10Connection;
	/**
	 * Events emitted by the server: `connection` when a client completed its handshake, `error` for unexpected internal errors.
	 */
	type AmqpServerEvents_1 = {
		connection: [connection: AnyConnection_1];
		error: [err: Error, connection?: AnyConnection_1];
	};
	/**
	 * Events emitted by a 0-9-1 connection: `channel` when a channel opens, `channel.error` when the server closes a channel,
	 * `error.amqp` when the server closes the connection, `socket.error`, `heartbeat.timeout` and `close`.
	 * 
	 */
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
		/**
		 * Feed bytes from the socket. The first call carries the protocol header, already verified by the server.
		 * */
		feed(chunk: Buffer): void;
		#private;
	}
	/**
	 * Events emitted by a 0-9-1 connection: `channel` when a channel opens, `channel.error` when the server closes a channel,
	 * `error.amqp` when the server closes the connection, `socket.error`, `heartbeat.timeout` and `close`.
	 */
	type AmqpConnectionEvents_1 = {
		channel: [channel: AmqpChannel];
		"channel.error": [err: AmqpError, channel: AmqpChannel];
		"error.amqp": [err: AmqpError];
		"socket.error": [err: Error];
		"heartbeat.timeout": [];
		close: [];
	};
	/**
	 * Events emitted by a 1.0 connection: `error.amqp` when the server closes it with an error condition, and `close`.
	 * 
	 */
	/**
	 * One AMQP 1.0 client connection: SASL, open, sessions and links
	 * 
	 */
	export class Amqp10Connection extends EventEmitter<Amqp10ConnectionEvents_1> {
		
		constructor(server: AmqpServer, socket: import("node:net").Socket);
		id: number;
		
		protocol: "1.0";
		server: AmqpServer;
		socket: import("net").Socket;
		
		state: Amqp10ConnectionState;
		/** @type {string | null} remote container id from open */
		containerId: string | null;
		/** @type {string | null} authenticated user, anonymous when SASL ANONYMOUS was used */
		user: string | null;
		
		vhost: VHost | null;
		/** @type {Record<string, any>} connection properties from open */
		clientProperties: Record<string, any>;
		/** negotiated max frame size */
		maxFrameSize: number;
		channelMax: number;
		/** @type {Map<number, Session>} sessions by remote channel */
		sessions: Map<number, Session>;
		/** @type {Record<string, any> | null} error from close, ours or theirs */
		closeReason: Record<string, any> | null;
		/** @type {'amqp' | 'servicebus'} plain AMQP 1.0, or the Azure Service Bus dialect once the client used $cbs or $management */
		dialect: "amqp" | "servicebus";
		/** 1.0 has no connection.blocked, so never */
		get supportsBlocked(): boolean;
		/**
		 * Find a link we send on by name, used to answer request response links
		 * */
		findLink(name: string): Link;
		
		write(buffer: Buffer): void;
		
		send(channel: number, performative: Amqp10Described_1, payload?: Buffer): void;
		/**
		 * Feed bytes from the socket, or the bytes the server already read while sniffing the protocol
		 * */
		feed(chunk: Buffer): void;
		/** No-op, AMQP 1.0 has no blocked notification */
		notifyBlocked(): void;
		/** No-op, AMQP 1.0 has no blocked notification */
		notifyUnblocked(): void;
		/**
		 * Server initiated close. Accepts a 0-9-1 style AmqpError (code 320 becomes amqp:connection:forced)
		 * or an object with a 1.0 condition and description.
		 * */
		closeWithError(err: {
			condition?: string;
			description?: string;
			code?: number;
			replyText?: string;
		}): void;
		/** Drop the socket and release everything */
		destroy(): void;
		#private;
	}
	/**
	 * Events emitted by a 1.0 connection: `error.amqp` when the server closes it with an error condition, and `close`.
	 */
	type Amqp10ConnectionEvents_1 = {
		"error.amqp": [err: {
			condition: string;
			description?: string;
		}];
		close: [];
	};
	/**
	 * Resolve a plain AMQP 1.0 link address. `/queues/x`, `/queue/x`, `/amq/queue/x` and bare `x` are queues,
	 * `/exchanges/x/key` and `/exchange/x/key` are exchanges with an optional routing key.
	 * */
	export function resolveAmqp10Address(address: string): ResolvedAddress;
	/**
	 * One AMQP 1.0 session, multiplexing links on a channel
	 */
	class Session {
		/**
		 * @param channel remote channel, reused as our channel
		 * */
		constructor(connection: Amqp10Connection, channel: number, begin: Record<string, any>);
		connection: Amqp10Connection;
		channel: number;
		nextIncomingId: any;
		nextOutgoingId: number;
		incomingWindow: number;
		outgoingWindow: number;
		/** @type {Map<number, Link>} links by remote handle */
		links: Map<number, Link>;
		/** @type {Map<number, Link>} link per unsettled outgoing delivery id */
		unsettled: Map<number, Link>;
		
		send(performative: Amqp10Described_1, payload?: Buffer): void;
		
		handle(frame: Amqp10Frame_1): void;
		nextDeliveryId(): number;
		/** Release every link */
		teardown(): void;
		#private;
	}
	/**
	 * One link endpoint. When the remote is a receiver this link sends from a queue, otherwise it receives into the broker.
	 * On Service Bus connections queues are peek locked and `$cbs` and `$management` requests are answered.
	 */
	class Link {
		
		constructor(session: Session, attach: Record<string, any>);
		session: Session;
		connection: Amqp10Connection;
		vhost: VHost;
		broker: import("smqp").Broker;
		name: any;
		handle: any;
		/** true when we deliver to the remote receiver */
		sending: boolean;
		sndSettleMode: any;
		rcvSettleMode: any;
		credit: number;
		deliveryCount: any;
		/** @type {Map<number, Unsettled>} our unsettled deliveries */
		unsettled: Map<number, Unsettled>;
		
		address: ResolvedAddress | null;
		/** Service Bus semantics when the connection speaks that dialect, otherwise null */
		get serviceBus(): ServiceBus;
		
		onFlow(flow: Record<string, any>): void;
		
		onTransfer(transfer: Record<string, any>, payload: Buffer): void;
		/**
		 * Send a message payload to the remote receiver, splitting it by the negotiated frame size
		 * @param payload encoded message sections
		 * @returns delivery id
		 */
		transfer(payload: Buffer, { settled, tag }: {
			settled: boolean;
			tag?: Buffer;
		}): number;
		/**
		 * Drop an unsettled delivery without touching the message
		 * */
		forget(deliveryId: number): void;
		/**
		 * Apply a disposition from the remote receiver
		 * */
		settle(deliveryId: number, state: Record<string, any> | undefined, remoteSettled: boolean): void;
		/** Remote detached: release and confirm */
		onDetach(): void;
		/** Cancel the consumer, requeue plain unsettled deliveries and drop any temporary queue */
		teardown(): void;
		#private;
	}
	type ResolvedAddress = {
		kind: "queue";
		queue: string;
		entity?: string;
	} | {
		kind: "exchange";
		exchange: string;
		routingKey: string;
	} | {
		kind: "topic";
		entity: string;
	} | {
		kind: "cbs";
	} | {
		kind: "management";
		entity: string;
	};
	type Unsettled = {
		message: import("smqp").Message;
		lockKey?: string;
	};
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
		/** @type {Map<string, import('../../types/interfaces.d.ts').ConsumerEntry>} consumers by client facing consumer tag */
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
		/** @type {Map<string, number>} exclusive queue name -> owning connection id */
		exclusiveQueues: Map<string, number>;
		/** @type {import('../types/interfaces.d.ts').Publishing | null} channel currently publishing, for basic.return correlation */
		publishing: Publishing_1 | null;
		/** Azure Service Bus semantics for this vhost, created on first use */
		get serviceBus(): ServiceBus;
		/** Expire messages, release expired locks and enqueue due scheduled messages */
		sweep(): void;
		/**
		 * @returns AMQP exchange type if the exchange exists
		 */
		getExchangeType(name: string): string | undefined;
		/**
		 * @param type AMQP exchange type: direct, topic or fanout
		 * 
		 */
		declareExchange(name: string, type: import("smqp").exchangeType, options?: import("smqp").ExchangeOptions): void;
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
	 * RabbitMQ style shovel: consume a source queue and republish every message to a destination
	 * exchange (with a routing key) or queue. Either side may be this emulator or a remote broker
	 * reached over AMQP with a `uri`. On-confirm semantics — the source message is acked once the
	 * destination has accepted the message (a remote destination is published in confirm mode).
	 */
	export class Shovel {
		/**
		 * @param name unique shovel name
		 * */
		constructor(name: string, config: ShovelConfig, server: AmqpServer);
		name: string;
		source: ShovelSource;
		destination: ShovelDestination;
		/** Resolves once the shovel is connected and consuming; rejects if a remote leg fails to connect */
		ready: Promise<void>;
		/** Stop the shovel, cancelling the source consumer and dropping any remote connections */
		close(): void;
		#private;
	}
	type ShovelSource = {
		/**
		 * source queue to consume
		 */
		queue: string;
		/**
		 * amqp uri of a remote broker to consume from; omit for this emulator
		 */
		uri?: string;
		/**
		 * vhost the source queue lives in when local, defaults to '/'
		 */
		vhost?: string;
	};
	type ShovelDestination = {
		/**
		 * destination exchange to publish to
		 */
		exchange?: string;
		/**
		 * routing key for the destination exchange, defaults to the message's own
		 */
		routingKey?: string;
		/**
		 * destination queue to publish to, when no exchange is given
		 */
		queue?: string;
		/**
		 * amqp uri of a remote broker to publish to; omit for this emulator
		 */
		uri?: string;
		/**
		 * vhost the destination lives in when local, defaults to '/'
		 */
		vhost?: string;
	};
	type ShovelConfig = {
		source: ShovelSource;
		destination: ShovelDestination;
	};
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
	/**
	 * @typedef {object} Incoming a delivery whose body frames are still arriving
	 * @property {any} fields
	 * @property {any} properties
	 * @property {Buffer[]} chunks
	 * @property {number} received
	 * @property {number} bodySize
	 */
	/**
	 * Minimal outbound AMQP 0-9-1 client used by the shovel to reach a remote broker. It speaks
	 * just enough of the protocol to consume a queue and confirm-publish, built on this package's
	 * own codec so the shovel needs no third party client at runtime.
	 */
	export class AmqpClient {
		/** @param uri e.g. amqp://user:pass@host:5672/vhost */
		constructor(uri: string);
		/** Connect, run the handshake and open a channel */
		connect(): Promise<void>;
		/** Put the channel into publisher confirm mode */
		selectConfirms(): Promise<void>;
		/**
		 * Publish and resolve when the broker confirms it (basic.ack).
		 * */
		publishConfirm(exchange: string, routingKey: string, content: Buffer, properties?: MessageProperties_1): Promise<void>;
		/**
		 * Consume a queue; `onDelivery` is called for every message with its content and properties.
		 * */
		consume(queue: string, onDelivery: (delivery: ClientDelivery) => void): Promise<void>;
		
		ack(deliveryTag: number): void;
		/** Drop the connection */
		close(): void;
		#private;
	}
	type ClientDelivery = {
		/**
		 * basic.deliver fields (deliveryTag, exchange, routingKey)
		 */
		fields: MethodFields_1;
		properties: MessageProperties_1;
		content: Buffer;
	};
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
	authenticate?: ((user: string, password: string, connection: AuthenticatingConnection_1) => boolean) | null;
	/** version string reported in server properties */
	version?: string;
	/** how often expired messages are swept from queues in ms, dead lettering them when configured, defaults to 100, 0 disables */
	evictInterval?: number;
	/** Service Bus peek lock duration in ms, defaults to 60000 */
	lockDuration?: number;
  }

  /** the connection being authenticated, either protocol */
  interface AuthenticatingConnection_1 {
	id: number;
	clientProperties: Record<string, any>;
	socket: import('node:net').Socket;
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

  /** whoever owns a consumer and wants to know when its queue disappears */
  interface ConsumerOwner {
	onConsumerCancelled(entry: ConsumerEntry_1): void;
  }

  interface ConsumerEntry_1 {
	/** client facing consumer tag */
	tag: string;
	noAck: boolean;
	channel: ConsumerOwner;
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
	channel: { onReturn(event: any, publishing: Publishing_1): void };
	properties: MessageProperties_1;
  }

  type ChannelState_1 = 'open' | 'closing' | 'closed';

  type ConnectionState_1 = 'protocol-header' | 'start' | 'tune' | 'open' | 'running' | 'closing' | 'closed';

  interface Amqp10Field {
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
  interface Amqp10Composite_1 {
	name: string;
	/** numeric descriptor, domain shifted 32 bits plus id */
	descriptor: number;
	symbol: string;
	source: string;
	provides: string[];
	fields: Amqp10Field[];
  }

  /** AMQP 1.0 restricted type, a primitive with a name, choices and sometimes a descriptor (message sections) */
  interface Amqp10Restricted_1 {
	name: string;
	source: string;
	choices: Array<[string, string]>;
	descriptor?: number;
	symbol?: string;
  }

  /** decoded described value: known composites and sections come back with `$type`, unknown descriptors with `$descriptor` */
  type Amqp10Described_1 = { $type: string; [field: string]: any } | { $descriptor: number | string; value: any };

  interface Amqp10Frame_1 {
	/** 0 amqp, 1 sasl */
	type: number;
	channel: number;
	/** null for empty (heartbeat) frames */
	performative: Amqp10Described_1 | null;
	payload: Buffer;
  }

  type Amqp10ConnectionState = 'header' | 'sasl' | 'sasl-done' | 'open' | 'running' | 'closing' | 'closed';

  /** decoded AMQP 1.0 message, sections by name plus the body sections in order */
  interface Amqp10Message_1 {
	header?: Record<string, any>;
	deliveryAnnotations?: Record<string, any>;
	messageAnnotations?: Record<string, any>;
	properties?: Record<string, any>;
	applicationProperties?: Record<string, any>;
	body: Array<{ $type: 'data' | 'amqp-sequence' | 'amqp-value'; value: any }>;
	footer?: Record<string, any>;
  }
	export function amqp10Typed(type: string, value: any): Typed;
	/**
	 * Encode a value to a buffer
	 * 
	 */
	export function encodeAmqp10(value: any, type?: string): Buffer<ArrayBufferLike>;
	/**
	 * Decode a single value from a buffer
	 * */
	export function decodeAmqp10(buffer: Buffer): any;
	/**
	 * Encode a frame: 8 byte header, performative and payload. A null performative gives an empty (heartbeat) frame.
	 * @param type 0 amqp, 1 sasl
	 * 
	 */
	export function encodeAmqp10Frame(type: number, channel: number, performative: Amqp10Described_1 | null, payload?: Buffer): Buffer<ArrayBufferLike>;
	/**
	 * Force an encoding type for a value in a `*` position, e.g. `typed('ulong', 5)`
	 */
	class Typed {
		
		constructor(type: string, value: any);
		type: string;
		value: any;
	}
	/**
	 * Incremental AMQP 1.0 frame parser
	 */
	export class Amqp10FrameParser {
		pending: Buffer<ArrayBuffer>;
		
		feed(chunk: Buffer): Amqp10Frame_1[];
	}
	/**
	 * Decode the message sections carried by transfer frames
	 * */
	export function decodeAmqp10Message(payload: Buffer): Amqp10Message_1;
	/**
	 * Encode an smqp message as 1.0 sections. Messages that arrived over 1.0 keep their sections,
	 * messages from 0-9-1 clients or the broker api are mapped from their properties.
	 * @param extra message annotations to merge in and a delivery count for the header
	 */
	export function encodeAmqp10Message(message: import("smqp").Message, extra?: {
		annotations?: Record<string, any>;
		deliveryCount?: number;
	}): Buffer<ArrayBufferLike>;
	/**
	 * Error answered on a management request
	 */
	export class ServiceBusError extends Error {
		
		constructor(statusCode: number, condition: string, description: string);
		statusCode: number;
		condition: string;
	}

	/**
	 * Azure Service Bus semantics on top of a vhost: entities, peek lock, deferral, dead lettering, scheduling and the
	 * `$cbs` and `$management` request response operations the SDK uses
	 */
	export class ServiceBus {
		
		constructor(vhost: VHost, server: AmqpServer);
		vhost: VHost;
		server: AmqpServer;
		broker: import("smqp").Broker;
		sequence: number;
		/** @type {Map<string, Lock>} active locks by lock token string */
		locks: Map<string, Lock>;
		/** @type {Map<string, Map<number, import('smqp').Message>>} deferred messages by entity and sequence number */
		deferred: Map<string, Map<number, import("smqp").Message>>;
		/** @type {Map<number, { entity: string, content: Buffer, properties: any, at: number }>} scheduled messages by sequence number */
		scheduled: Map<number, {
			entity: string;
			content: Buffer;
			properties: any;
			at: number;
		}>;
		get lockDuration(): number;
		/**
		 * Create a queue entity
		 * */
		createQueue(name: string): string;
		/**
		 * Create a topic entity, a fanout exchange
		 * */
		createTopic(name: string): string;
		/**
		 * Create a subscription, a queue bound to the topic, addressed as `topic/Subscriptions/name`
		 * */
		createSubscription(topic: string, name: string): string;
		/**
		 * Resolve a Service Bus address, creating queues and subscriptions on first use
		 * */
		resolve(address: string): ServiceBusAddress;
		/**
		 * Unpack batches and give every message an id, like the service does
		 * */
		prepare(message: Amqp10Message_1, messageFormat: number): Amqp10Message_1[];
		/**
		 * Service Bus state of a message, assigned on first contact
		 * */
		stateOf(message: import("smqp").Message): MessageState;
		/**
		 * Encode a message with the Service Bus annotations
		 * 
		 */
		encode(message: import("smqp").Message, lockedUntil?: number): Buffer<ArrayBufferLike>;
		/**
		 * Lock a message for peek lock delivery
		 * */
		lock(message: import("smqp").Message, entity: string, link: Link | null, deliveryId: number | null): Lock;
		/**
		 * Extend a lock
		 * @returns new expiry
		 */
		renew(key: string): number;
		/**
		 * Settle a locked message
		 * @param key lock token string
		 * 
		 */
		settle(key: string, status: "completed" | "abandoned" | "defered" | "suspended", options?: {
			deadLetterReason?: string;
			deadLetterDescription?: string;
			propertiesToModify?: Record<string, any>;
		}): void;
		/**
		 * Keep locks alive when the link that delivered them goes away; the SDK settles them through $management
		 * */
		detachLink(link: Link): void;
		/**
		 * Copy a message into the entity's dead letter queue
		 * */
		deadLetter(message: import("smqp").Message, entity: string, options: {
			deadLetterReason?: string;
			deadLetterDescription?: string;
			propertiesToModify?: Record<string, any>;
		}): void;
		/**
		 * Release expired locks and enqueue due scheduled messages; called by the vhost sweeper
		 * 
		 */
		sweep(now?: number): void;
		/**
		 * Messages visible to peek: everything in the queue plus deferred ones, ordered by sequence number
		 * */
		peek(entity: string, from: number, count: number): import("smqp").Message[];
		/**
		 * Handle a `$cbs` or `$management` request and build the response message bytes
		 * */
		handleRequest(kind: "cbs" | "management", entity: string, request: Amqp10Message_1): Buffer;
		#private;
	}
	type MessageState = {
		sequenceNumber: number;
		enqueuedTime: number;
		deliveryCount: number;
		deadLetterSource?: string;
		scheduledEnqueueTime?: number;
	};
	type Lock = {
		key: string;
		token: Buffer;
		message: import("smqp").Message;
		entity: string;
		link: Link | null;
		deliveryId: number | null;
		expires: number;
	};
	type ServiceBusAddress = {
		kind: "cbs";
	} | {
		kind: "management";
		entity: string;
	} | {
		kind: "topic";
		entity: string;
	} | {
		kind: "queue";
		entity: string;
		queue: string;
	};
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