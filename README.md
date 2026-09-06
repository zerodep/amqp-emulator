# @0dep/amqp-emulator

AMQP broker emulator for tests. It listens on a real TCP port and speaks both AMQP 0-9-1 and AMQP 1.0 on that port, so any client works against it: [amqplib](https://www.npmjs.com/package/amqplib), rascal, amqp-connection-manager and pika over 0-9-1, [rhea](https://www.npmjs.com/package/rhea) and other 1.0 clients over 1.0, and the [Azure Service Bus SDK](https://www.npmjs.com/package/@azure/service-bus) through its Service Bus dialect. All of them share the same queues, so a Service Bus sender can feed an amqplib consumer. Broker semantics come from [smqp](https://www.npmjs.com/package/smqp).

It is not RabbitMQ. It is small, in memory, single process, and meant to make integration tests fast and deterministic without a broker container.

## Usage

```javascript
import amqplib from 'amqplib';
import { AmqpServer } from '@0dep/amqp-emulator';

const server = new AmqpServer();
const { url } = await server.listen(); // random free port on 127.0.0.1

const connection = await amqplib.connect(url);
const channel = await connection.createChannel();
await channel.assertQueue('orders');
channel.sendToQueue('orders', Buffer.from('hello'));

const message = await channel.get('orders');
console.log(message.content.toString()); // hello

await connection.close();
await server.close();
```

Seed or inspect the broker directly from the test side:

```javascript
import amqplib from 'amqplib';
import { AmqpServer } from '@0dep/amqp-emulator';

const server = new AmqpServer();
const { url } = await server.listen();

const broker = server.getBroker();
broker.assertQueue('inbox', { durable: false, autoDelete: false });
broker.sendToQueue('inbox', { seeded: true }, { contentType: 'application/json' });

const connection = await amqplib.connect(url);
const channel = await connection.createChannel();
const message = await channel.get('inbox');
console.log(JSON.parse(message.content.toString())); // { seeded: true }

console.log(server.getStats()); // { messageCount: 1, unackedCount: 1, consumerCount: 0, queues: [...] }

channel.ack(message);
await connection.close();
await server.close();
```

AMQP 1.0 clients connect to the same port. Queues are addressed by name, exchanges as `/exchanges/name/routing-key`:

```javascript
import rhea from 'rhea';
import { AmqpServer } from '@0dep/amqp-emulator';

const server = new AmqpServer();
const { port } = await server.listen();

const container = rhea.create_container();
const connection = container.connect({ host: '127.0.0.1', port, reconnect: false });
await new Promise((resolve) => connection.once('connection_open', resolve));

const received = new Promise((resolve) => {
  connection.open_receiver('greetings').on('message', (context) => resolve(context.message.body));
});
const sender = connection.open_sender('greetings');
sender.on('sendable', () => sender.send({ body: 'hello from 1.0' }));

console.log(await received); // hello from 1.0

connection.close();
await new Promise((resolve) => connection.once('connection_close', resolve));
await server.close();
```

The Azure Service Bus SDK connects with a development emulator connection string, no TLS involved:

```javascript
import { ServiceBusClient } from '@azure/service-bus';
import { AmqpServer } from '@0dep/amqp-emulator';

const server = new AmqpServer();
const { port } = await server.listen();
const connectionString = `Endpoint=sb://127.0.0.1:${port}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true`;

const client = new ServiceBusClient(connectionString);
const sender = client.createSender('orders');
await sender.sendMessages({ body: { id: 1 }, applicationProperties: { tenant: 'acme' } });

const receiver = client.createReceiver('orders');
const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
console.log(message.body, message.deliveryCount); // { id: 1 } 0
await receiver.completeMessage(message);

await client.close();
await server.close();
```

## API

### `new AmqpServer([options])`

| option          | default   | description                                                               |
| --------------- | --------- | ------------------------------------------------------------------------- |
| `heartbeat`     | `60`      | heartbeat interval proposed to clients in seconds, `0` disables           |
| `frameMax`      | `131072`  | max frame size in bytes                                                   |
| `channelMax`    | `2047`    | max channels per connection, `0` means no limit                           |
| `closeTimeout`  | `1000`    | ms to wait for `connection.close-ok` before dropping the socket           |
| `vhosts`        | `null`    | allowed vhost names, `null` accepts any and creates them on demand        |
| `authenticate`  | `null`    | `(user, password, connection) => boolean`, `null` accepts everyone        |
| `version`       | `'0.1.0'` | version string in server properties                                       |
| `evictInterval` | `100`     | ms between sweeps that expire messages and dead letter them, `0` disables |

- `listen([port], [host])` resolves with `{ port, host, url }`. Port defaults to `0`, host to `127.0.0.1`.
- `url`, `port`, `host` of the listening server.
- `getBroker([vhost])` the smqp broker behind a vhost, for inspecting queues or seeding messages. Seeded content may be a Buffer, a string or an object; objects are delivered as JSON.
- `getStats([vhost])` message, unacked and consumer counts, totals and per queue, handy for assertions.
- `serviceBus([vhost])` the Service Bus layer of a vhost: `createQueue(name)`, `createTopic(name)`, `createSubscription(topic, name)`, plus `locks`, `deferred` and `scheduled` for inspection. Queues and subscriptions are also created on first use, topics must be created up front since a bare name otherwise means a queue.
- `getVHost([vhost])` the vhost wrapper with `broker`, declared `exchangeTypes` and `exclusiveQueues`.
- `connections` set of live `AmqpConnection` objects, each with `channels`, `user`, `vhost`, `clientProperties`.
- `disconnect([reason], [code])` sends `connection.close` to every client, 320 CONNECTION_FORCED by default. Useful for testing reconnect logic.
- `block([reason])` / `unblock()` send `connection.blocked` / `connection.unblocked` to clients that support it.
- `reset()` drops all connections and clears every vhost, the default exchanges stay.
- `close()` stops listening and destroys all connections.

Events: `connection` when a client completes the handshake, `error` for unexpected internal errors. Connections emit `channel`, `channel.error`, `error.amqp`, `socket.error`, `heartbeat.timeout` and `close`.

Also exported for protocol level work: `constants`, `methods`, `encodeMethod`, `decodeMethod`, `encodeProperties`, `decodeProperties`, `FrameParser`, `BufferReader`, `BufferWriter` and `AmqpError`.

## What is emulated

- Connection handshake with PLAIN and AMQPLAIN, tune negotiation, heartbeats, `connection.blocked`, `update-secret`.
- Channels with delivery tags per channel, `basic.qos` per consumer and channel wide, publisher confirms, `mandatory` returns, consumer cancel notifications.
- Exchanges: direct, topic and fanout, exchange to exchange bindings, `amq.direct`, `amq.topic` and `amq.fanout` predeclared, reserved `amq.` prefix.
- Queues: server named, durable, exclusive per connection, auto delete, `x-message-ttl`, `x-max-length`, `x-dead-letter-exchange`, `x-dead-letter-routing-key`, per message `expiration`. Expired messages are swept and dead lettered even without consumers.
- Error semantics like RabbitMQ: 404 NOT_FOUND, 406 PRECONDITION_FAILED on inequivalent redeclare or unknown delivery tag, 405 RESOURCE_LOCKED, 403 ACCESS_REFUSED, 530 NOT_ALLOWED, 540 NOT_IMPLEMENTED, and connection level 501/503/504/505 for protocol violations.

### AMQP 1.0

- SASL ANONYMOUS and PLAIN, or no SASL when no `authenticate` option is set. The same `authenticate` callback serves both protocols.
- Sessions and links with credit based flow control, transfers split by the negotiated frame size, dispositions `accepted`, `released`, `rejected` and `modified`, pre-settled deliveries, drain and echo flows.
- Addresses: `name`, `/queues/name`, `/queue/name` and `/amq/queue/name` are queues, created on first use. `/exchanges/name/key` and `/exchange/name/key` publish to an exchange or, for receivers, bind a server named queue with that key. Dynamic sources get a server named queue that is removed on detach.
- Messages arriving over 1.0 keep all their sections for 1.0 receivers. For 0-9-1 consumers the body becomes the content, `properties` map onto basic properties and `application-properties` become headers, and the other way around.
- Deleting a queue detaches its receivers with `amqp:resource-deleted`, unsettled deliveries are requeued when a link, session or connection ends, and `server.disconnect()` closes 1.0 connections with `amqp:connection:forced`.

### Azure Service Bus

A connection is treated as Service Bus once the client announces itself as the SDK or attaches to `$cbs` or `$management`.

- `$cbs` accepts any put-token, so any `SharedAccessKey` works.
- Queues, topics with subscriptions (`topic/Subscriptions/name`) and dead letter sub queues (`entity/$DeadLetterQueue`).
- Peek lock with lock tokens, `x-opt-sequence-number`, `x-opt-enqueued-time` and `x-opt-locked-until` annotations, delivery counts, lock expiry and renewal, receive and delete mode, batches sent with `sendMessages([...])`.
- Settlement by disposition: complete, abandon, defer and dead letter with reason and description. Settlement over `$management` for messages whose receiver link is gone, answered with lock lost when the lock expired.
- `$management` operations: peek, renew lock, schedule and cancel scheduled messages, receive deferred messages by sequence number, update disposition.
- Not implemented: sessions, subscription rules and filters, transactions, `$management` operations for session state and rules.

## Known differences from RabbitMQ

- Headers exchanges are refused with 540 NOT_IMPLEMENTED.
- `tx.select`, `tx.commit` and `tx.rollback` are acknowledged but nothing is transactional.
- Deliveries are not round-robined between consumers on the same queue. The first consumer with capacity wins, so use prefetch when several consumers share a queue.
- `basic.qos` without `global` applies to consumers created after the call, like RabbitMQ 3.3 and later. Channel wide prefetch is supported.
- Nothing is persisted. `durable` is recorded and checked on redeclare, but a `reset()` or process exit loses everything.
- AMQP 1.0 covers the core protocol as used by rhea plus the Service Bus dialect above. Transactions and TLS are not implemented.

## Abbreviations

- **AMQP** Advanced Message Queuing Protocol. 0-9-1 is the RabbitMQ wire protocol, 1.0 is the OASIS standard with a different type system and model.
- **SASL** Simple Authentication and Security Layer, the negotiation that carries credentials at the start of an AMQP 1.0 connection. ANONYMOUS and PLAIN are its mechanisms.
- **CBS** Claims Based Security, the `$cbs` link Azure clients use to hand the service a token after connecting.
- **SAS** Shared Access Signature, the token type Service Bus connection strings produce.
- **SDK** Software Development Kit, here the `@azure/service-bus` client library.
- **DLQ** Dead Letter Queue, the `$DeadLetterQueue` sub queue that receives messages a consumer gave up on.
- **TTL** Time To Live, how long a message stays in a queue before it expires.
- **QoS** Quality of Service, the 0-9-1 method that sets prefetch.
- **JSDoc** the comment syntax the types are generated from.

## Development

```bash
npm test          # mocha, then lint, type generation, tsc over the tests and the README examples
npm run generate:amqp09  # regenerate src/amqp09/defs.js from spec/amqp-rabbitmq-0.9.1.json
npm run generate:amqp10  # regenerate src/amqp10/defs.js from the OASIS spec xml in spec/
npm run dist      # generate types/index.d.ts from JSDoc with dts-buddy
npm run test:md   # execute the README examples with texample
npm run cov:html  # c8 coverage report
```

Tests are BDD style with mocha-cakes-2 and run the real amqplib and rhea clients against the emulator. Protocol edge cases use raw frame clients in `test/helpers/raw091-client.js` and `test/helpers/raw10-client.js`.

## Licenses and trademarks

Not affiliated with or endorsed by Microsoft, Broadcom/VMware, or OASIS. Azure, Azure Service Bus, RabbitMQ, and AMQP are trademarks of their respective owners and are used here only to describe interoperability.

- `spec/amqp-rabbitmq-0.9.1.json`: the AMQP 0-9-1 protocol definition, (c) VMware, Inc., MIT licensed, with its notice retained in the file. `src/amqp09/defs.js` is generated from it.
- `spec/amqp-core-*-v1.0.xml`: the AMQP 1.0 core specification, (c) OASIS Open 2012, redistributed under the OASIS IPR Policy with its copyright notice retained. `src/amqp10/defs.js` is generated from it.
- The `com.microsoft:*` operations and `$cbs` / `$management` behaviour reproduce the Azure Service Bus wire protocol for testing against the `@azure/service-bus` SDK (MIT licensed).

See [NOTICE](./NOTICE) for the full attributions.
