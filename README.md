# @0dep/amqp-emulator

AMQP 0-9-1 broker emulator for tests. It listens on a real TCP port and speaks the wire protocol, so any AMQP 0-9-1 client works against it: [amqplib](https://www.npmjs.com/package/amqplib), rascal, amqp-connection-manager, pika, or anything else that talks to RabbitMQ. Broker semantics come from [smqp](https://www.npmjs.com/package/smqp).

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
broker.assertQueue('inbox', { durable: false });
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

## Known differences from RabbitMQ

- Headers exchanges are refused with 540 NOT_IMPLEMENTED.
- `tx.select`, `tx.commit` and `tx.rollback` are acknowledged but nothing is transactional.
- Deliveries are not round-robined between consumers on the same queue. The first consumer with capacity wins, so use prefetch when several consumers share a queue.
- `basic.qos` without `global` applies to consumers created after the call, like RabbitMQ 3.3 and later. Channel wide prefetch is supported.
- Nothing is persisted. `durable` is recorded and checked on redeclare, but a `reset()` or process exit loses everything.
- Only AMQP 0-9-1. AMQP 1.0 clients, such as Azure Service Bus SDKs, will not connect.

## Development

```bash
npm test          # mocha, then lint, type generation, tsc over the tests and the README examples
npm run generate  # regenerate src/defs.js from spec/amqp-rabbitmq-0.9.1.json
npm run dist      # generate types/index.d.ts from JSDoc with dts-buddy
npm run test:md   # execute the README examples with texample
npm run cov:html  # c8 coverage report
```

Tests are BDD style with mocha-cakes-2 and run the real amqplib client against the emulator. Protocol edge cases use a raw frame client in `test/helpers/raw-client.js`.
