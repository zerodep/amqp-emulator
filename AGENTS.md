# AGENTS.md

Guidance for agents working in this repository.

## What this is

`amqp-emulator` is an AMQP broker emulator that listens on a real TCP port and speaks 0-9-1 and 1.0 on the same port, sniffing the 8 byte protocol header. Broker semantics (exchanges, queues, bindings, acks, dead lettering, ttl) come from `smqp`; this package adds the wire protocols, connection, channel, session and link state, and RabbitMQ-like error behaviour on top. ESM only, Node 24+.

## Working rules

- **TDD.** Write or extend a scenario in `test/feature/*-feature.js` first, run it red, then change `src/`. Coverage is 100% statements, branches, functions and lines and must stay there: `npx c8 -n src -r text mocha`. If a line cannot be reached from a test, delete it rather than leave it.
- **BDD with mocha-cakes-2.** `Feature` / `Scenario` / `Given` / `When` / `Then` / `And`. Every step is a mocha test, so a feature level `afterEach` runs after every step. Use `afterEachScenario` for per scenario cleanup. Steps share state through `let` variables declared in the scenario.
- **Real clients.** Tests drive the emulator with amqplib (0-9-1), rhea (1.0) and `@azure/service-bus` (Service Bus dialect) over TCP (`test/helpers/index.js` starts a server on a random port and tracks amqplib connections). Protocol edge cases the clients cannot produce use `test/helpers/raw091-client.js` and `test/helpers/raw10-client.js`, minimal frame level clients built on this package's codecs.
- **Import the package by name in tests** (`amqp-emulator`) so `tsc -p test` type checks tests against the generated `types/index.d.ts`.
- **No commits, no branches.** The user reviews and commits.
- `npm test` runs mocha, then `posttest`: `toc` (regenerates the README table of contents), eslint + prettier, `dts-buddy` type generation, `tsc -p test` and `texample ./README.md`, which executes every ` ```javascript ` block in the README against the real package. Use ` ```js ` for snippets that must not run. All must pass.

## Layout

- `spec/amqp-rabbitmq-0.9.1.json` RabbitMQ's protocol spec. `scripts/generate-amqp09-defs.js` turns it into `src/amqp09/defs.js` (`npm run generate:amqp09`). Never edit `defs.js` by hand. The generator asserts no method has more than 8 consecutive bit arguments, which the codec relies on.
- `src/amqp09/codec.js` table driven method and property encoding, field tables with amqplib compatible type tags (`{ '!': 'timestamp', value }` etc.). `src/amqp09/frame.js` incremental frame parser.
- `src/server.js` `AmqpServer`: net server, vhosts, block/unblock, disconnect, reset, shovels, topology. `load(definitions)` applies a RabbitMQ definitions export shaped object (plus Service Bus `topics`) and remembers it in `server.definitions`; `reset()` replays them after clearing the vhosts. `server.autoCreate` is the normalized `autoCreate` option (`{ exchangeType }` or `null`).
- `src/shovel.js` `Shovel`: RabbitMQ style shovel created with `server.shovel(name, { source, destination })`. Consumes a source queue and republishes to a destination exchange (routing key, or the message's own as fallback) or queue, across vhosts, acking the source on-confirm. Either side may be remote via a `uri`; the remote leg speaks 0-9-1 through `src/amqp09/client.js` `AmqpClient`, a minimal outbound client built on this package's own codec so nothing is added to runtime deps. Registered in `server.shovels`, closed on `server.close`/`reset` and by `server.closeShovel(name)`. A remote leg connects asynchronously; `shovel.ready` resolves once it is consuming (rejects if a remote connect fails).
- `src/vhost.js` `VHost`: one smqp `Broker` plus what smqp lacks: exclusive queue ownership and the `publishing` slot used to correlate `basic.return`. `declareQueue` maps `x-` arguments to smqp options for the channel and for definitions. `ensureExchange`/`ensureQueue` are the single place `autoCreate` is implemented: they return the existing resource, create it durable with the default setup when the option is on, or return undefined so callers throw their protocol's not found error. All 0-9-1 handlers and the 1.0 link resolve exchanges and queues through them.
- `src/amqp09/connection.js` `AmqpConnection`: protocol header, start/tune/open handshake, frame dispatch, heartbeats, close handshake. Internals are `#private`.
- `src/amqp09/channel.js` `AmqpChannel`: method handlers live in a `static #handlers` map inside the class body so they can reach private members; they run with the channel as `this`. Public methods called by connection and vhost: `teardown`, `onConsumerCancelled`, `onReturn`, `close`.
- `src/amqp09/errors.js` `AmqpError` carries `code`, `replyText`, `killConnection` and the offending `method`. Channel level errors become `channel.close`, connection level ones `connection.close`. Anything thrown that is not an `AmqpError` is a bug: it becomes 541 INTERNAL_ERROR and the server emits `error`.
- `spec/amqp-core-*-v1.0.xml` the OASIS AMQP 1.0 core spec. `scripts/generate-amqp10-defs.js` turns it into `src/amqp10/defs.js` (`npm run generate:amqp10`): composites (performatives, termini, sections with fields and descriptors), restricted types and primitive encodings. Never edit by hand.
- `src/amqp10/types.js` the 1.0 type system: `encodeValue`/`decodeValue` driven by the generated tables. Values are plain JS; described values decode to `{ $type, ...fields }` for composites, `{ $type, value }` for sections and `{ $descriptor, value }` for unknown descriptors. `typed(type, value)` forces an encoding in `*` positions. Frames are `encodeFrame` and `Amqp10FrameParser`.
- `src/amqp10/connection.js` `Amqp10Connection`: SASL, open, sessions; `src/amqp10/link.js` `Session` and `Link`: attach, flow, transfer, disposition, detach mapped onto smqp queues and exchanges; `src/amqp10/message.js` maps 1.0 sections to and from smqp content and 0-9-1 style properties, keeping the original sections under `kSections` for 1.0 receivers.
- `src/server.js` sniffs the protocol header per socket and instantiates the matching connection class; sockets that have not sent a header yet are tracked separately and dropped on `disconnect`, `reset` and `close`. Both connection classes expose `feed`, `closeWithError`, `destroy`, `notifyBlocked` and `notifyUnblocked` so the server treats them alike.
- `types/interfaces.d.ts` hand written shared interfaces. Keep it free of references to this package's own classes; dts-buddy cannot follow those, and tsc treats the generated classes as nominal because of `#private`. Event maps and unions over the classes are `@typedef`s in the source modules instead., referenced from JSDoc as `import('types').X` via the `paths` alias in `tsconfig.json`. `types/index.d.ts` is generated by dts-buddy; interface types are re-exported through `@typedef` lines at the bottom of `src/index.js`.

## smqp integration details worth knowing

- **Delivery tags** are per channel, kept in `channel.unacked` (delivery tag to smqp `Message`). Ack, nack and reject settle through the smqp message; unknown tags close the channel with 406.
- **Deliveries are deferred until `consume-ok` is on the wire.** smqp pushes pending messages synchronously inside `queue.consume()`, before we have replied. `#deferred` buffers them.
- **Channel wide prefetch** is emulated through the smqp `capacity` consume option, a hook returning the room left under the channel prefetch. smqp calls it whenever it grabs messages, including during consumer creation. After acks, `#poke()` nudges every consumed queue with `queue.consumeNext()` because smqp only re-evaluates the acked message's own queue.
- **`basic.cancel` keeps unacked messages unacked** with smqp `consumer.cancel({ keepPending: true })`. Channel and connection close do requeue, on purpose.
- **Wire properties are kept verbatim** under the `kWireProperties` symbol on the smqp properties object because smqp adds `messageId`, `timestamp` (milliseconds) and parses `expiration`. Messages seeded through the broker api lack the symbol and fall back to a filtered copy; their content may be a string or object and is coerced to a Buffer.
- **`basic.return` correlation**: `vhost.publishing` is set only during the synchronous `broker.publish()` call, and the broker `return` event fires synchronously, so the current publisher is always the right channel.
- **Consumer tags** given to smqp are `${connectionId}.${channelId}.${tag}` because smqp requires broker wide uniqueness while AMQP only requires per channel uniqueness.
- **Expiry is proactive.** smqp only evicts expired messages lazily when something consumes; `VHost` runs `broker.evictExpired()` on an unref'd interval (`evictInterval` option) so ttl messages dead letter without consumers, and `queue.declare` evicts before counting. Tests mock `setInterval` and `Date` together with `mock.timers` to drive this deterministically.
- **Counts come from `queue.getStats()`** (`messageCount - unackedCount` is the ready count reported in `declare-ok`); `server.getStats(vhost)` exposes the broker totals.
- **Queue deletion notifies consumers** through `vhost.deleteQueue`, which reads the consumer entry passed to smqp as the consume `owner`, then sends `basic.cancel` to the owning channel.

## AMQP 1.0 details worth knowing

- **Credit.** A link that sends to the client gets an smqp consumer with `prefetch: Infinity` and a `capacity` hook returning the link credit, which starts at zero so nothing is grabbed until the first `flow`. Remote `flow` sets `credit = remoteDeliveryCount + linkCredit - ourDeliveryCount`, then the queue is nudged with `queue.consumeNext()`. Links that receive from the client hand out 100 credits and top up below 50.
- **Settlement.** Our outgoing deliveries are kept in `link.unsettled` and `session.unsettled` by delivery id until a disposition arrives: accepted acks, released and modified requeue (modified with `undeliverable-here` drops), rejected drops. Pre-settled (`snd-settle-mode` 1) deliveries are acked at once. Unsettled ones are requeued on detach, end and close.
- **rhea quirks pinned by tests.** rhea confirms a server initiated detach with a detach of its own, which must not be treated as an unknown handle. rhea batches settlements of adjacent deliveries issued in the same tick into one disposition carrying the first state, so tests settle one at a time. rhea reuses handle 0 after a detach. rhea's `receiver.drain` expects the answering flow to carry `drain: true`.
- **Seeding for 1.0 tests.** `broker.assertQueue(name, { durable: false, autoDelete: false })`: smqp defaults `autoDelete` to true, so a seeded queue without the flag vanishes when the first receiver detaches.
- **Errors.** `LinkError(condition, description)` thrown anywhere in frame handling closes the connection with that condition; thrown while attaching it detaches the link instead. Anything else is a bug: `amqp:internal-error` plus a server `error` event. A payload that is not a valid message section is `amqp:decode-error`.
- **Not implemented.** Transactions, `$cbs`, `$management`, idle timeout heartbeats, TLS, message annotations on requeue other than `delivery-count`, session window enforcement (rhea's defaults are large enough).

## Service Bus dialect (layer 2)

- `src/servicebus/servicebus.js` `ServiceBus`, one per vhost via `vhost.serviceBus` (lazy) and `server.serviceBus()`. It owns entities (queue, topic as fanout exchange, subscription as `topic/Subscriptions/name` queue bound `#`, dead letter queue `entity/$DeadLetterQueue`), message state under the `kServiceBus` symbol on smqp properties (sequence number, enqueued time, delivery count, dead letter source), locks by lock token string, deferred messages per entity, scheduled messages, and the `$cbs` and `$management` request handling.
- **Dialect switch.** `Amqp10Connection.dialect` becomes `servicebus` when `open.properties.product === 'MSJSClient'` or a link attaches to `$cbs` or a `$management` address. Only then do links resolve through `ServiceBus.resolve`, lock deliveries and read dispositions the Service Bus way. Plain rhea connections keep layer 1 semantics.
- **Lock tokens.** A lock is a random UUID. The delivery tag carries it in .NET GUID byte order (`reorderUuid`), which the SDK reverses. Management requests carry it as a plain uuid. `Link.unsettled` entries keep `lockKey` so a disposition after expiry is answered with `rejected` and `com.microsoft:message-lock-lost`; a successful settlement is always answered with `accepted`, echoing `rejected` makes the SDK reject the promise.
- **Batches.** `sendMessages([...])` arrives as one transfer with message format `0x80013700` whose data sections are whole messages; `ServiceBus.prepare` unpacks them and assigns a message id when missing, because the SDK keys lock renewal by message id.
- **Requests.** `$cbs` and `$management` links have no consumer. A request on the receiving link is answered on the sending link whose name equals the request `reply-to`, with `correlation-id`, `status-code` and `status-description` in application properties (both dashed and camel cased spellings, the SDK reads either depending on the operation) and an `amqp-value` map body. 204 has no body.
- **Locks outlive links.** Closing a receiver does not release its locks (`detachLink` nulls `lock.link`); the SDK settles them through `$management` or they expire in `sweep`, which runs from the vhost sweeper. Lock expiry keeps the link's unsettled entry on purpose.
- **SDK quirks pinned by tests.** Auto lock renewal is on by default and defeats expiry tests, disable it with `maxAutoLockRenewalDurationInMs: 0`. `peekMessages` continues after the last peeked sequence number. Error codes on `ServiceBusError` are `MessageLockLost`, not `MessageLockLostError`. The SDK uses SASL ANONYMOUS, so `connection.user` is `anonymous`.
- **Not implemented.** Sessions, rules and filters, transactions, `$management` session and rule operations, message state annotations beyond dead letter source.

## Deliberate divergences from RabbitMQ

Headers exchanges 540, tx methods are no-ops, no round-robin between consumers on one queue, `basic.recover` with `requeue=false` and `recover-async` are 540, `immediate` publish is 540. See the README for the user facing list. Unknown exchange types are 503 COMMAND_INVALID on the connection like RabbitMQ.

## Tooling notes

- mocha is pinned to `^11` because mocha-cakes-2 breaks on mocha 12.
- `test/tsconfig.json` sets `noImplicitAny: false`; scenario state is untyped `let` by design.
- Heartbeat scenarios mock `setInterval` with `mock.timers` from `node:test` (usable standalone under mocha) and advance time with `mock.timers.tick()`. Only `setInterval` is mocked so mocha's own `setTimeout` based test timeouts keep working. Call `mock.timers.reset()` in the scenario's `after`.
