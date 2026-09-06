import rhea from 'rhea';

import { startServer, once, tick, waitForEvents, until } from '../helpers/index.js';

/**
 * Connect a rhea container to the emulator
 * @param {import('../helpers/index.js').startServer extends (...args: any) => Promise<infer T> ? T : never} ctx
 * @param {Record<string, any>} [options]
 */
function connectRhea(ctx, options = {}) {
  const container = rhea.create_container();
  const connection = container.connect({ host: '127.0.0.1', port: ctx.server.port, reconnect: false, ...options });
  return new Promise((resolve, reject) => {
    connection.once('connection_open', () => resolve(connection));
    connection.once('connection_error', (context) => reject(context.connection.get_error() || new Error('connection error')));
    connection.once('disconnected', (context) => reject(context.error || new Error('disconnected')));
  });
}

/**
 * @param {import('rhea').Connection} connection
 * @param {string} address
 * @param {number} [count]
 * @param {Record<string, any>} [options]
 * @returns {Promise<import('rhea').EventContext[]>}
 */
function receiveMessages(connection, address, count = 1, options = {}) {
  const { keepOpen, ...receiverOptions } = options;
  const contexts = [];
  return new Promise((resolve, reject) => {
    const receiver = connection.open_receiver({ source: { address }, ...receiverOptions });
    receiver.on('message', (context) => {
      contexts.push(context);
      if (contexts.length >= count) {
        if (!keepOpen) receiver.close();
        resolve(contexts);
      }
    });
    receiver.on('receiver_error', (context) => reject(context.receiver.error));
  });
}

Feature('AMQP 1.0', () => {
  Scenario('a rhea client connects on the same port as amqplib clients', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('an emulator', async () => {
      ctx = await startServer();
    });

    When('rhea connects with SASL anonymous', async () => {
      connection = await connectRhea(ctx);
    });

    Then('the connection is open and tracked by the server with its container id', () => {
      expect(connection.is_open()).to.be.true;
      expect(ctx.server.connections.size).to.equal(1);
      const [serverConnection] = ctx.server.connections;
      expect(serverConnection.protocol).to.equal('1.0');
      expect(serverConnection.containerId).to.equal(connection.options.container_id);
      expect(serverConnection.vhost.name).to.equal('/');
    });

    And('the server announced itself in the open frame', () => {
      expect(connection.properties.product).to.equal('amqp-emulator');
    });

    When('the client closes', async () => {
      const closed = once(connection, 'connection_close');
      connection.close();
      await closed;
      await tick();
    });

    Then('the server forgets the connection', () => {
      expect(ctx.server.connections.size).to.equal(0);
    });
  });

  Scenario('SASL plain', () => {
    let ctx;
    after(() => ctx.stop());

    Given('an emulator that only accepts admin', async () => {
      ctx = await startServer({ authenticate: (user, password) => user === 'admin' && password === 's3cret' });
    });

    Then('the right credentials are accepted', async () => {
      const connection = await connectRhea(ctx, { username: 'admin', password: 's3cret' });
      const [serverConnection] = ctx.server.connections;
      expect(serverConnection.user).to.equal('admin');
      connection.close();
    });

    And('the wrong credentials are refused', async () => {
      const err = await connectRhea(ctx, { username: 'admin', password: 'nope' }).catch((e) => e);
      expect(err).to.be.instanceOf(Error);
      expect(err.message).to.match(/sasl|auth/i);
    });
  });

  Scenario('sending and receiving through a queue', () => {
    let ctx, connection, contexts;
    after(() => ctx.stop());

    Given('a connected rhea client', async () => {
      ctx = await startServer();
      connection = await connectRhea(ctx);
    });

    When('a sender attaches to a queue address and sends two messages', async () => {
      const sender = connection.open_sender('orders');
      await once(sender, 'sendable');
      sender.send({ body: 'first', application_properties: { n: 1 }, message_id: 'm1', subject: 'created' });
      sender.send({ body: rhea.message.data_section(Buffer.from('second')), content_type: 'text/plain' });
      await waitForEvents(sender, 'accepted', 2);
      sender.close();
    });

    Then('the queue was created in the broker and holds both messages', () => {
      const queue = ctx.server.getBroker().getQueue('orders');
      expect(queue).to.be.ok;
      expect(queue.messageCount).to.equal(2);
    });

    When('a receiver attaches to the same address', async () => {
      contexts = await receiveMessages(connection, 'orders', 2);
    });

    Then('both messages arrive with their sections intact', () => {
      const [first, second] = contexts;
      expect(first.message.body).to.equal('first');
      expect(first.message.application_properties).to.deep.equal({ n: 1 });
      expect(first.message.message_id).to.equal('m1');
      expect(first.message.subject).to.equal('created');
      expect(second.message.body.content.toString()).to.equal('second');
      expect(second.message.content_type).to.equal('text/plain');
    });

    And('auto accepted deliveries are settled in the broker', async () => {
      const queue = ctx.server.getBroker().getQueue('orders');
      await until(() => queue.messageCount === 0);
      expect(queue.messageCount).to.equal(0);
    });
  });

  Scenario('explicit dispositions', () => {
    let ctx, connection, queue;
    after(() => ctx.stop());

    Given('a queue with three messages and a rhea client', async () => {
      ctx = await startServer();
      const broker = ctx.server.getBroker();
      broker.assertQueue('settle', { durable: false, autoDelete: false });
      for (const n of [1, 2, 3]) broker.sendToQueue('settle', Buffer.from(`${n}`));
      queue = broker.getQueue('settle');
      connection = await connectRhea(ctx);
    });

    When('a receiver without auto accept releases the first, rejects the second and accepts the third', async () => {
      const contexts = await receiveMessages(connection, 'settle', 3, { autoaccept: false, keepOpen: true });
      // rhea batches adjacent settlements issued in the same tick into one disposition, so settle one at a time
      contexts[0].delivery.release();
      await tick();
      contexts[1].delivery.reject();
      await tick();
      contexts[2].delivery.accept();
      await tick();
    });

    Then('the released message was redelivered to the receiver and the others are gone', () => {
      expect(queue.messageCount).to.equal(1);
      expect(queue.messages[0].pending).to.be.true;
      expect(queue.messages[0].content.toString()).to.equal('1');
      expect(queue.messages[0].fields.redelivered).to.be.true;
    });

    When('the receiver detaches', async () => {
      connection.each_receiver((receiver) => receiver.close());
      await tick();
    });

    Then('the released message is ready again', () => {
      expect(queue.messageCount).to.equal(1);
      expect(queue.messages[0].pending).to.be.false;
    });
  });

  Scenario('credit based flow control', () => {
    let ctx, connection, receiver, received;
    after(() => ctx.stop());

    Given('a queue with five messages and a receiver without a credit window', async () => {
      ctx = await startServer();
      const broker = ctx.server.getBroker();
      broker.assertQueue('credit', { durable: false, autoDelete: false });
      for (const n of [1, 2, 3, 4, 5]) broker.sendToQueue('credit', Buffer.from(`${n}`));
      connection = await connectRhea(ctx);
      received = [];
      receiver = connection.open_receiver({ source: { address: 'credit' }, credit_window: 0, autoaccept: false });
      receiver.on('message', (context) => {
        received.push(context.message.body.content.toString());
        context.delivery.accept();
      });
      await once(receiver, 'receiver_open');
      await tick(20);
    });

    Then('nothing is delivered without credit', () => {
      expect(received).to.deep.equal([]);
    });

    When('the client grants two credits', async () => {
      receiver.add_credit(2);
      await tick(20);
    });

    Then('exactly two messages are delivered', () => {
      expect(received).to.deep.equal(['1', '2']);
    });

    When('the client grants three more credits', async () => {
      receiver.add_credit(3);
      await tick(20);
    });

    Then('the remaining messages arrive and the queue is drained', () => {
      expect(received).to.deep.equal(['1', '2', '3', '4', '5']);
      expect(ctx.server.getBroker().getQueue('credit').messageCount).to.equal(0);
    });
  });

  Scenario('interoperability with amqplib', () => {
    let ctx, rheaConnection, channel;
    after(() => ctx.stop());

    Given('an amqplib channel and a rhea connection on the same emulator', async () => {
      ctx = await startServer();
      const amqp = await ctx.connect();
      channel = await amqp.createChannel();
      await channel.assertQueue('bridge');
      rheaConnection = await connectRhea(ctx);
    });

    Then('what rhea sends amqplib receives with properties and headers mapped', async () => {
      const sender = rheaConnection.open_sender('bridge');
      await once(sender, 'sendable');
      sender.send({
        body: rhea.message.data_section(Buffer.from('from 1.0')),
        content_type: 'application/json',
        message_id: 'x-1',
        correlation_id: 'c-1',
        application_properties: { tenant: 'acme' },
        durable: true,
      });
      const message = await new Promise((resolve) => {
        const poll = async () => {
          const m = await channel.get('bridge', { noAck: true });
          if (m) resolve(m);
          else setTimeout(poll, 5);
        };
        poll();
      });
      expect(message.content.toString()).to.equal('from 1.0');
      expect(message.properties).to.deep.include({
        contentType: 'application/json',
        messageId: 'x-1',
        correlationId: 'c-1',
        deliveryMode: 2,
      });
      expect(message.properties.headers).to.deep.equal({ tenant: 'acme' });
      sender.close();
    });

    And('what amqplib publishes rhea receives', async () => {
      channel.sendToQueue('bridge', Buffer.from('from 0-9-1'), { contentType: 'text/plain', messageId: 'y-1', headers: { hop: 2 } });
      const [context] = await receiveMessages(rheaConnection, 'bridge');
      expect(context.message.body.content.toString()).to.equal('from 0-9-1');
      expect(context.message.content_type).to.equal('text/plain');
      expect(context.message.message_id).to.equal('y-1');
      expect(context.message.application_properties).to.deep.equal({ hop: 2 });
    });
  });

  Scenario('exchange addresses', () => {
    let ctx, connection, channel;
    after(() => ctx.stop());

    Given('a topic exchange with a bound queue declared by amqplib', async () => {
      ctx = await startServer();
      const amqp = await ctx.connect();
      channel = await amqp.createChannel();
      await channel.assertExchange('events', 'topic');
      await channel.assertQueue('audit');
      await channel.bindQueue('audit', 'events', 'order.*');
      connection = await connectRhea(ctx);
    });

    Then('a sender on /exchanges/events/order.created routes into the bound queue', async () => {
      const sender = connection.open_sender('/exchanges/events/order.created');
      await once(sender, 'sendable');
      const accepted = once(sender, 'accepted');
      sender.send({ body: 'routed' });
      await accepted;
      const message = await channel.get('audit', { noAck: true });
      expect(message.content.toString()).to.equal('routed');
      expect(message.fields.routingKey).to.equal('order.created');
    });

    And('a receiver on /exchanges/events/order.# gets its own bound queue', async () => {
      const receiving = receiveMessages(connection, '/exchanges/events/order.#');
      await tick(20);
      channel.publish('events', 'order.paid', Buffer.from('paid'));
      const [context] = await receiving;
      expect(context.message.body.content.toString()).to.equal('paid');
    });

    And('a sender to an unknown exchange is detached with amqp:not-found', async () => {
      const sender = connection.open_sender('/exchanges/missing/key');
      const context = await once(sender, 'sender_error');
      expect(context.sender.error.condition).to.equal('amqp:not-found');
    });
  });

  Scenario('detaching a receiver requeues unsettled messages', () => {
    let ctx, connection, queue;
    after(() => ctx.stop());

    Given('a queue with a message held unsettled by a rhea receiver', async () => {
      ctx = await startServer();
      const broker = ctx.server.getBroker();
      broker.assertQueue('held', { durable: false, autoDelete: false });
      broker.sendToQueue('held', Buffer.from('hold me'));
      queue = broker.getQueue('held');
      connection = await connectRhea(ctx);
      const [context] = await receiveMessages(connection, 'held', 1, { autoaccept: false });
      expect(context.message.body.content.toString()).to.equal('hold me');
      expect(queue.messages[0].pending).to.be.true;
      await tick();
    });

    Then('the message is ready again after the receiver closed', () => {
      expect(queue.messageCount).to.equal(1);
      expect(queue.messages[0].pending).to.be.false;
    });

    And('closing the connection with a receiver still attached requeues too', async () => {
      const receiver = connection.open_receiver({ source: { address: 'held' }, autoaccept: false });
      await once(receiver, 'message');
      expect(queue.messages[0].pending).to.be.true;
      connection.close();
      await tick(20);
      expect(queue.messages[0].pending).to.be.false;
      expect(ctx.server.connections.size).to.equal(0);
    });
  });

  Scenario('message sections beyond the basics', () => {
    let ctx, connection, context;
    after(() => ctx.stop());

    Given('a rhea client that sends a message with annotations, footer, a sequence body and every property', async () => {
      ctx = await startServer();
      connection = await connectRhea(ctx);
      const sender = connection.open_sender('rich');
      await once(sender, 'sendable');
      sender.send({
        body: rhea.message.sequence_section([1, 'two']),
        delivery_annotations: { 'x-hop': 1 },
        message_annotations: { 'x-opt-key': 'v' },
        footer: { checksum: 'abc' },
        content_encoding: 'gzip',
        reply_to: 'replies',
        user_id: Buffer.from('user'),
        creation_time: new Date(1700000000000),
        absolute_expiry_time: new Date(Date.now() + 60000),
        priority: 7,
      });
      await once(sender, 'accepted');
      [context] = await receiveMessages(connection, 'rich');
    });

    Then('a 1.0 receiver gets the message annotations, footer and sequence body back but not the delivery annotations', () => {
      const message = context.message;
      expect(message.body.content).to.deep.equal([1, 'two']);
      expect(message.message_annotations).to.deep.equal({ 'x-opt-key': 'v' });
      expect(message.footer).to.deep.equal({ checksum: 'abc' });
      expect(message.delivery_annotations).to.be.undefined;
      expect(message.content_encoding).to.equal('gzip');
      expect(message.reply_to).to.equal('replies');
      expect(message.priority).to.equal(7);
    });

    And('an amqp-value body that is not a string is delivered to 0-9-1 as json', async () => {
      const amqp = await ctx.connect();
      const channel = await amqp.createChannel();
      const sender = connection.open_sender('rich');
      await once(sender, 'sendable');
      sender.send({ body: { nested: [1, 2] }, ttl: 60000 });
      sender.send({ body: rhea.message.sequence_section(['a']) });
      await tick(20);
      const first = await channel.get('rich', { noAck: true });
      expect(JSON.parse(first.content.toString())).to.deep.equal({ nested: [1, 2] });
      expect(first.properties.expiration).to.equal('60000');
      const second = await channel.get('rich', { noAck: true });
      expect(JSON.parse(second.content.toString())).to.deep.equal([['a']]);
    });

    And('a message published by amqplib with every property maps onto 1.0 sections', async () => {
      const amqp = await ctx.connect();
      const channel = await amqp.createChannel();
      channel.sendToQueue('rich', Buffer.from('full'), {
        contentType: 'text/plain',
        contentEncoding: 'utf8',
        messageId: 'm',
        correlationId: 'c',
        replyTo: 'r',
        type: 'subject',
        userId: 'guest',
        timestamp: 1700000000,
        expiration: '5000',
        deliveryMode: 2,
        priority: 3,
        headers: { h: 1 },
      });
      const [ctx2] = await receiveMessages(connection, 'rich');
      const m = ctx2.message;
      expect(m.body.content.toString()).to.equal('full');
      expect(m).to.include({
        content_type: 'text/plain',
        content_encoding: 'utf8',
        message_id: 'm',
        correlation_id: 'c',
        reply_to: 'r',
        subject: 'subject',
      });
      expect(m.user_id.toString()).to.equal('guest');
      expect(m.creation_time.getTime()).to.equal(1700000000000);
      expect(m.ttl).to.equal(5000);
      expect(m.durable).to.be.true;
      expect(m.priority).to.equal(3);
      expect(m.application_properties).to.deep.equal({ h: 1 });
    });

    And('seeded object and string bodies and redelivered messages carry a delivery count', async () => {
      const broker = ctx.server.getBroker();
      broker.assertQueue('seeded10', { durable: false, autoDelete: false });
      broker.sendToQueue('seeded10', { seeded: true });
      broker.sendToQueue('seeded10', 'plain string');
      broker.sendToQueue('seeded10');
      const [first, second, third] = await receiveMessages(connection, 'seeded10', 3, { autoaccept: false, keepOpen: true });
      expect(JSON.parse(first.message.body.content.toString())).to.deep.equal({ seeded: true });
      expect(second.message.body.content.toString()).to.equal('plain string');
      expect(third.message.body.content.toString()).to.equal('""');
      first.delivery.release();
      await tick();
      second.delivery.accept();
      await tick();
      third.delivery.accept();
      await tick();
      first.receiver.close();
      await tick();
      const [again] = await receiveMessages(connection, 'seeded10');
      expect(again.message.delivery_count).to.equal(1);
    });

    And('a released 1.0 message keeps its sections and gains a delivery count', async () => {
      const sender = connection.open_sender('released10');
      await once(sender, 'sendable');
      sender.send({ body: 'again', durable: true, message_annotations: { 'x-a': 1 } });
      await once(sender, 'accepted');
      const [first] = await receiveMessages(connection, 'released10', 1, { autoaccept: false, keepOpen: true });
      first.delivery.release();
      await tick();
      first.receiver.close();
      await tick();
      const [again] = await receiveMessages(connection, 'released10');
      expect(again.message.body).to.equal('again');
      expect(again.message.delivery_count).to.equal(1);
      expect(again.message.durable).to.be.true;
      expect(again.message.message_annotations).to.deep.equal({ 'x-a': 1 });
    });

    And('a receiver asking for pre-settled deliveries gets them settled', async () => {
      const broker = ctx.server.getBroker();
      broker.assertQueue('presettled', { durable: false, autoDelete: false });
      broker.sendToQueue('presettled', Buffer.from('fire and forget'));
      const [context] = await receiveMessages(connection, 'presettled', 1, { snd_settle_mode: 1, autoaccept: false });
      expect(context.delivery.remote_settled).to.be.true;
      expect(broker.getQueue('presettled').messageCount).to.equal(0);
    });
  });

  Scenario('large messages, dynamic receivers, drain and modified dispositions', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connected rhea client', async () => {
      ctx = await startServer({ frameMax: 4096 });
      connection = await connectRhea(ctx, { max_frame_size: 4096 });
    });

    Then('a message larger than the frame size is transferred in several frames both ways', async () => {
      const big = Buffer.alloc(20000, 'z');
      const sender = connection.open_sender('big');
      await once(sender, 'sendable');
      sender.send({ body: rhea.message.data_section(big) });
      await once(sender, 'accepted');
      const [context] = await receiveMessages(connection, 'big');
      expect(context.message.body.content.length).to.equal(20000);
    });

    And('a dynamic receiver gets a server named queue', async () => {
      const receiver = connection.open_receiver({ source: { dynamic: true } });
      const context = await once(receiver, 'receiver_open');
      const address = context.receiver.source.address;
      expect(address).to.match(/^amq\.gen-/);
      expect(ctx.server.getBroker().getQueue(address)).to.be.ok;
      const sender = connection.open_sender(address);
      await once(sender, 'sendable');
      sender.send({ body: 'dynamic' });
      const message = await once(receiver, 'message');
      expect(message.message.body).to.equal('dynamic');
      receiver.close();
      await tick();
      expect(ctx.server.getBroker().getQueue(address)).to.be.undefined;
    });

    And('drain settles outstanding credit', async () => {
      const receiver = connection.open_receiver({ source: { address: 'drained' }, credit_window: 0 });
      await once(receiver, 'receiver_open');
      const drained = once(receiver, 'receiver_drained');
      receiver.drain = true;
      receiver.add_credit(5);
      await drained;
      receiver.close();
    });

    And('a modified disposition with undeliverable-here drops the message', async () => {
      const broker = ctx.server.getBroker();
      broker.assertQueue('modified', { durable: false, autoDelete: false });
      broker.sendToQueue('modified', Buffer.from('1'));
      broker.sendToQueue('modified', Buffer.from('2'));
      const [first, second] = await receiveMessages(connection, 'modified', 2, { autoaccept: false, keepOpen: true });
      first.delivery.modified({ undeliverable_here: true });
      await tick();
      second.delivery.modified({ delivery_failed: true });
      await tick();
      const queue = broker.getQueue('modified');
      expect(queue.messages.map((m) => m.content.toString())).to.deep.equal(['2']);
      first.receiver.close();
    });
  });

  Scenario('links to a deleted queue and to a vanished exchange', () => {
    let ctx, connection, channel;
    after(() => ctx.stop());

    Given('a rhea receiver on a queue and a sender to an exchange', async () => {
      ctx = await startServer();
      const amqp = await ctx.connect();
      channel = await amqp.createChannel();
      await channel.assertQueue('doomed10');
      await channel.assertExchange('gone', 'topic');
      connection = await connectRhea(ctx);
    });

    Then('deleting the queue detaches the receiver with amqp:resource-deleted', async () => {
      const receiver = connection.open_receiver({ source: { address: 'doomed10' } });
      await once(receiver, 'receiver_open');
      const errored = once(receiver, 'receiver_error');
      await channel.deleteQueue('doomed10');
      const context = await errored;
      expect(context.receiver.error.condition).to.equal('amqp:resource-deleted');
    });

    And('a transfer to an exchange deleted after attach is rejected', async () => {
      const sender = connection.open_sender('/exchanges/gone/key');
      await once(sender, 'sendable');
      await channel.deleteExchange('gone');
      const rejected = once(sender, 'rejected');
      sender.send({ body: 'nowhere' });
      const context = await rejected;
      const outcome = /** @type {any} */ (rhea.message).unwrap_outcome(context.delivery.remote_state);
      expect(outcome).to.have.nested.property('error.condition', 'amqp:not-found');
    });

    And('a receiver on an unknown exchange is refused', async () => {
      const receiver = connection.open_receiver({ source: { address: '/exchanges/nope/x' } });
      const context = await once(receiver, 'receiver_error');
      expect(context.receiver.error.condition).to.equal('amqp:not-found');
    });

    And('ending a session tears its links down', async () => {
      const broker = ctx.server.getBroker();
      broker.assertQueue('sessioned', { durable: false, autoDelete: false });
      broker.sendToQueue('sessioned', Buffer.from('held'));
      const session = connection.create_session();
      session.begin();
      const receiver = session.open_receiver({ source: { address: 'sessioned' }, autoaccept: false });
      await once(receiver, 'message');
      expect(broker.getQueue('sessioned').messages[0].pending).to.be.true;
      const ended = once(session, 'session_close');
      session.close();
      await ended;
      expect(broker.getQueue('sessioned').messages[0].pending).to.be.false;
    });
  });

  Scenario('server initiated disconnect', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connected rhea client', async () => {
      ctx = await startServer();
      connection = await connectRhea(ctx);
    });

    Then('server.disconnect() closes it with amqp:connection:forced', async () => {
      const closed = once(connection, 'connection_close');
      ctx.server.disconnect();
      const context = await closed;
      expect(context.connection.get_error()).to.have.property('condition', 'amqp:connection:forced');
    });
  });
});
