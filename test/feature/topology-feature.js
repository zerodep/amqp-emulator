import rhea from 'rhea';
import { ServiceBusClient } from '@azure/service-bus';

import { AmqpServer } from 'amqp-emulator';

import { startServer, consumeMessages, once, tick } from '../helpers/index.js';

/**
 * @param {import('../helpers/index.js').startServer extends (...args: any) => Promise<infer T> ? T : never} ctx
 */
function connectRhea(ctx) {
  const container = rhea.create_container();
  const connection = container.connect({ host: '127.0.0.1', port: ctx.server.port, reconnect: false });
  return new Promise((resolve, reject) => {
    connection.once('connection_open', () => resolve(connection));
    connection.once('disconnected', (context) => reject(context.error || new Error('disconnected')));
  });
}

/**
 * @param {import('../helpers/index.js').startServer extends (...args: any) => Promise<infer T> ? T : never} ctx
 */
function connectionString(ctx) {
  return `Endpoint=sb://127.0.0.1:${ctx.server.port}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true`;
}

Feature('Topology', () => {
  Scenario('resources provisioned outside the application are given as definitions', () => {
    let ctx, channel, broker;
    after(() => ctx.stop());

    Given('an emulator started with RabbitMQ style definitions', async () => {
      ctx = await startServer({
        definitions: {
          vhosts: [{ name: '/' }, { name: 'tenant' }],
          exchanges: [
            { name: 'events', vhost: '/', type: 'topic', durable: true, auto_delete: false, internal: false, arguments: {} },
            { name: 'archive', type: 'fanout' },
          ],
          queues: [
            {
              name: 'audit',
              vhost: '/',
              durable: true,
              auto_delete: false,
              arguments: { 'x-message-ttl': 60000, 'x-dead-letter-exchange': 'archive' },
            },
            { name: 'archive-q', durable: true, autoDelete: false },
            { name: 'scratch', auto_delete: true },
            { name: 'tenant-q', vhost: 'tenant' },
          ],
          bindings: [
            { source: 'events', vhost: '/', destination: 'audit', destination_type: 'queue', routing_key: 'order.*', arguments: {} },
            { source: 'events', destination: 'archive', destinationType: 'exchange', routingKey: 'order.#' },
            { source: 'archive', destination: 'archive-q' },
          ],
        },
      });
      broker = ctx.server.getBroker();
    });

    Then('the resources exist with the declared options', () => {
      expect(broker.getExchange('events')).to.have.property('type', 'topic');
      expect(broker.getExchange('archive')).to.have.property('type', 'fanout');
      expect(broker.getQueue('audit').options).to.include({
        durable: true,
        autoDelete: false,
        messageTtl: 60000,
        deadLetterExchange: 'archive',
      });
      expect(broker.getQueue('archive-q').options).to.include({ durable: true, autoDelete: false });
      expect(broker.getQueue('scratch').options).to.include({ autoDelete: true });
      expect(ctx.server.getBroker('tenant').getQueue('tenant-q')).to.be.ok;
    });

    And('an amqplib client that declares nothing can check and use them', async () => {
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.checkExchange('events');
      await channel.checkQueue('audit');
      await channel.assertQueue('audit', { durable: true, arguments: { 'x-message-ttl': 60000, 'x-dead-letter-exchange': 'archive' } });
      channel.publish('events', 'order.created', Buffer.from('routed'));
      const message = await channel.get('audit', { noAck: true });
      expect(message.content.toString()).to.equal('routed');
      const [archived] = await consumeMessages(channel, 'archive-q', 1, { noAck: true });
      expect(archived.content.toString()).to.equal('routed');
    });

    And('the tenant vhost got its queue', async () => {
      const connection = await ctx.connect(`${ctx.url}/tenant`);
      const ch = await connection.createChannel();
      await ch.checkQueue('tenant-q');
    });

    When('the emulator is reset', () => {
      broker.sendToQueue('audit', 'leftover');
      ctx.server.reset();
    });

    Then('the definitions are applied again, like infrastructure that outlives the broker state', () => {
      expect(broker.getQueue('audit').messageCount).to.equal(0);
      expect(broker.getExchange('events')).to.have.property('type', 'topic');
      expect(broker.getQueue('audit').options).to.include({ messageTtl: 60000 });
      expect(ctx.server.getBroker('tenant').getQueue('tenant-q')).to.be.ok;
    });

    And('definitions loaded later are replayed on reset too', () => {
      ctx.server.load({ queues: [{ name: 'late' }] });
      expect(broker.getQueue('late')).to.be.ok;
      ctx.server.reset();
      expect(broker.getQueue('late')).to.be.ok;
      expect(broker.getQueue('audit')).to.be.ok;
    });

    And('definitions for a vhost outside the allowed list are refused', () => {
      expect(() => new AmqpServer({ vhosts: ['/'], definitions: { queues: [{ name: 'q', vhost: 'other' }] } })).to.throw(/vhost 'other'/);
    });
  });

  Scenario('Service Bus topics and subscriptions are given as definitions', function () {
    this.timeout(10000);
    let ctx, client;
    after(async () => {
      await client.close();
      await ctx.stop();
    });

    Given('an emulator with a topic, two subscriptions and a queue defined', async () => {
      ctx = await startServer({
        definitions: {
          queues: [{ name: 'orders' }],
          topics: [{ name: 'events', vhost: '/', subscriptions: ['audit', 'billing'] }, { name: 'bare-topic' }],
        },
      });
      client = new ServiceBusClient(connectionString(ctx));
    });

    Then('a message sent to the topic before any receiver attaches reaches both subscriptions', async () => {
      const sender = client.createSender('events');
      await sender.sendMessages({ body: 'fan out' });
      for (const subscription of ['audit', 'billing']) {
        const receiver = client.createReceiver('events', subscription, { receiveMode: 'receiveAndDelete' });
        const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
        expect(message.body).to.equal('fan out');
        await receiver.close();
      }
      await sender.close();
    });

    And('no queue was created with the topic name', () => {
      const broker = ctx.server.getBroker();
      expect(broker.getQueue('events')).to.be.undefined;
      expect(broker.getQueue('orders')).to.be.ok;
    });

    And('a topic without subscriptions is just an exchange', () => {
      expect(ctx.server.getVHost().getExchangeType('bare-topic')).to.equal('fanout');
      expect(ctx.server.getBroker().getQueue('bare-topic')).to.be.undefined;
    });
  });

  Scenario('missing resources are created with a default setup when autoCreate is on', () => {
    let ctx, channel, broker;
    after(() => ctx.stop());

    Given('an emulator with autoCreate and an amqplib channel', async () => {
      ctx = await startServer({ autoCreate: true });
      broker = ctx.server.getBroker();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
    });

    Then('a passive queue declare creates a durable queue', async () => {
      const ok = await channel.checkQueue('never-declared');
      expect(ok).to.include({ queue: 'never-declared', messageCount: 0 });
      expect(broker.getQueue('never-declared').options).to.include({ durable: true, autoDelete: false });
    });

    And('a passive exchange declare creates a topic exchange', async () => {
      await channel.checkExchange('never-declared-x');
      expect(broker.getExchange('never-declared-x')).to.include({ type: 'topic' });
      expect(broker.getExchange('never-declared-x').options).to.include({ durable: true, autoDelete: false });
    });

    And('publishing to an unknown exchange creates it and routes once something is bound', async () => {
      channel.publish('orders', 'created', Buffer.from('lost'));
      await channel.checkExchange('orders');
      await channel.bindQueue('order-q', 'orders', 'created');
      channel.publish('orders', 'created', Buffer.from('found'));
      const message = await channel.get('order-q', { noAck: true });
      expect(message.content.toString()).to.equal('found');
    });

    And('consuming from an unknown queue creates it', async () => {
      const consuming = consumeMessages(channel, 'inbox', 1, { noAck: true });
      await tick(20);
      expect(broker.getQueue('inbox')).to.be.ok;
      channel.sendToQueue('inbox', Buffer.from('hi'));
      const [message] = await consuming;
      expect(message.content.toString()).to.equal('hi');
    });

    And('binding unknown exchanges to each other creates both', async () => {
      await channel.bindExchange('dest-x', 'src-x', 'k');
      expect(broker.getExchange('src-x')).to.be.ok;
      expect(broker.getExchange('dest-x')).to.be.ok;
    });

    And('a headers exchange is still refused', async () => {
      const connection = await ctx.connect();
      const ch = await connection.createChannel();
      ch.on('error', () => {});
      const err = await ch.assertExchange('h', 'headers').catch((e) => e);
      expect(err).to.have.property('code', 540);
    });
  });

  Scenario('the exchange type used by autoCreate is configurable', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('an emulator that auto creates fanout exchanges', async () => {
      ctx = await startServer({ autoCreate: { exchangeType: 'fanout' } });
      const connection = await ctx.connect();
      channel = await connection.createChannel();
    });

    Then('publishing to an unknown exchange creates a fanout exchange', async () => {
      channel.publish('broadcast', 'ignored-key', Buffer.from('all'));
      await channel.checkExchange('broadcast');
      expect(ctx.server.getVHost().getExchangeType('broadcast')).to.equal('fanout');
    });
  });

  Scenario('AMQP 1.0 exchange addresses are created when autoCreate is on', () => {
    let ctx, connection, channel;
    after(() => ctx.stop());

    Given('an emulator with autoCreate, a rhea connection and an amqplib channel', async () => {
      ctx = await startServer({ autoCreate: true });
      connection = await connectRhea(ctx);
      const amqp = await ctx.connect();
      channel = await amqp.createChannel();
    });

    Then('a sender on an unknown exchange address attaches and the exchange exists', async () => {
      const sender = connection.open_sender('/exchanges/events/order.created');
      await once(sender, 'sendable');
      expect(ctx.server.getVHost().getExchangeType('events')).to.equal('topic');
      await channel.bindQueue('audit', 'events', 'order.*');
      const accepted = once(sender, 'accepted');
      sender.send({ body: 'routed' });
      await accepted;
      const message = await channel.get('audit', { noAck: true });
      expect(message.content.toString()).to.equal('routed');
    });

    And('a receiver on an unknown exchange address gets a bound queue on a fresh exchange', async () => {
      const receiver = connection.open_receiver({ source: { address: '/exchanges/alerts/#' } });
      const received = once(receiver, 'message');
      await once(receiver, 'receiver_open');
      expect(ctx.server.getVHost().getExchangeType('alerts')).to.equal('topic');
      channel.publish('alerts', 'disk.full', Buffer.from('beep'));
      const context = await received;
      expect(context.message.body.content.toString()).to.equal('beep');
    });

    And('a transfer to an exchange deleted after attach recreates it', async () => {
      const sender = connection.open_sender('/exchanges/gone/key');
      await once(sender, 'sendable');
      await channel.deleteExchange('gone');
      const accepted = once(sender, 'accepted');
      sender.send({ body: 'again' });
      await accepted;
      expect(ctx.server.getVHost().getExchangeType('gone')).to.equal('topic');
    });
  });
});
