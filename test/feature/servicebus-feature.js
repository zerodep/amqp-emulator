import { mock } from 'node:test';

import { ServiceBusClient } from '@azure/service-bus';

import { startServer } from '../helpers/index.js';

/**
 * @param {import('../helpers/index.js').startServer extends (...args: any) => Promise<infer T> ? T : never} ctx
 */
function connectionString(ctx) {
  return `Endpoint=sb://127.0.0.1:${ctx.server.port}/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true`;
}

Feature('Azure Service Bus SDK', function () {
  // the SDK does real round trips with retries, give every step room
  this.timeout(10000);

  Scenario('sending and receiving with peek lock', () => {
    let ctx, client, sender, receiver, message;
    after(async () => {
      await client.close();
      await ctx.stop();
    });

    Given('a Service Bus client pointed at the emulator', async () => {
      ctx = await startServer();
      client = new ServiceBusClient(connectionString(ctx));
      sender = client.createSender('orders');
      receiver = client.createReceiver('orders');
    });

    When('a message is sent', async () => {
      await sender.sendMessages({
        body: { id: 1 },
        applicationProperties: { tenant: 'acme' },
        messageId: 'm1',
        subject: 'created',
        contentType: 'application/json',
        correlationId: 'c1',
        timeToLive: 60000,
      });
    });

    Then('the connection authenticated through $cbs and the queue holds the message', () => {
      const [connection] = ctx.server.connections;
      expect(connection.protocol).to.equal('1.0');
      expect(connection.dialect).to.equal('servicebus');
      expect(connection.user).to.equal('anonymous');
      expect(ctx.server.getBroker().getQueue('orders').messageCount).to.equal(1);
    });

    When('the message is received', async () => {
      [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
    });

    Then('it carries the Service Bus properties', () => {
      expect(message.body).to.deep.equal({ id: 1 });
      expect(message.applicationProperties).to.deep.equal({ tenant: 'acme' });
      expect(message).to.include({ messageId: 'm1', subject: 'created', contentType: 'application/json', correlationId: 'c1' });
      expect(message.deliveryCount).to.equal(0);
      expect(message.sequenceNumber.toNumber()).to.be.above(0);
      expect(message.enqueuedTimeUtc).to.be.instanceOf(Date);
      expect(message.lockedUntilUtc.getTime()).to.be.above(Date.now());
      expect(message.lockToken).to.match(/^[0-9a-f-]{36}$/);
      expect(message.timeToLive).to.equal(60000);
    });

    And('the lock is known to the emulator', () => {
      const serviceBus = ctx.server.serviceBus();
      expect(serviceBus.locks.has(message.lockToken)).to.be.true;
    });

    When('it is completed', async () => {
      await receiver.completeMessage(message);
    });

    Then('the queue is empty and the lock released', () => {
      expect(ctx.server.getBroker().getQueue('orders').messageCount).to.equal(0);
      expect(ctx.server.serviceBus().locks.size).to.equal(0);
    });
  });

  Scenario('abandon, defer and dead letter', () => {
    let ctx, client, sender, receiver, message;
    after(async () => {
      await client.close();
      await ctx.stop();
    });

    Given('a queue with a message and a peek lock receiver', async () => {
      ctx = await startServer();
      client = new ServiceBusClient(connectionString(ctx));
      sender = client.createSender('work');
      receiver = client.createReceiver('work');
      await sender.sendMessages({ body: 'job' });
    });

    When('the message is received and abandoned', async () => {
      [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      await receiver.abandonMessage(message);
    });

    Then('it is redelivered with an incremented delivery count', async () => {
      [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      expect(message.body).to.equal('job');
      expect(message.deliveryCount).to.equal(1);
    });

    When('it is deferred', async () => {
      await receiver.deferMessage(message);
    });

    Then('it is no longer delivered but can be received by sequence number and completed', async () => {
      const none = await receiver.receiveMessages(1, { maxWaitTimeInMs: 300 });
      expect(none).to.have.length(0);
      const [deferred] = await receiver.receiveDeferredMessages([message.sequenceNumber]);
      expect(deferred.body).to.equal('job');
      expect(deferred.sequenceNumber.equals(message.sequenceNumber)).to.be.true;
      await receiver.completeMessage(deferred);
      expect(ctx.server.getBroker().getQueue('work').messageCount).to.equal(0);
    });

    When('another message is dead lettered', async () => {
      await sender.sendMessages({ body: 'bad', messageId: 'dl-1' });
      [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      await receiver.deadLetterMessage(message, { deadLetterReason: 'Broken', deadLetterErrorDescription: 'cannot process' });
    });

    Then('it shows up in the dead letter sub queue with the reason', async () => {
      const dlq = client.createReceiver('work', { subQueueType: 'deadLetter' });
      const [dead] = await dlq.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      expect(dead.body).to.equal('bad');
      expect(dead.messageId).to.equal('dl-1');
      expect(dead.deadLetterReason).to.equal('Broken');
      expect(dead.deadLetterErrorDescription).to.equal('cannot process');
      expect(dead.deadLetterSource).to.equal('work');
      await dlq.completeMessage(dead);
      await dlq.close();
      expect(ctx.server.getBroker().getQueue('work/$DeadLetterQueue').messageCount).to.equal(0);
    });
  });

  Scenario('peeking, scheduling and lock renewal', () => {
    let ctx, client, sender, receiver;
    after(async () => {
      await client.close();
      await ctx.stop();
      mock.timers.reset();
    });

    Given('a queue with two messages', async () => {
      // Lock expiry and scheduled enqueue are driven by the vhost sweeper (setInterval)
      // reading Date.now(); mock both so time can be advanced instantly instead of waited.
      // setTimeout stays real so the SDK's maxWaitTimeInMs receive loops still resolve.
      mock.timers.enable({ apis: ['Date', 'setInterval'] });
      ctx = await startServer({ lockDuration: 400 });
      client = new ServiceBusClient(connectionString(ctx));
      sender = client.createSender('peekable');
      // the SDK renews locks in the background by default, which would defeat the expiry step
      receiver = client.createReceiver('peekable', { maxAutoLockRenewalDurationInMs: 0 });
      await sender.sendMessages([{ body: 'one' }, { body: 'two' }]);
    });

    Then('peeking returns them without locking', async () => {
      const peeked = await receiver.peekMessages(5);
      expect(peeked.map((m) => m.body)).to.deep.equal(['one', 'two']);
      expect(peeked[0].sequenceNumber.toNumber()).to.be.below(peeked[1].sequenceNumber.toNumber());
      expect(ctx.server.serviceBus().locks.size).to.equal(0);
      const again = await receiver.peekMessages(1, { fromSequenceNumber: peeked[1].sequenceNumber });
      expect(again.map((m) => m.body)).to.deep.equal(['two']);
    });

    And('peeking an empty queue returns nothing', async () => {
      const empty = client.createReceiver('nothing-here');
      expect(await empty.peekMessages(1)).to.deep.equal([]);
      await empty.close();
    });

    And('scheduled messages are enqueued at their time and can be cancelled before', async () => {
      const [soon, later] = await sender.scheduleMessages([{ body: 'soon' }, { body: 'later' }], new Date(Date.now() + 300));
      expect(soon.toNumber()).to.be.above(0);
      await sender.cancelScheduledMessages([later]);
      expect(ctx.server.getBroker().getQueue('peekable').messageCount).to.equal(2);
      mock.timers.tick(500);
      // the SDK continues peeking after the last peeked sequence number, so only the new message shows up
      const peeked = await receiver.peekMessages(5);
      expect(peeked.map((m) => m.body)).to.deep.equal(['soon']);
    });

    And('a lock can be renewed', async () => {
      const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      const before = message.lockedUntilUtc.getTime();
      mock.timers.tick(50);
      const renewed = await receiver.renewMessageLock(message);
      expect(renewed.getTime()).to.be.above(before);
      await receiver.completeMessage(message);
    });

    And('an expired lock puts the message back with a higher delivery count', async () => {
      const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      expect(message.body).to.equal('two');
      mock.timers.tick(600);
      const err = await receiver.completeMessage(message).catch((e) => e);
      expect(err).to.have.property('code', 'MessageLockLost');
      const [again] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      expect(again.body).to.equal('two');
      expect(again.deliveryCount).to.equal(1);
      const renewErr = await receiver.renewMessageLock(message).catch((e) => e);
      expect(renewErr).to.have.property('code', 'MessageLockLost');
      await receiver.completeMessage(again);
    });

    And('a message seeded through the broker api can be received and dead lettered', async () => {
      const broker = ctx.server.getBroker();
      ctx.server.serviceBus().createQueue('seeded-q');
      broker.sendToQueue('seeded-q', Buffer.from('seeded'), { contentType: 'text/plain', headers: { origin: 'amqp 0-9-1' } });
      // a broker seeded message has no message id, which the SDK's auto lock renewal keys on, so disable it here
      const seededReceiver = client.createReceiver('seeded-q', { maxAutoLockRenewalDurationInMs: 0 });
      const [seeded] = await seededReceiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      expect(seeded.body.toString()).to.equal('seeded');
      expect(seeded.contentType).to.equal('text/plain');
      expect(seeded.applicationProperties).to.deep.equal({ origin: 'amqp 0-9-1' });
      await seededReceiver.deadLetterMessage(seeded, { deadLetterReason: 'seeded', deadLetterErrorDescription: 'test' });
      await seededReceiver.close();
      const dlq = client.createReceiver('seeded-q', { subQueueType: 'deadLetter', receiveMode: 'receiveAndDelete' });
      const [dead] = await dlq.receiveMessages(1, { maxWaitTimeInMs: 2000 });
      expect(dead.deadLetterReason).to.equal('seeded');
      expect(dead.applicationProperties.origin).to.equal('amqp 0-9-1');
      await dlq.close();
    });

    And('messages can be scheduled on a topic', async () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createSubscription('scheduled-topic', 'sub');
      const topicSender = client.createSender('scheduled-topic');
      await topicSender.scheduleMessages({ body: 'topical' }, new Date(Date.now() + 100));
      mock.timers.tick(300);
      expect(ctx.server.getBroker().getQueue('scheduled-topic/Subscriptions/sub').messageCount).to.equal(1);
      await topicSender.close();
    });
  });

  Scenario('receive and delete, topics and streaming', () => {
    let ctx, client;
    after(async () => {
      await client.close();
      await ctx.stop();
    });

    Given('a client and a topic with two subscriptions', async () => {
      ctx = await startServer();
      client = new ServiceBusClient(connectionString(ctx));
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createTopic('events');
      serviceBus.createSubscription('events', 'audit');
      serviceBus.createSubscription('events', 'billing');
    });

    Then('a message published to the topic reaches both subscriptions', async () => {
      const sender = client.createSender('events');
      await sender.sendMessages({ body: 'fan out' });
      for (const subscription of ['audit', 'billing']) {
        const receiver = client.createReceiver('events', subscription, { receiveMode: 'receiveAndDelete' });
        const [message] = await receiver.receiveMessages(1, { maxWaitTimeInMs: 2000 });
        expect(message.body).to.equal('fan out');
        expect(message.lockToken).to.be.undefined;
        await receiver.close();
      }
      expect(ctx.server.getBroker().getQueue('events/Subscriptions/audit').messageCount).to.equal(0);
      await sender.close();
    });

    And('a streaming receiver processes messages as they arrive', async () => {
      const receiver = client.createReceiver('stream');
      const received = [];
      const done = new Promise((resolve) => {
        receiver.subscribe({
          async processMessage(message) {
            received.push(message.body);
            await receiver.completeMessage(message);
            if (received.length === 2) resolve(true);
          },
          async processError() {},
        });
      });
      const sender = client.createSender('stream');
      await sender.sendMessages({ body: 'a' });
      await sender.sendMessages({ body: 'b' });
      await done;
      expect(received).to.deep.equal(['a', 'b']);
      await receiver.close();
      expect(ctx.server.getBroker().getQueue('stream').messageCount).to.equal(0);
    });

    And('a pre created queue is used as is', async () => {
      ctx.server.serviceBus().createQueue('preexisting');
      const sender = client.createSender('preexisting');
      await sender.sendMessages({ body: 'there' });
      expect(ctx.server.getBroker().getQueue('preexisting').messageCount).to.equal(1);
    });
  });
});
