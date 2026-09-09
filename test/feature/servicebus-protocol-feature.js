import { mock } from 'node:test';

import rhea from 'rhea';

import { startServer, once, tick } from '../helpers/index.js';

/**
 * A rhea connection that announces itself like the Service Bus SDK, which switches the emulator to the Service Bus dialect
 * @param {Awaited<ReturnType<typeof startServer>>} ctx
 */
function connectAsSdk(ctx) {
  const container = rhea.create_container();
  const connection = container.connect({
    host: '127.0.0.1',
    port: ctx.server.port,
    reconnect: false,
    properties: { product: 'MSJSClient' },
  });
  return new Promise((resolve) => connection.once('connection_open', () => resolve(connection)));
}

/**
 * A request response pair on a management or cbs address
 * @param {import('rhea').Connection} connection
 * @param {string} address
 */
async function requestLink(connection, address) {
  const replyTo = `reply-${Math.random().toString(36).slice(2)}`;
  const sender = connection.open_sender({ target: { address } });
  const receiver = connection.open_receiver({ source: { address }, name: replyTo, target: { address: replyTo } });
  // attach both listeners before awaiting, the two events can arrive in the same tick
  await Promise.all([once(sender, 'sendable'), once(receiver, 'receiver_open')]);
  /** @type {Map<string, (message: any) => void>} */
  const waiting = new Map();
  receiver.on('message', (context) => waiting.get(context.message.correlation_id)?.(context.message));
  let counter = 0;
  return {
    /**
     * @param {string} operation
     * @param {any} [body]
     * @param {Record<string, any>} [properties]
     * @returns {Promise<any>}
     */
    request(operation, body, properties = {}) {
      const messageId = `req-${++counter}`;
      return new Promise((resolve) => {
        waiting.set(messageId, resolve);
        sender.send({ message_id: messageId, reply_to: replyTo, body, application_properties: { operation, ...properties } });
      });
    },
    close() {
      sender.close();
      receiver.close();
    },
  };
}

Feature('Service Bus dialect over raw rhea links', function () {
  this.timeout(8000);

  Scenario('management operations answer with status codes', () => {
    let ctx, connection;
    after(async () => {
      await ctx.stop();
      mock.timers.reset();
    });

    Given('a connection posing as the SDK', async () => {
      // the "next sweep" enqueue below is driven by the vhost sweeper (setInterval) reading
      // Date.now(); mock both so it can be advanced instantly. setTimeout stays real so the
      // I/O round-trip settles (await tick(30) further down) still resolve.
      mock.timers.enable({ apis: ['Date', 'setInterval'] });
      ctx = await startServer();
      connection = await connectAsSdk(ctx);
      expect(/** @type {import('amqp-emulator').Amqp10Connection} */ (ctx.server.connections.values().next().value).dialect).to.equal(
        'servicebus',
      );
    });

    Then('an unknown operation is answered with 400 and amqp:not-implemented', async () => {
      const management = await requestLink(connection, 'unknown-op/$management');
      const response = await management.request('com.microsoft:make-coffee', {});
      expect(response.application_properties['status-code']).to.equal(400);
      expect(response.application_properties['error-condition']).to.equal('amqp:not-implemented');
      management.close();
    });

    And('renewing an unknown lock is 410 with lock lost', async () => {
      const management = await requestLink(connection, 'renew-none/$management');
      const response = await management.request('com.microsoft:renew-lock', { 'lock-tokens': [Buffer.alloc(16, 1)] });
      expect(response.application_properties['status-code']).to.equal(410);
      expect(response.application_properties['error-condition']).to.equal('com.microsoft:message-lock-lost');
      management.close();
    });

    And('peek returns the seeded message', async () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('peek-me');
      ctx.server.getBroker().sendToQueue('peek-me', Buffer.from('first'));
      const management = await requestLink(connection, 'peek-me/$management');
      const response = await management.request('com.microsoft:peek-message', {});
      expect(response.application_properties['status-code']).to.equal(200);
      expect(rhea.message.decode(response.body.messages[0].message).body.content.toString()).to.equal('first');
      management.close();
    });

    And('an unknown disposition status is 400 amqp:not-allowed', async () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('bad-disp');
      const broker = ctx.server.getBroker();
      broker.sendToQueue('bad-disp', Buffer.from('x'));
      const lock = serviceBus.lock(broker.getQueue('bad-disp').get(), 'bad-disp', null, null);
      const management = await requestLink(connection, 'bad-disp/$management');
      const response = await management.request('com.microsoft:update-disposition', {
        'lock-tokens': [lock.token],
        'disposition-status': 'forgotten',
      });
      expect(response.application_properties['status-code']).to.equal(400);
      expect(response.application_properties['error-condition']).to.equal('amqp:not-allowed');
      management.close();
    });

    And('receiving deferred messages that do not exist is 404', async () => {
      const management = await requestLink(connection, 'defer-none/$management');
      const response = await management.request('com.microsoft:receive-by-sequence-number', { 'sequence-numbers': [4242] });
      expect(response.application_properties['status-code']).to.equal(404);
      expect(response.application_properties['error-condition']).to.equal('com.microsoft:message-not-found');
      management.close();
    });

    And('a deferred message can be received and deleted by sequence number', async () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('defer-me');
      const broker = ctx.server.getBroker();
      broker.sendToQueue('defer-me', Buffer.from('deferred'));
      const message = broker.getQueue('defer-me').get();
      const sequenceNumber = serviceBus.stateOf(message).sequenceNumber;
      const lock = serviceBus.lock(message, 'defer-me', null, null);
      serviceBus.settle(lock.key, 'defered');
      const management = await requestLink(connection, 'defer-me/$management');
      const response = await management.request('com.microsoft:receive-by-sequence-number', {
        'sequence-numbers': [sequenceNumber],
        'receiver-settle-mode': 0,
      });
      expect(response.body.messages).to.have.length(1);
      expect(response.body.messages[0]['lock-token']).to.be.undefined;
      expect(serviceBus.deferred.get('defer-me').size).to.equal(0);
      management.close();
    });

    And('a scheduled message without a scheduled time is enqueued at the next sweep', async () => {
      ctx.server.serviceBus().createQueue('sched-me');
      const management = await requestLink(connection, 'sched-me/$management');
      const encoded = rhea.message.encode({ body: 'asap' });
      const response = await management.request('com.microsoft:schedule-message', {
        messages: [{ message: encoded, 'message-id': 'asap' }],
      });
      expect(response.application_properties['status-code']).to.equal(200);
      mock.timers.tick(150);
      expect(ctx.server.getBroker().getQueue('sched-me').messageCount).to.equal(1);
      management.close();
    });

    And('the namespace level $management refuses an unknown operation', async () => {
      const namespace = await requestLink(connection, '$management');
      const response = await namespace.request('com.microsoft:enumerate-rules', {});
      expect(response.application_properties['status-code']).to.equal(400);
      namespace.close();
    });

    And('a completed deferred message is removed', () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('defer-complete');
      const broker = ctx.server.getBroker();
      broker.sendToQueue('defer-complete', Buffer.from('gone'));
      const message = broker.getQueue('defer-complete').get();
      const deferLock = serviceBus.lock(message, 'defer-complete', null, null);
      serviceBus.settle(deferLock.key, 'defered');
      const completeLock = serviceBus.lock(message, 'defer-complete', null, null);
      serviceBus.settle(completeLock.key, 'completed');
      expect(serviceBus.deferred.get('defer-complete').size).to.equal(0);
      expect(broker.getQueue('defer-complete').messageCount).to.equal(0);
    });

    And('a management request whose reply-to matches no link is processed without a reply', async () => {
      const sender = connection.open_sender({ target: { address: 'orphan/$management' } });
      await once(sender, 'sendable');
      sender.send({
        message_id: 'x',
        reply_to: 'nobody-is-listening',
        application_properties: { operation: 'com.microsoft:peek-message' },
      });
      await tick(30);
      expect(ctx.errors).to.have.length(0);
      sender.close();
    });
  });

  Scenario('cbs accepts a token', () => {
    let ctx, connection, cbs;
    after(() => ctx.stop());

    Given('a connection with a $cbs link', async () => {
      ctx = await startServer();
      connection = await connectAsSdk(ctx);
      cbs = await requestLink(connection, '$cbs');
    });

    Then('put-token is accepted with 202', async () => {
      const response = await cbs.request('put-token', 'a-sas-token', { name: 'sb://host/entity', type: 'servicebus.windows.net:sastoken' });
      expect(response.application_properties['status-code']).to.equal(202);
    });

    And('any other cbs operation is refused with 400', async () => {
      const response = await cbs.request('delete-token', 'a-sas-token');
      expect(response.application_properties['status-code']).to.equal(400);
      expect(response.application_properties['error-condition']).to.equal('amqp:not-implemented');
      cbs.close();
    });
  });

  Scenario('topics fan out to subscriptions, receiving straight from a topic is refused', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connection posing as the SDK and a topic with a subscription', async () => {
      ctx = await startServer();
      connection = await connectAsSdk(ctx);
      ctx.server.serviceBus().createSubscription('announcements', 'all');
    });

    Then('a sender to the topic fans out to the subscription', async () => {
      const sender = connection.open_sender({ target: { address: 'announcements' } });
      await once(sender, 'sendable');
      sender.send({ body: 'broadcast' });
      await once(sender, 'accepted');
      expect(ctx.server.getBroker().getQueue('announcements/Subscriptions/all').messageCount).to.equal(1);
      sender.close();
    });

    And('a receiver straight from the topic is refused with amqp:not-found', async () => {
      const receiver = connection.open_receiver({ source: { address: 'announcements' } });
      const context = await once(receiver, 'receiver_error');
      expect(context.receiver.error.condition).to.equal('amqp:not-found');
    });
  });

  Scenario('a released peek lock delivery is redelivered', () => {
    let ctx, connection, queue;
    after(() => ctx.stop());

    Given('a connection posing as the SDK and a queue with one message', async () => {
      ctx = await startServer();
      connection = await connectAsSdk(ctx);
      ctx.server.serviceBus().createQueue('rel');
      ctx.server.getBroker().sendToQueue('rel', Buffer.from('hold'));
      queue = ctx.server.getBroker().getQueue('rel');
    });

    Then('releasing it hands it back with a higher delivery count and a fresh lock', async () => {
      const receiver = connection.open_receiver({ source: { address: 'rel' }, autoaccept: false, credit_window: 1 });
      const first = await once(receiver, 'message');
      expect(first.message.body.content.toString()).to.equal('hold');
      expect(first.message.message_annotations['x-opt-locked-until']).to.be.instanceOf(Date);
      expect(ctx.server.serviceBus().locks.size).to.equal(1);
      first.delivery.release();
      const again = await once(receiver, 'message');
      expect(again.message.body.content.toString()).to.equal('hold');
      expect(again.message.delivery_count).to.equal(1);
      expect(ctx.server.serviceBus().locks.size).to.equal(1);
      receiver.close();
      await tick();
    });

    And('closing the receiver leaves the lock alive for the management link to settle', async () => {
      const serviceBus = ctx.server.serviceBus();
      expect(serviceBus.locks.size).to.equal(1);
      const [lock] = serviceBus.locks.values();
      expect(lock.link).to.equal(null);
      expect(queue.messages[0].pending).to.be.true;
      const management = await requestLink(connection, 'rel/$management');
      const response = await management.request('com.microsoft:update-disposition', {
        'lock-tokens': [lock.token],
        'disposition-status': 'completed',
      });
      expect(response.application_properties['status-code']).to.equal(200);
      expect(serviceBus.locks.size).to.equal(0);
      expect(queue.messageCount).to.equal(0);
      management.close();
    });
  });

  Scenario('settlement and request edge cases', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connection posing as the SDK', async () => {
      ctx = await startServer();
      connection = await connectAsSdk(ctx);
    });

    Then('a rejected delivery with no error object still dead letters it', async () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('reject-bare');
      ctx.server.getBroker().sendToQueue('reject-bare', Buffer.from('x'));
      const receiver = connection.open_receiver({ source: { address: 'reject-bare' }, autoaccept: false, credit_window: 1 });
      const context = await once(receiver, 'message');
      context.delivery.reject();
      await tick(30);
      expect(ctx.server.getBroker().getQueue('reject-bare/$DeadLetterQueue').messageCount).to.equal(1);
      receiver.close();
    });

    And('a deferred message can be dead lettered directly', () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('defer-dl');
      const broker = ctx.server.getBroker();
      broker.sendToQueue('defer-dl', Buffer.from('y'));
      const message = broker.getQueue('defer-dl').get();
      serviceBus.settle(serviceBus.lock(message, 'defer-dl', null, null).key, 'defered');
      serviceBus.settle(serviceBus.lock(message, 'defer-dl', null, null).key, 'suspended', { deadLetterReason: 'stuck' });
      expect(serviceBus.deferred.get('defer-dl').size).to.equal(0);
      expect(broker.getQueue('defer-dl/$DeadLetterQueue').messageCount).to.equal(1);
    });

    And('a management request with no body is answered', async () => {
      const management = await requestLink(connection, 'no-body/$management');
      const response = await management.request('com.microsoft:peek-message');
      expect(response.application_properties['status-code']).to.equal(204);
      management.close();
    });

    And('a management operation that throws an unexpected error closes the connection', async () => {
      const serviceBus = ctx.server.serviceBus();
      serviceBus.createQueue('boom-q');
      ctx.server.getBroker().sendToQueue('boom-q', Buffer.from('z'));
      serviceBus.peek = () => {
        throw new Error('unexpected');
      };
      const errored = new Promise((resolve) => ctx.server.once('error', resolve));
      const closed = new Promise((resolve) => connection.once('connection_close', resolve));
      const management = await requestLink(connection, 'boom-q/$management');
      management.request('com.microsoft:peek-message', {});
      expect(await errored).to.have.property('message', 'unexpected');
      const context = await closed;
      expect(context.connection.get_error().condition).to.equal('amqp:internal-error');
    });
  });

  Scenario('a rejected delivery without dead letter info uses the error condition', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connection posing as the SDK and a queue with a message', async () => {
      ctx = await startServer();
      connection = await connectAsSdk(ctx);
      ctx.server.serviceBus().createQueue('reason');
      ctx.server.getBroker().sendToQueue('reason', Buffer.from('bad'));
    });

    Then('rejecting with only a condition dead letters it with that reason', async () => {
      const receiver = connection.open_receiver({ source: { address: 'reason' }, autoaccept: false, credit_window: 1 });
      const context = await once(receiver, 'message');
      context.delivery.reject({ condition: 'com.microsoft:dead-letter', description: 'no info here' });
      await tick(30);
      const dlq = ctx.server.getBroker().getQueue('reason/$DeadLetterQueue');
      expect(dlq.messageCount).to.equal(1);
      const properties = ctx.server.serviceBus().encode(dlq.messages[0]);
      const decoded = rhea.message.decode(properties);
      expect(decoded.application_properties.DeadLetterReason).to.equal('com.microsoft:dead-letter');
      expect(decoded.application_properties.DeadLetterErrorDescription).to.equal('no info here');
      receiver.close();
    });
  });
});
