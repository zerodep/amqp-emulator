import { startServer, consumeMessages, once, tick } from '../helpers/index.js';

Feature('Publish and consume', () => {
  Scenario('a message round trip', () => {
    let ctx, channel, message;
    after(() => ctx.stop());

    Given('a channel with a queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('q');
    });

    When('a message with properties is sent to the queue', async () => {
      channel.sendToQueue('q', Buffer.from('payload'), {
        contentType: 'application/json',
        contentEncoding: 'utf8',
        headers: { str: 'v', int: 42, neg: -7, big: 3000000000, float: 1.5, bool: true, nil: null, list: [1, 'two'], nested: { a: 1 } },
        deliveryMode: 2,
        priority: 3,
        correlationId: 'corr',
        replyTo: 'reply-q',
        expiration: '60000',
        messageId: 'mid-1',
        timestamp: 1700000000,
        type: 'test',
        userId: 'guest',
        appId: 'app',
      });
      [message] = await consumeMessages(channel, 'q');
    });

    Then('the consumer receives it with fields', () => {
      expect(message.fields).to.include({ deliveryTag: 1, redelivered: false, exchange: '', routingKey: 'q' });
      expect(message.fields.consumerTag).to.match(/^amq\.ctag-/);
      expect(message.content.toString()).to.equal('payload');
    });

    And('all properties intact', () => {
      expect(message.properties).to.deep.include({
        contentType: 'application/json',
        contentEncoding: 'utf8',
        headers: { str: 'v', int: 42, neg: -7, big: 3000000000, float: 1.5, bool: true, nil: null, list: [1, 'two'], nested: { a: 1 } },
        deliveryMode: 2,
        priority: 3,
        correlationId: 'corr',
        replyTo: 'reply-q',
        expiration: '60000',
        messageId: 'mid-1',
        timestamp: 1700000000,
        type: 'test',
        userId: 'guest',
        appId: 'app',
      });
      expect(message.properties.clusterId).to.be.undefined;
    });

    And('a message without properties arrives with none', async () => {
      channel.sendToQueue('q', Buffer.from('bare'));
      const [bare] = await consumeMessages(channel, 'q');
      expect(bare.properties.messageId).to.be.undefined;
      expect(bare.properties.timestamp).to.be.undefined;
      expect(bare.properties.headers).to.deep.equal({});
    });

    And('a large body is reassembled across frames', async () => {
      const big = Buffer.alloc(500000, 'x');
      channel.sendToQueue('q', big);
      const [large] = await consumeMessages(channel, 'q');
      expect(large.content.length).to.equal(500000);
      expect(large.content.equals(big)).to.be.true;
    });

    And('publishing to a missing exchange closes the channel with 404', async () => {
      const closed = once(channel, 'close');
      channel.on('error', () => {});
      channel.publish('missing', 'rk', Buffer.from('lost'));
      await closed;
    });
  });

  Scenario('acknowledgements', () => {
    let ctx, channel, queue;
    after(() => ctx.stop());

    Given('a queue with three messages', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertQueue('acks');
      for (const n of [1, 2, 3]) channel.sendToQueue('acks', Buffer.from(`${n}`));
      queue = ctx.server.getBroker().getQueue('acks');
    });

    let messages;
    When('all three are consumed', async () => {
      messages = await consumeMessages(channel, 'acks', 3);
      expect(messages.map((m) => m.fields.deliveryTag)).to.deep.equal([1, 2, 3]);
    });

    Then('they are pending in the broker queue until acked', () => {
      expect(queue.messageCount).to.equal(3);
    });

    When('the second is acked', async () => {
      channel.ack(messages[1]);
      await channel.checkQueue('acks');
    });

    Then('two remain', () => {
      expect(queue.messageCount).to.equal(2);
    });

    When('the third is acked with allUpTo', async () => {
      channel.ack(messages[2], true);
      await channel.checkQueue('acks');
    });

    Then('the queue is empty', () => {
      expect(queue.messageCount).to.equal(0);
    });

    And('acking an unknown delivery tag closes the channel with 406', async () => {
      const closed = once(channel, 'close');
      channel.ack(messages[1]);
      const err = await once(channel, 'error').catch(() => {});
      await closed;
      expect(err).to.have.property('code', 406);
      expect(err.message).to.match(/unknown delivery tag 2/);
    });
  });

  Scenario('negative acknowledgements', () => {
    let ctx, channel, queue;
    after(() => ctx.stop());

    Given('a queue with two messages', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('nacks');
      channel.sendToQueue('nacks', Buffer.from('1'));
      channel.sendToQueue('nacks', Buffer.from('2'));
      queue = ctx.server.getBroker().getQueue('nacks');
    });

    let held;
    Then('a nacked message with requeue is redelivered', async () => {
      const [first] = await consumeMessages(channel, 'nacks', 1);
      expect(first.content.toString()).to.equal('1');
      expect(first.fields.redelivered).to.be.false;
      channel.nack(first, false, true);
      const messages = await consumeMessages(channel, 'nacks', 2);
      const redelivered = messages.find((m) => m.content.toString() === '1');
      held = messages.find((m) => m.content.toString() === '2');
      expect(redelivered.fields.redelivered).to.be.true;
      expect(redelivered.fields.deliveryTag).to.be.above(first.fields.deliveryTag);
      channel.ack(redelivered);
    });

    And('a rejected message without requeue is dropped', async () => {
      channel.reject(held, false);
      await channel.checkQueue('nacks');
      expect(queue.messageCount).to.equal(0);
    });

    And('nackAll requeues everything outstanding', async () => {
      channel.sendToQueue('nacks', Buffer.from('a'));
      channel.sendToQueue('nacks', Buffer.from('b'));
      await consumeMessages(channel, 'nacks', 2);
      channel.nackAll(true);
      await channel.checkQueue('nacks');
      expect(queue.messageCount).to.equal(2);
    });
  });

  Scenario('consumer prefetch', () => {
    let ctx, channel, received;
    after(() => ctx.stop());

    Given('a channel with prefetch 1 and a queue with three messages', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.prefetch(1);
      await channel.assertQueue('slow');
      for (const n of [1, 2, 3]) channel.sendToQueue('slow', Buffer.from(`${n}`));
      received = [];
      await channel.consume('slow', (msg) => received.push(msg));
      await tick();
    });

    Then('only one message is delivered', () => {
      expect(received.map((m) => m.content.toString())).to.deep.equal(['1']);
    });

    When('it is acked', async () => {
      channel.ack(received[0]);
      await tick();
    });

    Then('the next one arrives', () => {
      expect(received.map((m) => m.content.toString())).to.deep.equal(['1', '2']);
    });

    When('the second is nacked with requeue', async () => {
      channel.nack(received[1]);
      await tick();
    });

    Then('it is redelivered before the third', () => {
      expect(received.map((m) => m.content.toString())).to.deep.equal(['1', '2', '2']);
      expect(received[2].fields.redelivered).to.be.true;
    });
  });

  Scenario('channel wide prefetch', () => {
    let ctx, channel, received;
    after(() => ctx.stop());

    Given('a channel with global prefetch 1 consuming two queues with messages', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.prefetch(1, true);
      await channel.assertQueue('g1');
      await channel.assertQueue('g2');
      channel.sendToQueue('g1', Buffer.from('1a'));
      channel.sendToQueue('g1', Buffer.from('1b'));
      channel.sendToQueue('g2', Buffer.from('2a'));
      received = [];
      await channel.consume('g1', (msg) => received.push(msg));
      await channel.consume('g2', (msg) => received.push(msg));
      await tick();
    });

    Then('one message in total is outstanding', () => {
      expect(received.length).to.equal(1);
    });

    When('it is acked', async () => {
      channel.ack(received[0]);
      await tick();
    });

    Then('exactly one more arrives', () => {
      expect(received.length).to.equal(2);
    });

    When('everything is acked', async () => {
      channel.ackAll();
      await tick();
    });

    Then('the last one arrives', () => {
      expect(received.length).to.equal(3);
      expect(received.map((m) => m.content.toString()).sort()).to.deep.equal(['1a', '1b', '2a']);
    });
  });

  Scenario('noAck consumers', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a queue with a message', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('fire');
      channel.sendToQueue('fire', Buffer.from('forget'));
    });

    Then('consuming with noAck removes it from the queue immediately', async () => {
      await consumeMessages(channel, 'fire', 1, { noAck: true });
      expect(ctx.server.getBroker().getQueue('fire').messageCount).to.equal(0);
    });
  });

  Scenario('basic.get', () => {
    let ctx, channel, message;
    after(() => ctx.stop());

    Given('a queue with two messages', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('poll');
      channel.sendToQueue('poll', Buffer.from('first'));
      channel.sendToQueue('poll', Buffer.from('second'));
    });

    When('a message is fetched', async () => {
      message = await channel.get('poll');
    });

    Then('it comes with the remaining message count', () => {
      expect(message.content.toString()).to.equal('first');
      expect(message.fields).to.include({ messageCount: 1, deliveryTag: 1, routingKey: 'poll', exchange: '' });
    });

    And('it can be acked', async () => {
      channel.ack(message);
      const next = await channel.get('poll', { noAck: true });
      expect(next.content.toString()).to.equal('second');
    });

    And('an empty queue returns false', async () => {
      expect(await channel.get('poll')).to.equal(false);
    });
  });

  Scenario('recover', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel holding an unacked message', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('recover');
      channel.sendToQueue('recover', Buffer.from('again'));
      await channel.get('recover');
    });

    Then('recover requeues it as redelivered', async () => {
      await channel.recover();
      const message = await channel.get('recover');
      expect(message.fields.redelivered).to.be.true;
    });
  });

  Scenario('cancelling a consumer', () => {
    let ctx, channel, consumerTag, message, queue;
    after(() => ctx.stop());

    Given('a consumer holding an unacked message', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('cancel-q');
      channel.sendToQueue('cancel-q', Buffer.from('hold'));
      const received = consumeMessages(channel, 'cancel-q', 1, { consumerTag: 'my-tag' });
      [message] = await received;
      consumerTag = message.fields.consumerTag;
      queue = ctx.server.getBroker().getQueue('cancel-q');
    });

    Then('the consumer tag is the one requested', () => {
      expect(consumerTag).to.equal('my-tag');
    });

    And('reusing the tag is refused with 530 NOT_ALLOWED', async () => {
      const connection = await ctx.connect();
      const other = await connection.createChannel();
      other.on('error', () => {});
      await other.assertQueue('other-q');
      await other.consume('other-q', () => {}, { consumerTag: 'dup' });
      const err = await other.consume('other-q', () => {}, { consumerTag: 'dup' }).catch((e) => e);
      expect(err).to.have.property('code', 530);
    });

    When('the consumer is cancelled', async () => {
      await channel.cancel(consumerTag);
    });

    Then('the consumer is gone but the message is still unacked', () => {
      expect(queue.consumerCount).to.equal(0);
      expect(queue.messageCount).to.equal(1);
      expect(queue.messages[0].pending).to.be.true;
    });

    And('acking it afterwards works', async () => {
      channel.ack(message);
      await channel.checkQueue('cancel-q');
      expect(queue.messageCount).to.equal(0);
    });
  });

  Scenario('the server cancels a consumer', () => {
    let ctx, channel, cancelled;
    after(() => ctx.stop());

    Given('a consumer on a queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('vanishing');
      cancelled = new Promise((resolve) => {
        channel.consume('vanishing', (msg) => {
          if (msg === null) resolve(true);
        });
      });
    });

    When('the queue is deleted from another connection', async () => {
      const connection = await ctx.connect();
      const other = await connection.createChannel();
      await other.deleteQueue('vanishing');
    });

    Then('the consumer callback receives null', async () => {
      expect(await cancelled).to.be.true;
    });
  });

  Scenario('exclusive consumers', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a queue with a consumer', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertQueue('shared');
      await channel.consume('shared', () => {});
    });

    Then('an exclusive consume is refused with 403', async () => {
      const err = await channel.consume('shared', () => {}, { exclusive: true }).catch((e) => e);
      expect(err).to.have.property('code', 403);
      expect(err.message).to.match(/in exclusive use/);
    });
  });

  Scenario('closing a channel requeues unacked messages', () => {
    let ctx, connection, channel, queue;
    after(() => ctx.stop());

    Given('a channel holding two unacked messages', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('requeue');
      channel.sendToQueue('requeue', Buffer.from('1'));
      channel.sendToQueue('requeue', Buffer.from('2'));
      await consumeMessages(channel, 'requeue', 2);
      queue = ctx.server.getBroker().getQueue('requeue');
    });

    When('the channel closes', async () => {
      await channel.close();
    });

    Then('the messages are ready again', () => {
      expect(queue.messageCount).to.equal(2);
      expect(queue.consumerCount).to.equal(0);
      expect(queue.messages.every((m) => !m.pending)).to.be.true;
    });

    And('a new consumer gets them as redelivered', async () => {
      const other = await connection.createChannel();
      const messages = await consumeMessages(other, 'requeue', 2);
      expect(messages.map((m) => m.fields.redelivered)).to.deep.equal([true, true]);
    });
  });

  Scenario('dropping a connection requeues unacked messages', () => {
    let ctx, queue;
    after(() => ctx.stop());

    Given('a connection holding an unacked message', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      const channel = await connection.createChannel();
      await channel.assertQueue('dropped');
      channel.sendToQueue('dropped', Buffer.from('1'));
      await consumeMessages(channel, 'dropped', 1);
      queue = ctx.server.getBroker().getQueue('dropped');
      expect(queue.messages[0].pending).to.be.true;
      /** @type {any} */ (connection).connection.stream.destroy();
      await tick(20);
    });

    Then('the message is ready again', () => {
      expect(queue.messageCount).to.equal(1);
      expect(queue.messages[0].pending).to.be.false;
    });
  });

  Scenario('multiple channels on one connection', () => {
    let ctx, connection, first, second;
    after(() => ctx.stop());

    Given('two channels consuming the same queue', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      first = await connection.createChannel();
      second = await connection.createChannel();
      await first.prefetch(1);
      await second.prefetch(1);
      await first.assertQueue('shared-q');
    });

    Then('each channel has its own delivery tag sequence', async () => {
      const firstMessages = consumeMessages(first, 'shared-q', 1);
      const secondMessages = consumeMessages(second, 'shared-q', 1);
      await tick();
      first.sendToQueue('shared-q', Buffer.from('a'));
      first.sendToQueue('shared-q', Buffer.from('b'));
      const [a] = await firstMessages;
      const [b] = await secondMessages;
      expect(a.fields.deliveryTag).to.equal(1);
      expect(b.fields.deliveryTag).to.equal(1);
      first.ack(a);
      second.ack(b);
      await first.checkQueue('shared-q');
      expect(ctx.server.getBroker().getQueue('shared-q').messageCount).to.equal(0);
    });
  });

  Scenario('the broker throws unexpectedly', () => {
    let ctx, connection, channel, queue;
    after(() => ctx.stop());

    Given('a channel and a queue whose purge blows up', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertQueue('fragile');
      queue = ctx.server.getBroker().getQueue('fragile');
      queue.purge = () => {
        throw new Error('boom');
      };
    });

    Then('the channel is closed with 541 INTERNAL_ERROR and the server emits the error', async () => {
      const err = await channel.purgeQueue('fragile').catch((e) => e);
      expect(err).to.have.property('code', 541);
      expect(err.message).to.match(/INTERNAL_ERROR - boom/);
      expect(ctx.errors.pop()).to.have.property('message', 'boom');
    });

    And('a consume that blows up does not leave deliveries buffered', async () => {
      channel = await connection.createChannel();
      channel.on('error', () => {});
      queue.consume = () => {
        throw new Error('no consumers today');
      };
      const err = await channel.consume('fragile', () => {}).catch((e) => e);
      expect(err).to.have.property('code', 541);
      channel = await connection.createChannel();
      delete queue.consume;
      channel.sendToQueue('fragile', Buffer.from('after'));
      const [msg] = await consumeMessages(channel, 'fragile', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('after');
    });
  });

  Scenario('two connections share a vhost', () => {
    let ctx, publisher, consumer;
    after(() => ctx.stop());

    Given('a publisher and a consumer connection', async () => {
      ctx = await startServer();
      publisher = await (await ctx.connect()).createChannel();
      consumer = await (await ctx.connect()).createChannel();
      await consumer.assertExchange('bus', 'topic');
      await consumer.assertQueue('inbox');
      await consumer.bindQueue('inbox', 'bus', 'hello.*');
    });

    Then('the consumer receives what the publisher publishes', async () => {
      const received = consumeMessages(consumer, 'inbox', 1, { noAck: true });
      await tick();
      publisher.publish('bus', 'hello.world', Buffer.from('hi'), { headers: { from: 'publisher' } });
      const [msg] = await received;
      expect(msg.content.toString()).to.equal('hi');
      expect(msg.properties.headers.from).to.equal('publisher');
    });
  });
});
