import { mock } from 'node:test';

import { startServer, consumeMessages, once, tick } from '../helpers/index.js';

Feature('Queues', () => {
  Scenario('declaring queues', () => {
    let ctx, channel, broker, generated;
    after(() => ctx.stop());

    Given('a connected channel', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      broker = ctx.server.getBroker();
    });

    When('a named durable queue is declared', async () => {
      const ok = await channel.assertQueue('orders', { durable: true });
      expect(ok).to.deep.equal({ queue: 'orders', messageCount: 0, consumerCount: 0 });
    });

    Then('it exists in the broker', () => {
      expect(broker.getQueue('orders')).to.have.nested.property('options.durable', true);
    });

    When('a server named queue is declared', async () => {
      generated = await channel.assertQueue('', { exclusive: true });
    });

    Then('the name is generated with the amq.gen- prefix', () => {
      expect(generated.queue).to.match(/^amq\.gen-[\w-]{22}$/);
      expect(broker.getQueue(generated.queue)).to.be.ok;
    });

    And('redeclaring with the same arguments returns counts', async () => {
      channel.sendToQueue('orders', Buffer.from('1'));
      channel.sendToQueue('orders', Buffer.from('2'));
      const ok = await channel.assertQueue('orders', { durable: true });
      expect(ok).to.deep.equal({ queue: 'orders', messageCount: 2, consumerCount: 0 });
    });

    And('checkQueue works on an existing queue', async () => {
      const ok = await channel.checkQueue('orders');
      expect(ok.messageCount).to.equal(2);
    });

    And('declaring a new amq. queue is refused with 403', async () => {
      const err = await channel.assertQueue('amq.mine').catch((e) => e);
      expect(err).to.have.property('code', 403);
    });
  });

  Scenario('checking a missing queue', () => {
    let ctx, channel, error;
    after(() => ctx.stop());

    Given('a channel', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
    });

    When('a missing queue is checked', async () => {
      error = await channel.checkQueue('missing').catch((e) => e);
    });

    Then('the channel is closed with 404 NOT_FOUND', () => {
      expect(error).to.have.property('code', 404);
      expect(error.message).to.match(/no queue 'missing' in vhost '\/'/);
      expect(error).to.have.property('classId', 50);
      expect(error).to.have.property('methodId', 10);
    });
  });

  Scenario('redeclaring with different arguments', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel with a durable queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertQueue('durable-q', { durable: true });
    });

    Then('redeclaring as non-durable closes the channel with 406', async () => {
      const err = await channel.assertQueue('durable-q', { durable: false }).catch((e) => e);
      expect(err).to.have.property('code', 406);
      expect(err.message).to.match(/inequivalent arg 'durable' for queue 'durable-q'/);
    });
  });

  Scenario('exclusive queues', () => {
    let ctx, owner, other, otherChannel;
    after(() => ctx.stop());

    Given('a connection with an exclusive queue', async () => {
      ctx = await startServer();
      owner = await ctx.connect();
      const channel = await owner.createChannel();
      await channel.assertQueue('mine', { exclusive: true });
    });

    And('another connection', async () => {
      other = await ctx.connect();
      otherChannel = await other.createChannel();
      otherChannel.on('error', () => {});
    });

    Then('the other connection cannot declare the queue, 405 RESOURCE_LOCKED', async () => {
      const err = await otherChannel.assertQueue('mine', { exclusive: true }).catch((e) => e);
      expect(err).to.have.property('code', 405);
      expect(err.message).to.match(/cannot obtain exclusive access to locked queue 'mine'/);
    });

    And('the other connection cannot consume from it either', async () => {
      otherChannel = await other.createChannel();
      otherChannel.on('error', () => {});
      const err = await otherChannel.consume('mine', () => {}).catch((e) => e);
      expect(err).to.have.property('code', 405);
    });

    When('the owning connection closes', async () => {
      await owner.close();
      await tick();
    });

    Then('the exclusive queue is deleted', () => {
      expect(ctx.server.getBroker().getQueue('mine')).to.be.undefined;
    });

    And('the other connection can now declare it', async () => {
      otherChannel = await other.createChannel();
      await otherChannel.assertQueue('mine', { exclusive: true });
    });
  });

  Scenario('an exclusive queue removed through the broker api', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connection with an exclusive queue that is deleted behind its back', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      const channel = await connection.createChannel();
      await channel.assertQueue('behind-back', { exclusive: true });
      ctx.server.getBroker().deleteQueue('behind-back');
    });

    Then('closing the connection is still fine', async () => {
      await connection.close();
      await tick();
      expect(ctx.server.connections.size).to.equal(0);
      expect(ctx.server.getVHost().exclusiveQueues.size).to.equal(0);
    });
  });

  Scenario('purging and deleting queues', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a queue with two messages', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertQueue('junk');
      channel.sendToQueue('junk', Buffer.from('1'));
      channel.sendToQueue('junk', Buffer.from('2'));
      await channel.checkQueue('junk');
    });

    Then('deleteQueue with ifEmpty fails with 406', async () => {
      const err = await channel.deleteQueue('junk', { ifEmpty: true }).catch((e) => e);
      expect(err).to.have.property('code', 406);
      expect(err.message).to.match(/queue 'junk' in vhost '\/' not empty/);
    });

    And('purgeQueue reports the purged message count', async () => {
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      const ok = await channel.purgeQueue('junk');
      expect(ok).to.deep.equal({ messageCount: 2 });
      expect((await channel.checkQueue('junk')).messageCount).to.equal(0);
    });

    And('deleteQueue with ifUnused fails with 406 when consumed', async () => {
      await channel.consume('junk', () => {});
      const err = await channel.deleteQueue('junk', { ifUnused: true }).catch((e) => e);
      expect(err).to.have.property('code', 406);
      expect(err.message).to.match(/queue 'junk' in vhost '\/' in use/);
    });

    And('deleteQueue reports the deleted message count', async () => {
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.sendToQueue('junk', Buffer.from('3'));
      const ok = await channel.deleteQueue('junk');
      expect(ok).to.deep.equal({ messageCount: 1 });
      expect(ctx.server.getBroker().getQueue('junk')).to.be.undefined;
    });

    And('deleting a missing queue is silently ok', async () => {
      const ok = await channel.deleteQueue('junk');
      expect(ok).to.deep.equal({ messageCount: 0 });
    });
  });

  Scenario('auto delete queues', () => {
    let ctx, channel, consumerTag;
    after(() => ctx.stop());

    Given('an auto delete queue with a consumer', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('temp', { autoDelete: true });
      ({ consumerTag } = await channel.consume('temp', () => {}));
    });

    When('the consumer is cancelled', async () => {
      await channel.cancel(consumerTag);
    });

    Then('the queue is gone', () => {
      expect(ctx.server.getBroker().getQueue('temp')).to.be.undefined;
    });
  });

  Scenario('queue arguments', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
    });

    Then('x-max-length drops the oldest message', async () => {
      await channel.assertQueue('capped', { arguments: { 'x-max-length': 2 } });
      channel.sendToQueue('capped', Buffer.from('1'));
      channel.sendToQueue('capped', Buffer.from('2'));
      channel.sendToQueue('capped', Buffer.from('3'));
      const ok = await channel.checkQueue('capped');
      expect(ok.messageCount).to.equal(2);
      const first = await channel.get('capped', { noAck: true });
      expect(first.content.toString()).to.equal('2');
    });

    And('x-message-ttl expires messages', async () => {
      await channel.assertQueue('short-lived', { arguments: { 'x-message-ttl': 20 } });
      channel.sendToQueue('short-lived', Buffer.from('gone soon'));
      await tick(40);
      expect(await channel.get('short-lived', { noAck: true })).to.equal(false);
    });

    And('per message expiration expires messages', async () => {
      await channel.assertQueue('per-message');
      channel.sendToQueue('per-message', Buffer.from('gone soon'), { expiration: '20' });
      channel.sendToQueue('per-message', Buffer.from('stays'));
      await tick(40);
      const msg = await channel.get('per-message', { noAck: true });
      expect(msg.content.toString()).to.equal('stays');
    });

    And('x-dead-letter-exchange receives rejected messages', async () => {
      await channel.assertExchange('dlx', 'topic');
      await channel.assertQueue('dead');
      await channel.bindQueue('dead', 'dlx', '#');
      await channel.assertQueue('work', { arguments: { 'x-dead-letter-exchange': 'dlx', 'x-dead-letter-routing-key': 'dead.work' } });
      channel.sendToQueue('work', Buffer.from('fail me'), { contentType: 'text/plain' });
      const [msg] = await consumeMessages(channel, 'work');
      channel.reject(msg, false);
      const [dead] = await consumeMessages(channel, 'dead', 1, { noAck: true });
      expect(dead.content.toString()).to.equal('fail me');
      expect(dead.fields.routingKey).to.equal('dead.work');
      expect(dead.properties.contentType).to.equal('text/plain');
    });
  });

  Scenario('messages seeded through the broker api', () => {
    let ctx, channel, broker;
    after(() => ctx.stop());

    Given('a queue seeded with a buffer, a string and an object via the smqp broker', async () => {
      ctx = await startServer();
      broker = ctx.server.getBroker();
      broker.assertQueue('seeded', { durable: false });
      broker.sendToQueue('seeded', Buffer.from('buffer'), { contentType: 'text/plain', messageId: 'seed-1' });
      broker.sendToQueue('seeded', 'string');
      broker.sendToQueue('seeded', { json: true });
      broker.sendToQueue('seeded');
      const connection = await ctx.connect();
      channel = await connection.createChannel();
    });

    Then('a client receives the buffer with its properties and without smqp internals', async () => {
      const msg = await channel.get('seeded', { noAck: true });
      expect(msg.content.toString()).to.equal('buffer');
      expect(msg.properties.contentType).to.equal('text/plain');
      expect(msg.properties.messageId).to.equal('seed-1');
      expect(msg.properties.timestamp).to.be.undefined;
    });

    And('the string as utf8 content', async () => {
      const msg = await channel.get('seeded', { noAck: true });
      expect(msg.content.toString()).to.equal('string');
      expect(msg.properties.messageId).to.be.undefined;
    });

    And('the object as json content', async () => {
      const msg = await channel.get('seeded', { noAck: true });
      expect(JSON.parse(msg.content.toString())).to.deep.equal({ json: true });
    });

    And('missing content as an empty body', async () => {
      const msg = await channel.get('seeded', { noAck: true });
      expect(msg.content.length).to.equal(0);
    });

    And('a consumer gets seeded messages with empty exchange and routing key', async () => {
      broker.sendToQueue('seeded', Buffer.from('consumed'));
      const [msg] = await consumeMessages(channel, 'seeded', 1, { noAck: true });
      expect(msg.fields).to.include({ exchange: '', routingKey: '' });
      expect(msg.content.toString()).to.equal('consumed');
    });
  });

  Scenario('expired messages without a consumer', () => {
    let ctx, channel;
    after(async () => {
      mock.timers.reset();
      await ctx.stop();
    });

    Given('mocked time and a queue with a 500ms ttl dead lettering to another queue', async () => {
      mock.timers.enable({ apis: ['setInterval', 'Date'] });
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertExchange('dlx', 'topic');
      await channel.assertQueue('dead');
      await channel.bindQueue('dead', 'dlx', '#');
      await channel.assertQueue('mortal', { arguments: { 'x-message-ttl': 500, 'x-dead-letter-exchange': 'dlx' } });
      channel.sendToQueue('mortal', Buffer.from('soon'));
      channel.sendToQueue('mortal', Buffer.from('later'), { expiration: '5000' });
      expect((await channel.checkQueue('mortal')).messageCount).to.equal(2);
    });

    When('the ttl passes with nobody consuming', () => {
      mock.timers.tick(600);
    });

    Then('the expired message has been dead lettered', async () => {
      const [dead] = await consumeMessages(channel, 'dead', 1, { noAck: true });
      expect(dead.content.toString()).to.equal('soon');
      expect(dead.fields.exchange).to.equal('dlx');
    });

    And('the declare-ok count no longer includes it', async () => {
      expect((await channel.checkQueue('mortal')).messageCount).to.equal(1);
    });

    When('the per message expiration passes too', () => {
      mock.timers.tick(5000);
    });

    Then('the queue is empty', async () => {
      expect((await channel.checkQueue('mortal')).messageCount).to.equal(0);
      expect(ctx.server.getBroker().getQueue('dead').messageCount).to.equal(1);
    });
  });

  Scenario('broker statistics', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('two queues with messages, one of them consumed without acks', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('stats-a');
      await channel.assertQueue('stats-b');
      channel.sendToQueue('stats-a', Buffer.from('1'));
      channel.sendToQueue('stats-a', Buffer.from('2'));
      channel.sendToQueue('stats-b', Buffer.from('3'));
      await channel.consume('stats-a', () => {});
      await tick();
    });

    Then('server.getStats() reports totals and per queue numbers', () => {
      const stats = ctx.server.getStats();
      expect(stats).to.deep.include({ messageCount: 3, unackedCount: 2, consumerCount: 1 });
      expect(stats.queues.find((q) => q.name === 'stats-a')).to.deep.equal({
        name: 'stats-a',
        messageCount: 2,
        unackedCount: 2,
        consumerCount: 1,
      });
      expect(stats.queues.find((q) => q.name === 'stats-b')).to.deep.equal({
        name: 'stats-b',
        messageCount: 1,
        unackedCount: 0,
        consumerCount: 0,
      });
    });

    And('declare-ok reports only ready messages', async () => {
      const connection = await ctx.connect();
      const other = await connection.createChannel();
      expect(await other.checkQueue('stats-a')).to.deep.equal({ queue: 'stats-a', messageCount: 0, consumerCount: 1 });
    });

    And('stats for an unknown vhost are empty', () => {
      expect(ctx.server.getStats('elsewhere')).to.deep.equal({ messageCount: 0, unackedCount: 0, consumerCount: 0, queues: [] });
    });
  });

  Scenario('the last declared queue', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel that declared a server named queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('', { exclusive: true });
    });

    Then('an empty queue name in consume refers to it', async () => {
      const received = once(channel, 'return');
      const { consumerTag } = await channel.consume('', () => {});
      expect(consumerTag).to.match(/^amq\.ctag-/);
      received.catch(() => {});
    });
  });
});
