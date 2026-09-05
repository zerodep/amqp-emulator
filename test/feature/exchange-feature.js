import { startServer, consumeMessages, once } from '../helpers/index.js';

Feature('Exchanges', () => {
  Scenario('declaring exchanges', () => {
    let ctx, channel, broker;
    after(() => ctx.stop());

    Given('a connected channel', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      broker = ctx.server.getBroker();
    });

    When('a topic, a direct and a fanout exchange are declared', async () => {
      await channel.assertExchange('t', 'topic', { durable: true });
      await channel.assertExchange('d', 'direct', { durable: false });
      await channel.assertExchange('f', 'fanout', { durable: false, autoDelete: true });
    });

    Then('they exist in the broker with the declared types', () => {
      expect(broker.getExchange('t')).to.have.property('type', 'topic');
      expect(broker.getExchange('d')).to.have.property('type', 'direct');
      expect(broker.getExchange('f')).to.be.ok;
      expect(ctx.server.getVHost().getExchangeType('f')).to.equal('fanout');
    });

    And('redeclaring with the same arguments is fine', async () => {
      await channel.assertExchange('t', 'topic', { durable: true });
    });

    And('the default exchanges are already there', async () => {
      await channel.checkExchange('amq.topic');
      await channel.checkExchange('amq.direct');
      await channel.checkExchange('amq.fanout');
    });

    And('a headers exchange is refused with 540 NOT_IMPLEMENTED', async () => {
      const connection = await ctx.connect();
      const ch = await connection.createChannel();
      ch.on('error', () => {});
      const err = await ch.assertExchange('h', 'headers').catch((e) => e);
      expect(err).to.have.property('code', 540);
    });
  });

  Scenario('declaring an exchange with a different type', () => {
    let ctx, channel, error;
    after(() => ctx.stop());

    Given('a channel with a topic exchange', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertExchange('events', 'topic');
    });

    When('the exchange is redeclared as direct', async () => {
      error = await channel.assertExchange('events', 'direct').catch((e) => e);
    });

    Then('the channel is closed with 406 PRECONDITION_FAILED', () => {
      expect(error).to.have.property('code', 406);
      expect(error.message).to.match(/inequivalent arg 'type' for exchange 'events' in vhost '\/'/);
    });
  });

  Scenario('declaring an exchange with different durability', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel with a durable exchange', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertExchange('durable-x', 'topic', { durable: true });
    });

    Then('redeclaring it as non durable closes the channel with 406', async () => {
      const err = await channel.assertExchange('durable-x', 'topic', { durable: false }).catch((e) => e);
      expect(err).to.have.property('code', 406);
      expect(err.message).to.match(/inequivalent arg 'durable' for exchange 'durable-x'/);
    });
  });

  Scenario('declaring an exchange with an unknown type', () => {
    let ctx, connection, channel;
    after(() => ctx.stop());

    Given('a channel', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
    });

    Then('the connection is closed with 503 COMMAND_INVALID', async () => {
      const closed = once(connection, 'close');
      await channel.assertExchange('weird', 'x-weird').catch(() => {});
      const err = await closed;
      expect(err.message).to.match(/503 \(COMMAND-INVALID\)/);
      expect(err.message).to.match(/unknown exchange type 'x-weird'/);
    });
  });

  Scenario('checking a missing exchange', () => {
    let ctx, connection, channel, error, closed;
    after(() => ctx.stop());

    Given('a channel', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      closed = once(channel, 'close');
    });

    When('a missing exchange is checked', async () => {
      error = await channel.checkExchange('missing').catch((e) => e);
    });

    Then('the channel is closed with 404 NOT_FOUND', async () => {
      expect(error).to.have.property('code', 404);
      expect(error.message).to.match(/no exchange 'missing' in vhost '\/'/);
      await closed;
    });

    And('the connection is still usable', async () => {
      const ch = await connection.createChannel();
      await ch.assertExchange('still-here', 'topic');
    });
  });

  Scenario('reserved names', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
    });

    Then('declaring a new amq. exchange is refused with 403', async () => {
      const err = await channel.assertExchange('amq.mine', 'topic').catch((e) => e);
      expect(err).to.have.property('code', 403);
    });

    And('deleting a built in amq. exchange is refused with 403', async () => {
      const connection = await ctx.connect();
      const ch = await connection.createChannel();
      ch.on('error', () => {});
      const err = await ch.deleteExchange('amq.direct').catch((e) => e);
      expect(err).to.have.property('code', 403);
      expect(ctx.server.getBroker().getExchange('amq.direct')).to.be.ok;
    });
  });

  Scenario('routing', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel with topic, direct and fanout exchanges and bound queues', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertExchange('t', 'topic');
      await channel.assertExchange('d', 'direct');
      await channel.assertExchange('f', 'fanout');
      await channel.assertQueue('tq');
      await channel.assertQueue('dq');
      await channel.assertQueue('fq');
      await channel.bindQueue('tq', 't', 'order.*.created');
      await channel.bindQueue('dq', 'd', 'exact.key');
      await channel.bindQueue('fq', 'f', 'ignored');
    });

    Then('topic wildcards route', async () => {
      channel.publish('t', 'order.1.created', Buffer.from('yes'));
      channel.publish('t', 'order.1.deleted', Buffer.from('no'));
      const [msg] = await consumeMessages(channel, 'tq', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('yes');
      expect(ctx.server.getBroker().getQueue('tq').messageCount).to.equal(0);
    });

    And('direct requires an exact key', async () => {
      channel.publish('d', 'exact', Buffer.from('no'));
      channel.publish('d', 'exact.key', Buffer.from('yes'));
      const [msg] = await consumeMessages(channel, 'dq', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('yes');
      expect(msg.fields.routingKey).to.equal('exact.key');
    });

    And('fanout ignores the routing key', async () => {
      channel.publish('f', 'whatever', Buffer.from('yes'));
      const [msg] = await consumeMessages(channel, 'fq', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('yes');
      expect(msg.fields.exchange).to.equal('f');
    });

    And('binding a queue to the default exchange is refused with 403', async () => {
      const connection = await ctx.connect();
      const ch = await connection.createChannel();
      ch.on('error', () => {});
      expect(await ch.bindQueue('fq', '', 'x').catch((e) => e)).to.have.property('code', 403);
    });

    And('binding a queue to a missing exchange closes the channel with 404', async () => {
      const connection = await ctx.connect();
      const ch = await connection.createChannel();
      ch.on('error', () => {});
      expect(await ch.bindQueue('fq', 'nope', 'x').catch((e) => e)).to.have.property('code', 404);
    });

    And('an unbound queue no longer receives', async () => {
      await channel.unbindQueue('fq', 'f', 'ignored');
      channel.publish('f', 'whatever', Buffer.from('lost'));
      await channel.checkQueue('fq');
      expect(ctx.server.getBroker().getQueue('fq').messageCount).to.equal(0);
    });

    And('unbinding from a missing exchange closes the channel with 404', async () => {
      channel.on('error', () => {});
      const err = await channel.unbindQueue('fq', 'gone', 'x').catch((e) => e);
      expect(err).to.have.property('code', 404);
    });
  });

  Scenario('exchange to exchange bindings', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a source exchange bound to a destination exchange with a queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertExchange('source', 'topic');
      await channel.assertExchange('destination', 'topic');
      await channel.assertQueue('e2e-q');
      await channel.bindQueue('e2e-q', 'destination', '#');
      await channel.bindExchange('destination', 'source', 'forward.*');
    });

    Then('messages published to the source arrive via the destination', async () => {
      channel.publish('source', 'forward.this', Buffer.from('hello'));
      const [msg] = await consumeMessages(channel, 'e2e-q', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('hello');
    });

    And('binding the same pair again is idempotent', async () => {
      await channel.bindExchange('destination', 'source', 'forward.*');
      channel.publish('source', 'forward.once', Buffer.from('once'));
      const [msg] = await consumeMessages(channel, 'e2e-q', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('once');
      expect(ctx.server.getBroker().getQueue('e2e-q').messageCount).to.equal(0);
    });

    And('binding to or from a missing exchange closes the channel with 404', async () => {
      for (const [destination, source] of [
        ['missing', 'source'],
        ['destination', 'missing'],
      ]) {
        const connection = await ctx.connect();
        const ch = await connection.createChannel();
        ch.on('error', () => {});
        expect(await ch.bindExchange(destination, source, '#').catch((e) => e)).to.have.property('code', 404);
        const ch2 = await connection.createChannel();
        ch2.on('error', () => {});
        expect(await ch2.unbindExchange(destination, source, '#').catch((e) => e)).to.have.property('code', 404);
      }
    });

    And('an unexpected broker failure while binding closes the channel with 541', async () => {
      const broker = ctx.server.getBroker();
      const original = broker.bindExchange;
      broker.bindExchange = () => {
        throw new Error('shovel jam');
      };
      const connection = await ctx.connect();
      const ch = await connection.createChannel();
      ch.on('error', () => {});
      const err = await ch.bindExchange('destination', 'source', 'other.*').catch((e) => e);
      broker.bindExchange = original;
      expect(err).to.have.property('code', 541);
      expect(ctx.errors.pop()).to.have.property('message', 'shovel jam');
    });

    And('after unbind they no longer do', async () => {
      await channel.unbindExchange('destination', 'source', 'forward.*');
      channel.publish('source', 'forward.this', Buffer.from('lost'));
      await channel.checkQueue('e2e-q');
      expect(ctx.server.getBroker().getQueue('e2e-q').messageCount).to.equal(0);
    });
  });

  Scenario('deleting exchanges', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a channel with an exchange bound to a queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.assertExchange('doomed', 'topic');
      await channel.assertQueue('dq');
      await channel.bindQueue('dq', 'doomed', '#');
    });

    Then('deleting it with ifUnused fails with 406', async () => {
      const err = await channel.deleteExchange('doomed', { ifUnused: true }).catch((e) => e);
      expect(err).to.have.property('code', 406);
    });

    And('deleting it without guards removes it', async () => {
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      channel.on('error', () => {});
      await channel.deleteExchange('doomed');
      expect(ctx.server.getBroker().getExchange('doomed')).to.be.undefined;
    });

    And('deleting a missing exchange is silently ok', async () => {
      await channel.deleteExchange('doomed');
    });

    And('the exchange can be declared again with another type', async () => {
      await channel.assertExchange('doomed', 'direct');
    });
  });
});
