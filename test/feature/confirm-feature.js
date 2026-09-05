import { startServer, consumeMessages, once } from '../helpers/index.js';

Feature('Publisher confirms', () => {
  Scenario('publishing on a confirm channel', () => {
    let ctx, channel, confirmed;
    after(() => ctx.stop());

    Given('a confirm channel with a queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createConfirmChannel();
      await channel.assertQueue('confirmed');
    });

    When('a message is sent with a confirm callback', () => {
      confirmed = new Promise((resolve, reject) => {
        channel.sendToQueue('confirmed', Buffer.from('sure'), {}, (err) => (err ? reject(err) : resolve(true)));
      });
    });

    Then('the callback is invoked without error', async () => {
      expect(await confirmed).to.be.true;
    });

    And('waitForConfirms resolves once everything is acked', async () => {
      for (let i = 0; i < 10; i++) channel.sendToQueue('confirmed', Buffer.from(`${i}`));
      await channel.waitForConfirms();
      expect(ctx.server.getBroker().getQueue('confirmed').messageCount).to.equal(11);
    });
  });

  Scenario('mandatory publish without a route', () => {
    let ctx, channel, returned, confirmed;
    after(() => ctx.stop());

    Given('a confirm channel with an exchange without bindings', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createConfirmChannel();
      await channel.assertExchange('unbound', 'topic');
      returned = once(channel, 'return');
    });

    When('a mandatory message is published', () => {
      confirmed = new Promise((resolve, reject) => {
        channel.publish('unbound', 'nowhere', Buffer.from('lost'), { mandatory: true, headers: { a: 1 } }, (err) =>
          err ? reject(err) : resolve(true),
        );
      });
    });

    Then('the message is returned with 312 NO_ROUTE', async () => {
      const msg = await returned;
      expect(msg.fields).to.include({ replyCode: 312, replyText: 'NO_ROUTE', exchange: 'unbound', routingKey: 'nowhere' });
      expect(msg.content.toString()).to.equal('lost');
      expect(msg.properties.headers).to.deep.equal({ a: 1 });
    });

    And('the publish is still confirmed', async () => {
      expect(await confirmed).to.be.true;
    });

    And('a mandatory message to a missing queue via the default exchange is returned too', async () => {
      const next = once(channel, 'return');
      channel.sendToQueue('no-such-queue', Buffer.from('lost again'), { mandatory: true });
      const msg = await next;
      expect(msg.fields).to.include({ replyCode: 312, exchange: '', routingKey: 'no-such-queue' });
    });

    And('a non mandatory message without route is dropped silently', async () => {
      await new Promise((resolve, reject) => {
        channel.publish('unbound', 'nowhere', Buffer.from('dropped'), {}, (err) => (err ? reject(err) : resolve(true)));
      });
    });
  });

  Scenario('mandatory publish with a route', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a confirm channel with a bound queue', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createConfirmChannel();
      await channel.assertExchange('bound', 'topic');
      await channel.assertQueue('bound-q');
      await channel.bindQueue('bound-q', 'bound', '#');
    });

    Then('a mandatory message is delivered and not returned', async () => {
      let returned = false;
      channel.on('return', () => (returned = true));
      channel.publish('bound', 'somewhere', Buffer.from('found'), { mandatory: true });
      const [msg] = await consumeMessages(channel, 'bound-q', 1, { noAck: true });
      expect(msg.content.toString()).to.equal('found');
      await channel.waitForConfirms();
      expect(returned).to.be.false;
    });
  });

  Scenario('mandatory publish on a plain channel', () => {
    let ctx, channel;
    after(() => ctx.stop());

    Given('a plain channel with an unbound exchange', async () => {
      ctx = await startServer();
      const connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertExchange('plain-unbound', 'fanout');
    });

    Then('a mandatory message is returned', async () => {
      const returned = once(channel, 'return');
      channel.publish('plain-unbound', '', Buffer.from('back'), { mandatory: true });
      const msg = await returned;
      expect(msg.content.toString()).to.equal('back');
    });
  });
});
