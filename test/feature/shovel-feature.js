import net from 'node:net';

import { startServer, consumeMessages, until, tick } from '../helpers/index.js';

Feature('Shovel', () => {
  Scenario('a local shovel moves messages from a source queue to a destination exchange', () => {
    let ctx, connection, channel;
    after(async () => {
      await ctx.stop();
    });

    Given('an emulator with a source queue and a destination exchange bound to a sink queue', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('in');
      await channel.assertExchange('out', 'topic');
      await channel.assertQueue('sink');
      await channel.bindQueue('sink', 'out', '#');
    });

    When('a shovel is created from the source queue to the destination exchange', () => {
      ctx.server.shovel('s1', {
        source: { queue: 'in' },
        destination: { exchange: 'out', routingKey: 'moved' },
      });
    });

    And('a message is published to the source queue', () => {
      channel.sendToQueue('in', Buffer.from('payload'), { messageId: 'm1', contentType: 'text/plain' });
    });

    Then('it arrives through the destination exchange with its properties preserved', async () => {
      const [message] = await consumeMessages(channel, 'sink', 1, { noAck: true });
      expect(message.content.toString()).to.equal('payload');
      expect(message.fields.routingKey).to.equal('moved');
      expect(message.fields.exchange).to.equal('out');
      expect(message.properties.messageId).to.equal('m1');
      expect(message.properties.contentType).to.equal('text/plain');
    });

    And('the source queue is left empty', async () => {
      await until(() => ctx.server.getBroker().getQueue('in').messageCount === 0);
      expect((await channel.checkQueue('in')).messageCount).to.equal(0);
    });
  });

  Scenario('a shovel to a destination queue can be closed to stop forwarding', () => {
    let ctx, connection, channel;
    after(async () => {
      await ctx.stop();
    });

    Given('a source and destination queue with a shovel between them', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('src');
      await channel.assertQueue('dst');
      ctx.server.shovel('to-queue', { source: { queue: 'src' }, destination: { queue: 'dst' } });
    });

    When('a message is published to the source queue', async () => {
      channel.sendToQueue('src', Buffer.from('one'));
      await until(() => ctx.server.getBroker().getQueue('dst').messageCount === 1);
    });

    Then('it is moved to the destination queue', async () => {
      const [message] = await consumeMessages(channel, 'dst', 1, { noAck: true });
      expect(message.content.toString()).to.equal('one');
    });

    When('the shovel is closed and another message is published', async () => {
      ctx.server.closeShovel('to-queue');
      channel.sendToQueue('src', Buffer.from('two'));
      await until(() => ctx.server.getBroker().getQueue('src').messageCount === 1);
    });

    Then('the message stays in the source queue', () => {
      expect(ctx.server.getBroker().getQueue('dst').messageCount).to.equal(0);
      expect(ctx.server.getBroker().getQueue('src').messageCount).to.equal(1);
    });
  });

  Scenario('the destination routing key defaults to the message routing key, across vhosts', () => {
    let ctx, connection, channel;
    after(async () => {
      await ctx.stop();
    });

    Given('a source queue fed through an exchange and a destination exchange in another vhost', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertExchange('src-ex', 'topic');
      await channel.assertQueue('routed');
      await channel.bindQueue('routed', 'src-ex', '#');
      // seed the destination vhost's exchange and a queue behind it through the broker api
      const other = ctx.server.getBroker('other');
      other.assertExchange('dst-ex', 'topic');
      other.assertQueue('landing', { durable: false, autoDelete: false });
      other.bindQueue('landing', 'dst-ex', '#');
      // no routingKey on the destination, so the shovel must reuse the message's own
      ctx.server.shovel('cross', {
        source: { queue: 'routed' },
        destination: { exchange: 'dst-ex', vhost: 'other' },
      });
    });

    When('a message is published with a routing key', async () => {
      channel.publish('src-ex', 'orders.new', Buffer.from('payload'), { messageId: 'r1' });
      await until(() => ctx.server.getBroker('other').getQueue('landing').messageCount === 1);
    });

    Then('it lands in the other vhost carrying its routing key and properties', () => {
      const message = ctx.server.getBroker('other').getQueue('landing').peek();
      expect(message.fields.routingKey).to.equal('orders.new');
      expect(message.properties.messageId).to.equal('r1');
      expect(message.content.toString()).to.equal('payload');
    });
  });

  Scenario('shovel creation is validated and cleaned up on reset', () => {
    let ctx;
    after(async () => {
      await ctx.stop();
    });

    Given('a running emulator with a source queue', async () => {
      ctx = await startServer();
      ctx.server.getBroker().assertQueue('q', { durable: false, autoDelete: false });
    });

    Then('bad configurations are rejected', () => {
      expect(() => ctx.server.shovel('', { source: { queue: 'q' }, destination: { queue: 'd' } })).to.throw(TypeError);
      expect(() => ctx.server.shovel('no-src', { source: {}, destination: { queue: 'd' } })).to.throw(TypeError, /source\.queue/);
      expect(() => ctx.server.shovel('no-dst', { source: { queue: 'q' }, destination: {} })).to.throw(TypeError, /destination/);
      expect(() => ctx.server.shovel('no-dst2', { source: { queue: 'q' } })).to.throw(TypeError, /destination/);
    });

    And('a duplicate name is rejected and closing an unknown shovel is a no-op', () => {
      ctx.server.shovel('dup', { source: { queue: 'q' }, destination: { queue: 'd' } });
      expect(() => ctx.server.shovel('dup', { source: { queue: 'q' }, destination: { queue: 'd' } })).to.throw(/already exists/);
      expect(() => ctx.server.closeShovel('never-created')).to.not.throw();
    });

    And('closing a shovel twice is safe', () => {
      const shovel = ctx.server.shovels.get('dup');
      shovel.close();
      expect(() => shovel.close()).to.not.throw();
    });

    And('reset clears all shovels', () => {
      ctx.server.shovel('another', { source: { queue: 'q' }, destination: { queue: 'd' } });
      ctx.server.reset();
      expect(ctx.server.shovels.size).to.equal(0);
    });
  });

  Scenario('a shovel to a remote broker moves messages over AMQP', () => {
    let source, remote, srcChannel, remoteChannel;
    after(async () => {
      await source.stop();
      await remote.stop();
    });

    Given('two emulators with a shovel from the first to a remote exchange on the second', async () => {
      source = await startServer();
      remote = await startServer();
      srcChannel = await (await source.connect()).createChannel();
      remoteChannel = await (await remote.connect()).createChannel();
      await srcChannel.assertQueue('in');
      await remoteChannel.assertExchange('out', 'topic');
      await remoteChannel.assertQueue('sink');
      await remoteChannel.bindQueue('sink', 'out', '#');
      const shovel = source.server.shovel('to-remote', {
        source: { queue: 'in' },
        destination: { uri: remote.url, exchange: 'out', routingKey: 'moved' },
      });
      await shovel.ready;
    });

    When('a message is published on the source emulator', () => {
      srcChannel.sendToQueue('in', Buffer.from('remote payload'), { messageId: 'r1', contentType: 'text/plain' });
    });

    Then('it arrives on the remote emulator through the destination exchange', async () => {
      const [message] = await consumeMessages(remoteChannel, 'sink', 1, { noAck: true });
      expect(message.content.toString()).to.equal('remote payload');
      expect(message.fields.routingKey).to.equal('moved');
      expect(message.properties.messageId).to.equal('r1');
      expect(message.properties.contentType).to.equal('text/plain');
    });

    And('the source queue drains once the remote broker confirms', async () => {
      await until(() => source.server.getBroker().getQueue('in').messageCount === 0);
      expect(source.server.getBroker().getQueue('in').messageCount).to.equal(0);
    });
  });

  Scenario('a message seeded through the broker api is shovelled to a remote broker with wire properties only', () => {
    let source, remote, remoteChannel;
    after(async () => {
      await source.stop();
      await remote.stop();
    });

    Given('two emulators with a shovel from a source queue to a remote queue', async () => {
      source = await startServer();
      remote = await startServer();
      remoteChannel = await (await remote.connect()).createChannel();
      await remoteChannel.assertQueue('sink');
      source.server.getBroker().assertQueue('in', { durable: false, autoDelete: false });
      const shovel = source.server.shovel('seeded-to-remote', {
        source: { queue: 'in' },
        destination: { uri: remote.url, queue: 'sink' },
      });
      await shovel.ready;
    });

    When('a message is seeded into the source queue through the broker api', () => {
      source.server.getBroker().sendToQueue('in', 'seeded', { messageId: 'seed-1', contentType: 'text/plain' });
    });

    Then('it arrives on the remote emulator with the seeded properties and without smqp internals', async () => {
      const [message] = await consumeMessages(remoteChannel, 'sink', 1, { noAck: true });
      expect(message.content.toString()).to.equal('seeded');
      expect(message.properties.messageId).to.equal('seed-1');
      expect(message.properties.contentType).to.equal('text/plain');
      expect(message.properties.timestamp).to.be.undefined;
    });
  });

  Scenario('a shovel consuming from a remote broker moves messages into the local emulator', () => {
    let local, remote, localChannel, remoteChannel;
    after(async () => {
      await local.stop();
      await remote.stop();
    });

    Given('a shovel consuming a remote queue into a local queue', async () => {
      local = await startServer();
      remote = await startServer();
      localChannel = await (await local.connect()).createChannel();
      remoteChannel = await (await remote.connect()).createChannel();
      await remoteChannel.assertQueue('remote-src');
      await localChannel.assertQueue('landing');
      const shovel = local.server.shovel('from-remote', {
        source: { queue: 'remote-src', uri: remote.url },
        destination: { queue: 'landing' },
      });
      await shovel.ready;
    });

    When('a message and an empty-bodied message are published to the remote source queue', () => {
      remoteChannel.sendToQueue('remote-src', Buffer.from('from remote'), { messageId: 's1' });
      remoteChannel.sendToQueue('remote-src', Buffer.alloc(0), { messageId: 's2' });
    });

    Then('both land in the local queue, empty body intact', async () => {
      const [first, second] = await consumeMessages(localChannel, 'landing', 2, { noAck: true });
      expect(first.content.toString()).to.equal('from remote');
      expect(first.properties.messageId).to.equal('s1');
      expect(second.content).to.have.length(0);
      expect(second.properties.messageId).to.equal('s2');
    });

    And('the remote source queue drains once the local emulator has it', async () => {
      await until(() => remote.server.getBroker().getQueue('remote-src').messageCount === 0);
      expect(remote.server.getBroker().getQueue('remote-src').messageCount).to.equal(0);
    });
  });

  Scenario('a remote destination queue, routing-key fallback, and a refused publish', () => {
    let source, remote, srcChannel, remoteChannel;
    after(async () => {
      await source.stop();
      await remote.stop();
    });

    Given('a shovel to a remote destination queue with no routing key given', async () => {
      source = await startServer();
      remote = await startServer();
      srcChannel = await (await source.connect()).createChannel();
      remoteChannel = await (await remote.connect()).createChannel();
      await srcChannel.assertExchange('feed', 'topic');
      await srcChannel.assertQueue('rq-in');
      await srcChannel.bindQueue('rq-in', 'feed', '#');
      await remoteChannel.assertQueue('rq-landing');
      const shovel = source.server.shovel('remote-queue', {
        source: { queue: 'rq-in' },
        destination: { uri: remote.url, queue: 'rq-landing' },
      });
      await shovel.ready;
    });

    When('a normal and an empty message are published through the feed exchange', () => {
      srcChannel.publish('feed', 'k1', Buffer.from('full'), { messageId: 'q1' });
      srcChannel.publish('feed', 'k2', Buffer.alloc(0), { messageId: 'q2' });
    });

    Then('both arrive on the remote destination queue', async () => {
      const [first, second] = await consumeMessages(remoteChannel, 'rq-landing', 2, { noAck: true });
      expect(first.content.toString()).to.equal('full');
      expect(first.properties.messageId).to.equal('q1');
      expect(second.content).to.have.length(0);
      expect(second.properties.messageId).to.equal('q2');
    });

    And('a shovel whose remote exchange does not exist leaves its source message unacked', async () => {
      await srcChannel.assertQueue('refused-in');
      const shovel = source.server.shovel('refused', {
        source: { queue: 'refused-in' },
        destination: { uri: remote.url, exchange: 'no-such-exchange', routingKey: 'x' },
      });
      await shovel.ready;
      srcChannel.sendToQueue('refused-in', Buffer.from('stuck'));
      // the remote refuses the publish with a channel error, so the confirm never comes and the source keeps the message unacked
      await tick(50);
      expect(source.server.getBroker().getQueue('refused-in').messageCount).to.equal(1);
    });

    And('disconnecting the remote broker settles the shovel client without throwing', async () => {
      source.server.getBroker().assertQueue('cc-in', { durable: false, autoDelete: false });
      const shovel = source.server.shovel('conn-close', {
        source: { queue: 'cc-in' },
        destination: { uri: remote.url, queue: 'rq-landing' },
      });
      await shovel.ready;
      remote.server.disconnect(); // remote sends connection.close to the shovel's client
      await tick(50);
      expect(source.server.shovels.has('conn-close')).to.be.true;
    });
  });

  Scenario('both endpoints remote, destination routing key falls back to the message routing key', () => {
    let hub, a, b, aChannel, bChannel;
    after(async () => {
      await hub.stop();
      await a.stop();
      await b.stop();
    });

    Given('a shovel hosted on one emulator moving between two others', async () => {
      hub = await startServer();
      a = await startServer();
      b = await startServer();
      aChannel = await (await a.connect()).createChannel();
      bChannel = await (await b.connect()).createChannel();
      await aChannel.assertExchange('af', 'topic');
      await aChannel.assertQueue('as');
      await aChannel.bindQueue('as', 'af', '#');
      await bChannel.assertExchange('bx', 'topic');
      await bChannel.assertQueue('bsink');
      await bChannel.bindQueue('bsink', 'bx', '#');
      const shovel = hub.server.shovel('both', {
        source: { queue: 'as', uri: a.url },
        destination: { exchange: 'bx', uri: b.url }, // no routing key given
      });
      await shovel.ready;
    });

    When('a message is published on the first with a routing key', () => {
      aChannel.publish('af', 'path.key', Buffer.from('hop'), { messageId: 'h1' });
    });

    Then('it reaches the third carrying its own routing key', async () => {
      const [message] = await consumeMessages(bChannel, 'bsink', 1, { noAck: true });
      expect(message.content.toString()).to.equal('hop');
      expect(message.fields.routingKey).to.equal('path.key');
      expect(message.properties.messageId).to.equal('h1');
    });
  });

  Scenario('a broker-seeded source message without wire properties is shoveled', () => {
    let ctx, channel;
    after(async () => {
      await ctx.stop();
    });

    Given('a local shovel between two queues and a message seeded straight through the broker', async () => {
      ctx = await startServer();
      channel = await (await ctx.connect()).createChannel();
      await channel.assertQueue('seed-src');
      await channel.assertQueue('seed-dst');
      ctx.server.shovel('seeded', { source: { queue: 'seed-src' }, destination: { queue: 'seed-dst' } });
      ctx.server.getBroker().sendToQueue('seed-src', Buffer.from('seeded'), { contentType: 'text/plain' });
    });

    Then('it is moved to the destination queue', async () => {
      const [message] = await consumeMessages(channel, 'seed-dst', 1, { noAck: true });
      expect(message.content.toString()).to.equal('seeded');
    });
  });

  Scenario('a remote uri with credentials and a vhost is honoured', () => {
    let source, remote, srcChannel, vhostChannel;
    after(async () => {
      await source.stop();
      await remote.stop();
    });

    Given('a shovel to an exchange on a specific vhost of a credentialed remote', async () => {
      source = await startServer();
      remote = await startServer();
      srcChannel = await (await source.connect()).createChannel();
      // declare the destination through the wire so the vhost knows the exchange type
      vhostChannel = await (await remote.connect(`amqp://127.0.0.1:${remote.server.port}/v1`)).createChannel();
      await vhostChannel.assertExchange('vx', 'topic');
      await vhostChannel.assertQueue('vsink');
      await vhostChannel.bindQueue('vsink', 'vx', '#');
      await srcChannel.assertQueue('vsrc');
      const shovel = source.server.shovel('creds', {
        source: { queue: 'vsrc' },
        destination: { uri: `amqp://user:pass@127.0.0.1:${remote.server.port}/v1`, exchange: 'vx', routingKey: 'k' },
      });
      await shovel.ready;
    });

    When('a message is published to the source queue', () => {
      srcChannel.sendToQueue('vsrc', Buffer.from('cred-hop'), { messageId: 'c1' });
    });

    Then('it arrives in the exchange on the credentialed vhost', async () => {
      const [message] = await consumeMessages(vhostChannel, 'vsink', 1, { noAck: true });
      expect(message.content.toString()).to.equal('cred-hop');
      expect(message.fields.routingKey).to.equal('k');
      expect(message.properties.messageId).to.equal('c1');
    });
  });

  Scenario('a remote that drops during the handshake makes the shovel fail to become ready', () => {
    let ctx, stub;
    after(async () => {
      await ctx.stop();
      await new Promise((resolve) => stub.close(resolve));
    });

    Given('an emulator and a stub server that accepts then drops the connection', async () => {
      ctx = await startServer();
      ctx.server.getBroker().assertQueue('h-in', { durable: false, autoDelete: false });
      stub = net.createServer((socket) => socket.destroy());
      await new Promise((resolve) => stub.listen(0, '127.0.0.1', resolve));
    });

    Then('creating a shovel to it rejects its ready promise', async () => {
      const shovel = ctx.server.shovel('bad-remote', {
        source: { queue: 'h-in' },
        destination: { uri: `amqp://127.0.0.1:${stub.address().port}`, queue: 'x' },
      });
      const err = await shovel.ready.catch((e) => e);
      expect(err).to.be.an('error');
    });
  });
});
