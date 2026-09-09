import callbackApi from 'amqplib/callback_api';
import { credentials } from 'amqplib';

import { Raw091Client } from '../helpers/raw091-client.js';

import { startServer, once, tick } from '../helpers/index.js';

Feature('Connection', () => {
  Scenario('a client connects to the emulator', () => {
    let ctx, connection, serverConnection;
    after(() => ctx.stop());

    Given('an emulator listening on a random port', async () => {
      ctx = await startServer();
      expect(ctx.server.port).to.be.above(0);
      expect(ctx.url).to.match(/^amqp:\/\/127\.0\.0\.1:\d+$/);
    });

    When('amqplib connects', async () => {
      connection = await ctx.connect();
    });

    Then('the handshake completed and server properties are exposed', () => {
      const props = connection.connection.serverProperties;
      expect(props.product).to.equal('amqp-emulator');
      expect(props.capabilities.publisher_confirms).to.be.true;
    });

    And('the server tracks the connection with user and vhost', () => {
      expect(ctx.server.connections.size).to.equal(1);
      [serverConnection] = ctx.server.connections;
      expect(serverConnection.user).to.equal('guest');
      expect(serverConnection.vhost.name).to.equal('/');
      expect(serverConnection.clientProperties.product).to.equal('amqplib');
    });

    When('the client closes', async () => {
      await connection.close();
      await tick();
    });

    Then('the server forgets the connection', () => {
      expect(ctx.server.connections.size).to.equal(0);
    });

    And('writing to the closed connection is a no-op', () => {
      expect(serverConnection.state).to.equal('closed');
      serverConnection.sendMethod(0, 'connection.blocked', { reason: 'too late' });
    });
  });

  Scenario('connecting to a vhost', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('an emulator', async () => {
      ctx = await startServer();
    });

    When('amqplib connects to vhost "orders"', async () => {
      connection = await ctx.connect(`${ctx.url}/orders`);
    });

    Then('a vhost with a separate broker is created', () => {
      expect(ctx.server.vhosts.has('orders')).to.be.true;
      expect(ctx.server.getBroker('orders')).to.not.equal(ctx.server.getBroker('/'));
    });

    And('queues declared on it live in that broker only', async () => {
      const channel = await connection.createChannel();
      await channel.assertQueue('orders-q');
      expect(ctx.server.getBroker('orders').getQueue('orders-q')).to.be.ok;
      expect(ctx.server.getBroker('/').getQueue('orders-q')).to.be.undefined;
    });
  });

  Scenario('restricted vhosts', () => {
    let ctx, error;
    after(() => ctx.stop());

    Given('an emulator that only allows vhost "/"', async () => {
      ctx = await startServer({ vhosts: ['/'] });
    });

    When('amqplib connects to vhost "nope"', async () => {
      error = await ctx.connect(`${ctx.url}/nope`).catch((err) => err);
    });

    Then('the connection is refused with connection.close during the handshake', () => {
      expect(error).to.be.instanceOf(Error);
      expect(error.message).to.match(/ConnectionClose/);
    });

    And('no connection remains on the server', async () => {
      await tick();
      expect(ctx.server.connections.size).to.equal(0);
    });
  });

  Scenario('authentication', () => {
    let ctx, error;
    after(() => ctx.stop());

    Given('an emulator with an authenticate callback', async () => {
      ctx = await startServer({ authenticate: (user, password) => user === 'admin' && password === 's3cret' });
    });

    When('amqplib connects with the wrong password', async () => {
      error = await ctx.connect(`amqp://admin:wrong@127.0.0.1:${ctx.server.port}`).catch((err) => err);
    });

    Then('the connection is refused with 403 ACCESS_REFUSED', () => {
      expect(error).to.be.instanceOf(Error);
      expect(error.message).to.match(/403 \(ACCESS-REFUSED\)/);
    });

    When('amqplib connects with the right password', async () => {
      error = await ctx.connect(`amqp://admin:s3cret@127.0.0.1:${ctx.server.port}`).catch((err) => err);
    });

    Then('the connection is accepted', () => {
      expect(error).to.not.be.instanceOf(Error);
    });
  });

  Scenario('authentication mechanisms', () => {
    let ctx;
    after(() => ctx.stop());

    Given('an emulator that only accepts admin', async () => {
      ctx = await startServer({ authenticate: (user, password) => user === 'admin' && password === 's3cret' });
    });

    Then('AMQPLAIN credentials are understood', async () => {
      const connection = await ctx.connect(ctx.url, { credentials: credentials.amqplain('admin', 's3cret') });
      const [serverConnection] = ctx.server.connections;
      expect(serverConnection.user).to.equal('admin');
      await connection.close();
    });

    And('an unsupported mechanism is refused with 530 NOT_ALLOWED', async () => {
      const raw = new Raw091Client(ctx.server.port);
      await raw.connect();
      raw.write(Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'));
      await raw.waitFor('connection.start');
      raw.send('connection.start-ok', 0, { clientProperties: {}, mechanism: 'EXTERNAL', response: Buffer.alloc(0), locale: 'en_US' });
      const close = await raw.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(530);
      expect(close.fields.replyText).to.match(/unsupported authentication mechanism EXTERNAL/);
      await raw.destroy();
    });
  });

  Scenario('updating the secret', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connected client', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
    });

    Then('connection.update-secret is acknowledged', async () => {
      await connection.updateSecret(Buffer.from('new-secret'), 'rotation');
    });
  });

  Scenario('connecting to a blocked broker', () => {
    let ctx, raw;
    after(async () => {
      await raw.destroy();
      await ctx.stop();
    });

    Given('a blocked emulator', async () => {
      ctx = await startServer();
      ctx.server.block('maintenance');
    });

    When('a client announcing the connection.blocked capability connects', async () => {
      raw = new Raw091Client(ctx.server.port);
      await raw.connect();
      await raw.handshake({ clientProperties: { capabilities: { 'connection.blocked': true } } });
    });

    Then('connection.blocked follows open-ok right away', async () => {
      const frame = await raw.nextFrame();
      expect(frame.method).to.equal('connection.blocked');
      expect(frame.fields.reason).to.equal('maintenance');
    });
  });

  Scenario('the broker forces connections closed', () => {
    let ctx, connection, closed;
    after(() => ctx.stop());

    Given('a connected client', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      closed = once(connection, 'close');
    });

    When('the server disconnects everyone, twice for good measure', () => {
      ctx.server.disconnect();
      ctx.server.disconnect();
    });

    Then('the client sees a 320 CONNECTION_FORCED error and closes', async () => {
      const err = await closed;
      expect(err.message).to.match(/320 \(CONNECTION-FORCED\)/);
    });
  });

  Scenario('blocked and unblocked notifications', () => {
    let ctx, connection;
    after(() => ctx.stop());

    Given('a connected client', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
    });

    Then('server block() emits blocked on the client', async () => {
      const blocked = once(connection, 'blocked');
      ctx.server.block('low on memory');
      expect(await blocked).to.equal('low on memory');
    });

    And('server unblock() emits unblocked', async () => {
      const unblocked = once(connection, 'unblocked');
      ctx.server.unblock();
      await unblocked;
    });
  });

  Scenario('a non-AMQP client', () => {
    let ctx, response;
    after(() => ctx.stop());

    Given('an emulator', async () => {
      ctx = await startServer();
    });

    When('something speaks HTTP to it', async () => {
      const net = await import('node:net');
      response = await new Promise((resolve) => {
        const socket = net.connect(ctx.server.port, '127.0.0.1', () => socket.write('GET / HTTP/1.0\r\n\r\n'));
        const chunks = [];
        socket.on('data', (c) => chunks.push(c));
        socket.on('close', () => resolve(Buffer.concat(chunks)));
      });
    });

    Then('the server replies with the protocol header it supports and hangs up', () => {
      expect(response.toString('binary')).to.equal(`AMQP${String.fromCharCode(0, 0, 9, 1)}`);
    });
  });

  Scenario('reset drops connections and state', () => {
    let ctx, connection, closed;
    after(() => ctx.stop());

    Given('a client that declared a queue', async () => {
      ctx = await startServer();
      connection = await ctx.connect();
      closed = once(connection, 'close');
      const channel = await connection.createChannel();
      await channel.assertQueue('persist-me', { durable: true });
      expect(ctx.server.getBroker().getQueue('persist-me')).to.be.ok;
    });

    When('the server is reset', () => {
      ctx.server.reset();
    });

    Then('the client connection is gone', async () => {
      await closed;
      expect(ctx.server.connections.size).to.equal(0);
    });

    And('the queue is gone but default exchanges remain', () => {
      expect(ctx.server.getBroker().getQueue('persist-me')).to.be.undefined;
      expect(ctx.server.getBroker().getExchange('amq.topic')).to.be.ok;
    });
  });

  Scenario('server without a channel limit', () => {
    let ctx;
    after(() => ctx.stop());

    Given('an emulator with channelMax 0', async () => {
      ctx = await startServer({ channelMax: 0 });
    });

    Then('the client channel max is taken as is', async () => {
      const raw = new Raw091Client(ctx.server.port);
      await raw.connect();
      raw.write(Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'));
      await raw.waitFor('connection.start');
      raw.send('connection.start-ok', 0, {
        clientProperties: {},
        mechanism: 'PLAIN',
        response: Buffer.from(['', 'guest', 'guest'].join(String.fromCharCode(0))),
        locale: 'en_US',
      });
      const tune = await raw.waitFor('connection.tune');
      expect(tune.fields.channelMax).to.equal(0);
      raw.send('connection.tune-ok', 0, { channelMax: 5, frameMax: tune.fields.frameMax, heartbeat: 0 });
      raw.send('connection.open', 0, { virtualHost: '/' });
      await raw.waitFor('connection.open-ok');
      const [connection] = ctx.server.connections;
      expect(connection.channelMax).to.equal(5);
      await raw.destroy();
    });

    And('amqplib gets no limit either', async () => {
      await ctx.connect();
      const [connection] = ctx.server.connections;
      expect(connection.channelMax).to.equal(0);
    });
  });

  Scenario('server lifecycle', () => {
    Then('port, host and url are unknown before listen and close is harmless', async () => {
      const { AmqpServer } = await import('amqp-emulator');
      const server = new AmqpServer();
      expect(server.port).to.be.undefined;
      expect(server.host).to.equal('localhost');
      expect(server.url).to.equal('amqp://localhost:undefined');
      server.unblock();
      await server.close();
    });

    And('listening on a taken port rejects', async () => {
      const { AmqpServer } = await import('amqp-emulator');
      const first = new AmqpServer();
      const { port } = await first.listen();
      const second = new AmqpServer();
      const emitted = new Promise((resolve) => second.on('error', resolve));
      const err = await second.listen(port).catch((e) => e);
      expect(err).to.have.property('code', 'EADDRINUSE');
      expect(await emitted).to.equal(err);
      await first.close();
    });

    And('listening on all interfaces yields a localhost url', async () => {
      const { AmqpServer } = await import('amqp-emulator');
      const server = new AmqpServer();
      const { url } = await server.listen(0, '0.0.0.0');
      expect(url).to.match(/^amqp:\/\/localhost:\d+$/);
      await server.close();
    });

    And('listening on ipv6 brackets the host', async () => {
      const { AmqpServer } = await import('amqp-emulator');
      const server = new AmqpServer();
      const { url } = await server.listen(0, '::1');
      expect(url).to.match(/^amqp:\/\/\[::1\]:\d+$/);
      await server.close();
    });
  });

  Scenario('connecting with the callback api', () => {
    let ctx;
    after(() => ctx.stop());

    Given('an emulator', async () => {
      ctx = await startServer();
    });

    Then('amqplib callback_api connects and declares', (done) => {
      callbackApi.connect(ctx.url, (err, connection) => {
        if (err) return done(err);
        connection.createChannel((err, channel) => {
          if (err) return done(err);
          channel.assertQueue('cb-queue', {}, (err, ok) => {
            if (err) return done(err);
            expect(ok.queue).to.equal('cb-queue');
            connection.close(done);
          });
        });
      });
    });
  });
});
