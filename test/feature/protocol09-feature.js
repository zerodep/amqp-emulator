import { mock } from 'node:test';

import { constants } from '@0dep/amqp-emulator';

import { startServer, tick } from '../helpers/index.js';
import { Raw091Client } from '../helpers/raw091-client.js';

Feature('Protocol edge cases', () => {
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let ctx;
  /** @type {Raw091Client} */
  let client;

  before(async () => {
    ctx = await startServer();
  });
  after(() => ctx.stop());
  afterEachScenario(() => client?.destroy());

  /** the most recently accepted 0-9-1 connection */
  function lastConnection() {
    return /** @type {import('@0dep/amqp-emulator').AmqpConnection} */ ([...ctx.server.connections].at(-1));
  }

  /** @param {number} port */
  function rawClient(port = ctx.server.port) {
    client = new Raw091Client(port);
    return client;
  }

  async function connectRaw(options) {
    rawClient();
    await client.connect();
    await client.handshake(options);
    return client;
  }

  Scenario('the handshake gets an unexpected frame', () => {
    Given('a raw client that sends the protocol header', async () => {
      rawClient();
      await client.connect();
      client.write(Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'));
      await client.waitFor('connection.start');
    });

    When('a heartbeat frame is followed by tune-ok instead of start-ok', () => {
      client.sendFrame(constants.FRAME_HEARTBEAT, 0, Buffer.alloc(0));
      client.send('connection.tune-ok', 0, { channelMax: 0, frameMax: 0, heartbeat: 0 });
    });

    Then('the server closes with 503 COMMAND_INVALID', async () => {
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(503);
      expect(close.fields.replyText).to.match(/expected connection.start-ok, got connection.tune-ok/);
    });

    And('acknowledging the close ends the connection', async () => {
      client.send('connection.close-ok', 0);
      expect(await client.closed).to.be.true;
    });
  });

  Scenario('a body frame during the handshake', () => {
    Given('a raw client that sent the protocol header', async () => {
      rawClient();
      await client.connect();
      client.write(Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'));
      await client.waitFor('connection.start');
    });

    Then('a method on channel 1 closes with 505 UNEXPECTED_FRAME', async () => {
      client.send('channel.open', 1);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
    });
  });

  Scenario('a body frame during the handshake', () => {
    Given('another raw client that sent the protocol header', async () => {
      rawClient();
      await client.connect();
      client.write(Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'));
      await client.waitFor('connection.start');
    });

    Then('sending a body frame closes with 505 UNEXPECTED_FRAME', async () => {
      client.sendFrame(constants.FRAME_BODY, 0, Buffer.from('nope'));
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
    });
  });

  Scenario('the client aborts during the handshake', () => {
    Given('a raw client that sent the protocol header', async () => {
      rawClient();
      await client.connect();
      client.write(Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'));
      await client.waitFor('connection.start');
    });

    Then('connection.close is answered with close-ok', async () => {
      client.send('connection.close', 0, { replyCode: 200, replyText: 'changed my mind', classId: 0, methodId: 0 });
      await client.waitFor('connection.close-ok');
      expect(await client.closed).to.be.true;
    });
  });

  Scenario('bad frames on an open connection', () => {
    Given('a connected raw client', () => connectRaw());

    Then('a frame with an unknown type closes with 505', async () => {
      client.sendFrame(9, 1, Buffer.alloc(0));
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
    });
  });

  Scenario('a broken frame end marker', () => {
    Given('a connected raw client', () => connectRaw());

    Then('the connection is closed with 501 FRAME_ERROR', async () => {
      const frame = Buffer.from([constants.FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, 0x00]);
      client.write(frame);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(501);
    });
  });

  Scenario('an unknown method id', () => {
    Given('a connected raw client', () => connectRaw());

    Then('the connection is closed with 541 INTERNAL_ERROR and the server emits error', async () => {
      client.sendFrame(constants.FRAME_METHOD, 1, Buffer.from([0, 99, 0, 99]));
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(541);
      expect(ctx.errors.pop())
        .to.have.property('message')
        .that.match(/Unknown AMQP method class 99 method 99/);
    });
  });

  Scenario('non method frames on channel 0', () => {
    Given('a connected raw client', () => connectRaw());

    Then('a content header on channel 0 closes with 505', async () => {
      client.sendHeader(0, 0);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
    });
  });

  Scenario('unexpected connection methods', () => {
    let connection;
    Given('a connected raw client', async () => {
      await connectRaw();
      connection = lastConnection();
    });

    Then('connection.tune-ok after the handshake closes with 503', async () => {
      client.send('connection.tune-ok', 0, { channelMax: 0, frameMax: 0, heartbeat: 0 });
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(503);
      expect(connection.state).to.equal('closing');
    });

    And('further frames while closing are ignored until close-ok', async () => {
      client.send('channel.open', 1);
      client.send('connection.blocked', 0, { reason: 'ignored' });
      await tick(20);
      expect(ctx.server.connections.has(connection)).to.be.true;
      client.send('connection.close-ok', 0);
      expect(await client.closed).to.be.true;
      expect(ctx.server.connections.has(connection)).to.be.false;
    });
  });

  Scenario('connection methods on a channel', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('connection.close on channel 1 closes the connection with 503', async () => {
      client.send('connection.close', 1, { replyCode: 200, replyText: '', classId: 0, methodId: 0 });
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(503);
    });
  });

  Scenario('frames on unopened channels', () => {
    Given('a connected raw client', () => connectRaw());

    Then('a stray channel.close-ok is ignored', async () => {
      client.send('channel.close-ok', 7);
      await client.openChannel(1);
    });

    And('a method other than channel.open closes the connection with 504', async () => {
      client.send('queue.declare', 2, { queue: 'q' });
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(504);
      expect(close.fields.replyText).to.match(/expected 'channel.open', got queue.declare/);
    });
  });

  Scenario('content frames on unopened channels', () => {
    Given('a connected raw client', () => connectRaw());

    Then('a content header closes with 504', async () => {
      client.sendHeader(3, 0);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(504);
    });
  });

  Scenario('a body frame on an unopened channel', () => {
    Given('a connected raw client', () => connectRaw());

    Then('the connection closes with 504', async () => {
      client.sendFrame(constants.FRAME_BODY, 3, Buffer.from('x'));
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(504);
    });
  });

  Scenario('channel number above channel-max', () => {
    let limited;
    after(() => limited.stop());

    Given('a server with channel-max 2', async () => {
      limited = await startServer({ channelMax: 2 });
      rawClient(limited.server.port);
      await client.connect();
      await client.handshake();
    });

    Then('opening channel 3 closes the connection with 504', async () => {
      client.send('channel.open', 3);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(504);
      expect(close.fields.replyText).to.match(/exceeds channel-max 2/);
    });
  });

  Scenario('opening a channel twice', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('a second channel.open closes the connection with 504', async () => {
      client.send('channel.open', 1);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(504);
      expect(close.fields.replyText).to.match(/second 'channel.open' seen/);
    });
  });

  Scenario('channel flow and access request', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('channel.flow is echoed', async () => {
      client.send('channel.flow', 1, { active: false });
      const ok = await client.waitFor('channel.flow-ok');
      expect(ok.fields.active).to.be.false;
    });

    And('access.request gets a ticket', async () => {
      client.send('access.request', 1, { realm: '/data' });
      const ok = await client.waitFor('access.request-ok');
      expect(ok.fields.ticket).to.equal(1);
    });
  });

  Scenario('transactions', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('tx.select, tx.commit and tx.rollback are acknowledged', async () => {
      client.send('tx.select', 1);
      await client.waitFor('tx.select-ok');
      client.send('tx.commit', 1);
      await client.waitFor('tx.commit-ok');
      client.send('tx.rollback', 1);
      await client.waitFor('tx.rollback-ok');
    });
  });

  Scenario('methods the emulator does not implement', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('basic.recover-async closes the channel with 540', async () => {
      client.send('basic.recover-async', 1, { requeue: true });
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(540);
      client.send('channel.close-ok', 1);
    });

    And('a server-to-client method sent by the client closes the channel with 540', async () => {
      await client.openChannel(2);
      client.send('basic.consume-ok', 2, { consumerTag: 'x' });
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(540);
      expect(close.fields.replyText).to.match(/basic.consume-ok is not implemented/);
    });
  });

  Scenario('the client closes a channel the server is closing', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    When('the server closes the channel because of a missing queue', async () => {
      client.send('queue.declare', 1, { queue: 'missing', passive: true });
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(404);
    });

    And('content frames sent before the client noticed are ignored', async () => {
      client.send('basic.publish', 1, { exchange: '', routingKey: 'x' });
      client.sendHeader(1, 3);
      client.sendFrame(constants.FRAME_BODY, 1, Buffer.from('abc'));
      client.send('queue.declare', 1, { queue: 'ignored' });
      await tick();
      expect(ctx.server.getBroker().getQueue('ignored')).to.be.undefined;
    });

    Then('a channel.close from the client is answered with close-ok', async () => {
      client.send('channel.close', 1, { replyCode: 200, replyText: '', classId: 0, methodId: 0 });
      await client.waitFor('channel.close-ok');
      await tick();
      expect(lastConnection().channels.has(1)).to.be.false;
    });

    And('the channel can be opened again', async () => {
      await client.openChannel(1);
    });
  });

  Scenario('content frames out of order', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('a header without a publish closes the connection with 505', async () => {
      client.sendHeader(1, 0);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
    });
  });

  Scenario('a method while content is expected', () => {
    Given('a connected raw client that started a publish', async () => {
      await connectRaw();
      await client.openChannel(1);
      client.send('basic.publish', 1, { exchange: '', routingKey: 'q' });
    });

    Then('another method instead of the header closes the connection with 505', async () => {
      client.send('queue.declare', 1, { queue: 'q' });
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
      expect(close.fields.replyText).to.match(/expected content header for basic.publish, got queue.declare/);
    });
  });

  Scenario('a body frame before the header', () => {
    Given('a connected raw client that started a publish', async () => {
      await connectRaw();
      await client.openChannel(1);
      client.send('basic.publish', 1, { exchange: '', routingKey: 'q' });
    });

    Then('the connection closes with 505', async () => {
      client.sendFrame(constants.FRAME_BODY, 1, Buffer.from('early'));
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
    });
  });

  Scenario('a content header with an unknown class', () => {
    Given('a connected raw client that started a publish', async () => {
      await connectRaw();
      await client.openChannel(1);
      client.send('basic.publish', 1, { exchange: '', routingKey: 'q' });
    });

    Then('the connection closes with 505 UNEXPECTED_FRAME', async () => {
      const payload = Buffer.alloc(14);
      payload.writeUInt16BE(99, 0);
      client.sendFrame(constants.FRAME_HEADER, 1, payload);
      const close = await client.waitFor('connection.close');
      expect(close.fields.replyCode).to.equal(505);
      expect(close.fields.replyText).to.match(/Unknown content class 99/);
      expect(ctx.errors).to.have.length(0);
    });
  });

  Scenario('publishing with immediate', () => {
    Given('a connected raw client with a channel and queue', async () => {
      await connectRaw();
      await client.openChannel(1);
      client.send('queue.declare', 1, { queue: 'imm' });
      await client.waitFor('queue.declare-ok');
    });

    Then('immediate=true closes the channel with 540', async () => {
      client.send('basic.publish', 1, { exchange: '', routingKey: 'imm', immediate: true });
      client.sendHeader(1, 0);
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(540);
    });
  });

  Scenario('recover without requeue', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('basic.recover requeue=false closes the channel with 540', async () => {
      client.send('basic.recover', 1, { requeue: false });
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(540);
    });
  });

  Scenario('nowait declarations', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('exchange, queue, bind, consume and cancel with nowait get no reply', async () => {
      client.send('exchange.declare', 1, { exchange: 'nw', type: 'topic', nowait: true });
      client.send('queue.declare', 1, { queue: 'nw-q', nowait: true });
      client.send('queue.bind', 1, { queue: 'nw-q', exchange: 'nw', routingKey: '#', nowait: true });
      client.send('basic.consume', 1, { queue: 'nw-q', consumerTag: 'nw-tag', nowait: true });
      client.send('basic.cancel', 1, { consumerTag: 'nw-tag', nowait: true });
      client.send('exchange.bind', 1, { destination: 'nw', source: 'amq.topic', routingKey: '#', nowait: true });
      client.send('exchange.unbind', 1, { destination: 'nw', source: 'amq.topic', routingKey: '#', nowait: true });
      client.send('queue.purge', 1, { queue: 'nw-q', nowait: true });
      client.send('queue.delete', 1, { queue: 'nw-q', nowait: true });
      client.send('exchange.delete', 1, { exchange: 'nw', nowait: true });
      client.send('confirm.select', 1, { nowait: true });
      client.send('queue.declare', 1, { queue: 'sync' });
      const reply = await client.nextFrame();
      expect(reply.method).to.equal('queue.declare-ok');
      expect(ctx.server.getBroker().getExchange('nw')).to.be.undefined;
      expect(ctx.server.getBroker().getQueue('nw-q')).to.be.undefined;
    });
  });

  Scenario('a client that only opened a socket', () => {
    let before;
    Given('a raw tcp connection without a protocol header', async () => {
      rawClient();
      await client.connect();
      await tick();
      before = ctx.server.connections.size;
    });

    Then('it is not a connection yet and blocking the broker sends it nothing', async () => {
      ctx.server.block('quiet');
      await tick();
      expect(ctx.server.connections.size).to.equal(before);
      expect(client.raw).to.have.length(0);
      ctx.server.unblock();
    });

    And('disconnecting drops the socket without a handshake', async () => {
      ctx.server.disconnect();
      expect(await client.closed).to.be.true;
      expect(client.raw).to.have.length(0);
    });
  });

  Scenario('a protocol header split over two chunks', () => {
    Given('a raw client sending four bytes at a time', async () => {
      rawClient();
      await client.connect();
      client.write(Buffer.from('AMQP', 'binary'));
      await tick();
      client.write(Buffer.from(String.fromCharCode(0, 0, 9, 1), 'binary'));
    });

    Then('the handshake starts', async () => {
      const start = await client.nextFrame();
      expect(start.method).to.equal('connection.start');
    });
  });

  Scenario('an empty queue name without a declared queue', () => {
    Given('a connected raw client with a fresh channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('basic.get with an empty queue name closes the channel with 404', async () => {
      client.send('basic.get', 1, { queue: '' });
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(404);
    });
  });

  Scenario('declaring the default exchange', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('exchange.declare with an empty name closes the channel with 403', async () => {
      client.send('exchange.declare', 1, { exchange: '', type: 'direct' });
      const close = await client.waitFor('channel.close');
      expect(close.fields.replyCode).to.equal(403);
      expect(close.fields.replyText).to.match(/default exchange/);
    });
  });

  Scenario('the server closes a channel on its own initiative', () => {
    Given('a connected raw client with a channel', async () => {
      await connectRaw();
      await client.openChannel(1);
    });

    Then('channel.close carries the code and zero class and method ids', async () => {
      const { AmqpError } = await import('@0dep/amqp-emulator');
      const connection = lastConnection();
      const channel = connection.channels.get(1);
      channel.close(new AmqpError(320, 'CONNECTION_FORCED - go away'));
      channel.close(new AmqpError(320, 'closing twice is a no-op'));
      const close = await client.waitFor('channel.close');
      expect(close.fields).to.deep.equal({ replyCode: 320, replyText: 'CONNECTION_FORCED - go away', classId: 0, methodId: 0 });
      client.send('channel.close-ok', 1);
      await tick();
      expect(connection.channels.has(1)).to.be.false;
    });
  });

  Scenario('operating on the last declared queue', () => {
    Given('a connected raw client that declared a queue', async () => {
      await connectRaw();
      await client.openChannel(1);
      client.send('queue.declare', 1, { queue: 'last-one' });
      await client.waitFor('queue.declare-ok');
    });

    Then('queue.delete with an empty name deletes it', async () => {
      client.send('queue.delete', 1, { queue: '' });
      await client.waitFor('queue.delete-ok');
      expect(ctx.server.getBroker().getQueue('last-one')).to.be.undefined;
    });
  });

  Scenario('pipelined frames', () => {
    Given('a raw client sending the protocol header and a heartbeat in one chunk', async () => {
      rawClient();
      await client.connect();
      client.write(
        Buffer.concat([Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary'), Buffer.from([8, 0, 0, 0, 0, 0, 0, 0xce])]),
      );
    });

    Then('the handshake starts', async () => {
      const start = await client.nextFrame();
      expect(start.method).to.equal('connection.start');
    });

    And('an empty vhost and a client channel-max are accepted when the server has no limit', async () => {
      client.send('connection.start-ok', 0, {
        clientProperties: {},
        mechanism: 'PLAIN',
        response: Buffer.from(['', 'guest', 'guest'].join(String.fromCharCode(0))),
        locale: 'en_US',
      });
      await client.waitFor('connection.tune');
      client.send('connection.tune-ok', 0, { channelMax: 5, frameMax: 4096, heartbeat: 0 });
      client.send('connection.open', 0, { virtualHost: '' });
      await client.waitFor('connection.open-ok');
      const connection = lastConnection();
      expect(connection.vhost.name).to.equal('/');
      expect(connection.channelMax).to.equal(5);
      expect(connection.frameMax).to.equal(4096);
    });

    And('frames after a connection.close in the same chunk are dropped', async () => {
      const { encodeMethod } = await import('@0dep/amqp-emulator');
      client.write(
        Buffer.concat([
          encodeMethod('connection.close', 0, { replyCode: 200, replyText: 'bye', classId: 0, methodId: 0 }),
          encodeMethod('channel.open', 1),
        ]),
      );
      const reply = await client.nextFrame();
      expect(reply.method).to.equal('connection.close-ok');
      expect(await client.closed).to.be.true;
      expect(client.frames).to.have.length(0);
    });
  });

  Scenario('heartbeats', () => {
    let slow, connection;
    after(async () => {
      mock.timers.reset();
      await slow.stop();
    });

    Given('a server with a 1 second heartbeat and mocked intervals', async () => {
      mock.timers.enable({ apis: ['setInterval'] });
      slow = await startServer({ heartbeat: 1 });
      rawClient(slow.server.port);
      await client.connect();
      await client.handshake({ heartbeat: 1 });
      connection = /** @type {import('@0dep/amqp-emulator').AmqpConnection} */ ([...slow.server.connections].at(-1));
    });

    When('a heartbeat interval passes without traffic', () => {
      mock.timers.tick(1000);
    });

    Then('the server sends a heartbeat frame', async () => {
      const frame = await client.nextFrame();
      expect(frame.type).to.equal(constants.FRAME_HEARTBEAT);
    });

    When('the client heartbeats before the next interval', async () => {
      client.sendFrame(constants.FRAME_HEARTBEAT, 0, Buffer.alloc(0));
      await tick();
      mock.timers.tick(1000);
    });

    Then('the connection stays open', () => {
      expect(connection.state).to.equal('running');
    });

    When('the client stays silent for two more intervals', () => {
      let timedOut = false;
      connection.on('heartbeat.timeout', () => (timedOut = true));
      mock.timers.tick(2000);
      expect(timedOut).to.be.true;
    });

    Then('the server drops the connection', async () => {
      expect(await client.closed).to.be.true;
      expect(slow.server.connections.has(connection)).to.be.false;
    });
  });

  Scenario('a socket error', () => {
    Given('a connected raw client', () => connectRaw());

    Then('the server connection emits socket.error and is removed', async () => {
      const connection = lastConnection();
      const errored = new Promise((resolve) => connection.on('socket.error', resolve));
      connection.socket.destroy(Object.assign(new Error('boom'), { code: 'EBOOM' }));
      expect(await errored).to.have.property('code', 'EBOOM');
      expect(ctx.server.connections.has(connection)).to.be.false;
    });
  });
});
