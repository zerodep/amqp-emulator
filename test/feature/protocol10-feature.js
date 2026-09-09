import { encodeAmqp10 as encode, amqp10Typed as typed } from 'amqp-emulator';

import { startServer, tick } from '../helpers/index.js';
import { Raw10Client, AMQP_HEADER, SASL_HEADER } from '../helpers/raw10-client.js';

const AMQP091_HEADER = Buffer.from(`AMQP${String.fromCharCode(0, 0, 9, 1)}`, 'binary');

Feature('AMQP 1.0 protocol edge cases', () => {
  /** @type {Awaited<ReturnType<typeof startServer>>} */
  let ctx;
  /** @type {Raw10Client} */
  let client;

  before(async () => {
    ctx = await startServer();
  });
  after(() => ctx.stop());
  afterEachScenario(() => client?.destroy());

  /** @param {number} [port] */
  async function rawClient(port = ctx.server.port) {
    client = new Raw10Client(port);
    await client.connect();
    return client;
  }

  /** the most recently accepted 1.0 connection */
  function lastConnection() {
    return /** @type {import('amqp-emulator').Amqp10Connection} */ ([...ctx.server.connections].at(-1));
  }

  Scenario('SASL failures', () => {
    Given('a raw client', () => rawClient());

    Then('an unsupported mechanism gets outcome code 1 and the socket is closed', async () => {
      const outcome = await client.handshake({ mechanism: 'EXTERNAL' });
      expect(outcome.performative.code).to.equal(1);
      expect(await client.closed).to.be.true;
    });
  });

  Scenario('SASL plain without a response and empty sasl frames', () => {
    Given('a raw client that sent the SASL header in two pieces', async () => {
      await rawClient();
      client.expectHeaders++;
      client.write(SASL_HEADER.subarray(0, 4));
      await tick();
      client.write(SASL_HEADER.subarray(4));
      await client.waitFor('sasl-mechanisms');
    });

    Then('an empty sasl frame closes the connection with amqp:illegal-state', async () => {
      client.write(Buffer.from([0, 0, 0, 8, 2, 1, 0, 0]));
      expect(await client.closed).to.be.true;
    });

    And('PLAIN without an initial response is accepted as an unknown user when nothing authenticates', async () => {
      await rawClient();
      client.sendHeader(SASL_HEADER);
      await client.waitFor('sasl-mechanisms');
      client.sendSasl({ $type: 'sasl-init', mechanism: 'PLAIN' });
      const outcome = await client.waitFor('sasl-outcome');
      expect(outcome.performative.code).to.equal(0);
      client.sendHeader(AMQP_HEADER);
      client.send(0, { $type: 'open', containerId: 'raw10', channelMax: 5 });
      await client.waitFor('open');
      const connection = lastConnection();
      expect(connection.user).to.equal(null);
      expect(connection.channelMax).to.equal(5);
    });
  });

  Scenario('a sasl frame after sasl completed', () => {
    Given('a raw client that finished SASL and sent the AMQP header', async () => {
      await rawClient();
      client.sendHeader(SASL_HEADER);
      await client.waitFor('sasl-mechanisms');
      client.sendSasl({ $type: 'sasl-init', mechanism: 'ANONYMOUS' });
      await client.waitFor('sasl-outcome');
      client.sendHeader(AMQP_HEADER);
    });

    Then('another sasl-init closes the connection with amqp:illegal-state', async () => {
      client.sendSasl({ $type: 'sasl-init', mechanism: 'ANONYMOUS' });
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:illegal-state');
    });
  });

  Scenario('skipping SASL when authentication is required', () => {
    let secured;
    after(() => secured.stop());

    Given('a server with an authenticate callback', async () => {
      secured = await startServer({ authenticate: () => true });
      await rawClient(secured.server.port);
    });

    Then('a bare AMQP header is answered with the SASL header and the socket is closed', async () => {
      client.sendHeader(AMQP_HEADER);
      expect(await client.closed).to.be.true;
      expect(client.headers[0]).to.deep.equal(SASL_HEADER);
    });
  });

  Scenario('a wrong header after SASL', () => {
    Given('a raw client that finished SASL', async () => {
      await rawClient();
      client.sendHeader(SASL_HEADER);
      await client.waitFor('sasl-mechanisms');
      client.sendSasl({ $type: 'sasl-init', mechanism: 'ANONYMOUS' });
      await client.waitFor('sasl-outcome');
    });

    Then('a 0-9-1 header, even split in two, is answered with the AMQP 1.0 header and the socket is closed', async () => {
      client.expectHeaders++;
      client.write(AMQP091_HEADER.subarray(0, 3));
      await tick();
      client.write(AMQP091_HEADER.subarray(3));
      expect(await client.closed).to.be.true;
      expect(client.headers.at(-1)).to.deep.equal(AMQP_HEADER);
    });
  });

  Scenario('the connection is dropped during SASL', () => {
    Given('a raw client in the middle of SASL', async () => {
      await rawClient();
      client.sendHeader(SASL_HEADER);
      await client.waitFor('sasl-mechanisms');
    });

    Then('server.disconnect() destroys it without a close frame', async () => {
      ctx.server.disconnect();
      expect(await client.closed).to.be.true;
      expect(client.frames).to.have.length(0);
    });
  });

  Scenario('something other than open after the header', () => {
    Given('a raw client that sent the AMQP header', async () => {
      await rawClient();
      client.sendHeader(AMQP_HEADER);
    });

    Then('a begin closes the connection with amqp:illegal-state', async () => {
      client.send(0, { $type: 'begin', nextOutgoingId: 0, incomingWindow: 1, outgoingWindow: 1 });
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:illegal-state');
      expect(close.performative.error.description).to.match(/expected open, got begin/);
    });
  });

  Scenario('open without negotiation fields and heartbeats', () => {
    let limited;
    after(() => limited.stop());

    Given('a server without a channel limit and a raw client opening with the bare minimum', async () => {
      limited = await startServer({ channelMax: 0 });
      await rawClient(limited.server.port);
      await client.handshake({ sasl: false });
    });

    Then('the server keeps its own limits', () => {
      const connection = /** @type {import('amqp-emulator').Amqp10Connection} */ ([...limited.server.connections].at(-1));
      expect(connection.channelMax).to.equal(0xffff);
      expect(connection.maxFrameSize).to.equal(131072);
      expect(connection.supportsBlocked).to.be.false;
    });

    And('empty frames are ignored', async () => {
      client.send(0, null);
      await tick();
      expect(client.frames).to.have.length(0);
    });

    And('block and unblock do nothing for 1.0 clients', async () => {
      limited.server.block('nope');
      limited.server.unblock();
      await tick();
      expect(client.frames).to.have.length(0);
    });
  });

  Scenario('unknown performatives and sessions', () => {
    Given('an open raw client', async () => {
      await rawClient();
      await client.handshake();
    });

    Then('a flow on a channel without a session closes with amqp:not-found', async () => {
      client.send(4, { $type: 'flow', nextOutgoingId: 0, incomingWindow: 1, outgoingWindow: 1 });
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:not-found');
    });
  });

  Scenario('ending a session that does not exist', () => {
    Given('an open raw client', async () => {
      await rawClient();
      await client.handshake();
    });

    Then('end on an unknown channel closes with amqp:not-found', async () => {
      client.send(3, { $type: 'end' });
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:not-found');
    });
  });

  Scenario('a performative the emulator does not implement', () => {
    Given('an open raw client', async () => {
      await rawClient();
      await client.handshake();
    });

    Then('the connection closes with amqp:not-implemented', async () => {
      client.send(0, { $type: 'sasl-mechanisms', saslServerMechanisms: ['X'] });
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:not-implemented');
    });
  });

  Scenario('link frames on unknown handles and ignored frames', () => {
    Given('an open raw client with a session', async () => {
      await rawClient();
      await client.handshake();
      await client.begin();
    });

    Then('a flow without a handle and a disposition from a sender are ignored', async () => {
      client.send(0, { $type: 'flow', nextOutgoingId: 0, incomingWindow: 1, outgoingWindow: 1 });
      client.send(0, { $type: 'disposition', role: false, first: 1 });
      await tick();
      expect(client.frames).to.have.length(0);
    });

    And('a transfer on an unattached handle closes with amqp:session:unattached-handle', async () => {
      client.send(
        0,
        { $type: 'transfer', handle: 9, deliveryId: 1, deliveryTag: Buffer.from('t'), messageFormat: 0 },
        encode({ $type: 'amqp-value', value: 'x' }),
      );
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:session:unattached-handle');
    });
  });

  Scenario('attaching a sender without a target address', () => {
    Given('an open raw client with a session', async () => {
      await rawClient();
      await client.handshake();
      await client.begin();
    });

    Then('the link is attached with a null target and detached with amqp:not-found', async () => {
      const attach = await client.attach(0, { name: 'no-target', role: false, source: { $type: 'source' }, target: { $type: 'target' } });
      expect(attach.performative.target).to.be.undefined;
      const detach = await client.waitFor('detach');
      expect(detach.performative.error.condition).to.equal('amqp:not-found');
    });
  });

  Scenario('a transfer on a link where the client receives', () => {
    Given('an open raw client attached as receiver', async () => {
      await rawClient();
      await client.handshake();
      await client.begin();
      await client.attach(0, { name: 'recv', role: true, source: { $type: 'source', address: 'raw-q' }, target: { $type: 'target' } });
    });

    Then('sending a transfer on it closes with amqp:not-allowed', async () => {
      client.send(
        0,
        { $type: 'transfer', handle: 0, deliveryId: 1, deliveryTag: Buffer.from('t'), messageFormat: 0 },
        encode({ $type: 'amqp-value', value: 'x' }),
      );
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:not-allowed');
    });
  });

  Scenario('transfers that do not decode', () => {
    Given('an open raw client attached as sender', async () => {
      await rawClient();
      await client.handshake();
      await client.begin();
      await client.attach(0, { name: 'send', role: false, source: { $type: 'source' }, target: { $type: 'target', address: 'raw-q' } });
      await client.waitFor('flow');
    });

    Then('a payload that is not a message section closes with amqp:decode-error', async () => {
      client.send(0, { $type: 'transfer', handle: 0, deliveryId: 1, deliveryTag: Buffer.from('t') }, encode({ $type: 'source' }));
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:decode-error');
      expect(ctx.errors).to.have.length(0);
    });
  });

  Scenario('credit refill and settlement replies', () => {
    let queue;
    Given('an open raw client attached as sender with the initial credit', async () => {
      await rawClient();
      await client.handshake();
      await client.begin();
      await client.attach(0, { name: 'send', role: false, source: { $type: 'source' }, target: { $type: 'target', address: 'refill' } });
      const flow = await client.waitFor('flow');
      expect(flow.performative.linkCredit).to.equal(100);
      queue = ctx.server.getBroker().getQueue('refill');
    });

    Then('after fifty one transfers the server tops the credit up again', async () => {
      for (let i = 1; i <= 51; i++) {
        client.send(
          0,
          { $type: 'transfer', handle: 0, deliveryId: i, deliveryTag: Buffer.from(`${i}`), messageFormat: 0, settled: true },
          encode({ $type: 'amqp-value', value: `${i}` }),
        );
      }
      const flow = await client.waitFor('flow');
      expect(flow.performative.linkCredit).to.equal(100);
      expect(flow.performative.deliveryCount).to.equal(51);
      expect(queue.messageCount).to.equal(51);
    });

    And('a transfer without a body and one with a binary amqp-value are stored with buffer content', async () => {
      client.send(
        0,
        { $type: 'transfer', handle: 0, deliveryId: 52, deliveryTag: Buffer.from('52'), messageFormat: 0, settled: true },
        encode({ $type: 'properties', subject: 'empty' }),
      );
      client.send(
        0,
        { $type: 'transfer', handle: 0, deliveryId: 53, deliveryTag: Buffer.from('53'), messageFormat: 0, settled: true },
        encode({ $type: 'amqp-value', value: Buffer.from('bin') }),
      );
      await tick();
      const messages = queue.messages.slice(-2);
      expect(messages[0].content.length).to.equal(0);
      expect(messages[1].content.toString()).to.equal('bin');
    });

    And('an unsettled disposition from a receiver is answered with a settled one', async () => {
      await client.attach(0, {
        handle: 1,
        name: 'recv',
        role: true,
        source: { $type: 'source', address: 'refill' },
        target: { $type: 'target' },
      });
      client.send(0, {
        $type: 'flow',
        nextOutgoingId: 0,
        incomingWindow: 2048,
        outgoingWindow: 2048,
        handle: 1,
        deliveryCount: 0,
        linkCredit: 1,
      });
      const transfer = await client.waitFor('transfer');
      client.send(0, {
        $type: 'disposition',
        role: true,
        first: transfer.performative.deliveryId,
        settled: false,
        state: { $type: 'accepted' },
      });
      const reply = await client.waitFor('disposition');
      expect(reply.performative).to.include({ role: false, first: transfer.performative.deliveryId, settled: true });
      expect(queue.messageCount).to.equal(52);
    });

    And('a disposition without a state counts as accepted', async () => {
      client.send(0, {
        $type: 'flow',
        nextOutgoingId: 0,
        incomingWindow: 2048,
        outgoingWindow: 2048,
        handle: 1,
        deliveryCount: 1,
        linkCredit: 1,
      });
      const transfer = await client.waitFor('transfer');
      client.send(0, { $type: 'disposition', role: true, first: transfer.performative.deliveryId, settled: true });
      await tick();
      expect(queue.messageCount).to.equal(51);
    });

    And('a flow on the sending link and a flow without counts are harmless', async () => {
      client.send(0, { $type: 'flow', nextOutgoingId: 0, incomingWindow: 2048, outgoingWindow: 2048, handle: 0, linkCredit: 3 });
      client.send(0, { $type: 'flow', nextOutgoingId: 0, incomingWindow: 2048, outgoingWindow: 2048, handle: 1 });
      await tick();
      expect(client.frames).to.have.length(0);
    });

    And('a transfer after the queue was deleted recreates it', async () => {
      ctx.server.getBroker().deleteQueue('refill');
      client.send(
        0,
        { $type: 'transfer', handle: 0, deliveryId: 54, deliveryTag: Buffer.from('54'), messageFormat: 0, settled: true },
        encode({ $type: 'properties', userId: typed('string', 'u') }),
      );
      await tick();
      const recreated = ctx.server.getBroker().getQueue('refill');
      expect(recreated.messageCount).to.equal(1);
      expect(recreated.messages[0].properties.userId).to.equal('u');
    });

    And('a flow with echo is answered and a disposition for an unknown delivery is ignored', async () => {
      client.send(0, {
        $type: 'flow',
        nextOutgoingId: 0,
        incomingWindow: 2048,
        outgoingWindow: 2048,
        handle: 1,
        deliveryCount: 1,
        linkCredit: 0,
        echo: true,
      });
      const flow = await client.waitFor('flow');
      expect(flow.performative.handle).to.equal(1);
      client.send(0, { $type: 'disposition', role: true, first: 999, settled: true, state: { $type: 'accepted' } });
      await tick();
      expect(client.frames).to.have.length(0);
    });
  });

  Scenario('unexpected internal errors', () => {
    Given('an open raw client with a session', async () => {
      await rawClient();
      await client.handshake();
      await client.begin();
    });

    Then('a link that fails to attach for an unexpected reason is detached with amqp:internal-error', async () => {
      const broker = ctx.server.getBroker();
      const original = broker.getQueue;
      broker.getQueue = () => {
        throw new Error('broker down');
      };
      await client.attach(0, { name: 'broken', role: true, source: { $type: 'source', address: 'any' }, target: { $type: 'target' } });
      broker.getQueue = original;
      const detach = await client.waitFor('detach');
      expect(detach.performative.error.condition).to.equal('amqp:internal-error');
    });

    And('an exception outside link setup closes the connection with amqp:internal-error and is reported', async () => {
      const connection = lastConnection();
      connection.sessions.get(0).handle = () => {
        throw new Error('session boom');
      };
      client.send(0, { $type: 'flow', nextOutgoingId: 0, incomingWindow: 1, outgoingWindow: 1, handle: 0 });
      const close = await client.waitFor('close');
      expect(close.performative.error.condition).to.equal('amqp:internal-error');
      expect(ctx.errors.pop()).to.have.property('message', 'session boom');
    });
  });

  Scenario('closing twice', () => {
    Given('an open raw client', async () => {
      await rawClient();
      await client.handshake();
    });

    Then('a client close after the server started closing ends the connection', async () => {
      const connection = lastConnection();
      ctx.server.disconnect('going away', 503);
      ctx.server.disconnect();
      const close = await client.waitFor('close');
      expect(close.performative.error).to.deep.equal({ $type: 'error', condition: 'amqp:internal-error', description: 'going away' });
      const { encodeAmqp10Frame } = await import('amqp-emulator');
      client.write(
        Buffer.concat([
          encodeAmqp10Frame(0, 0, { $type: 'close' }),
          encodeAmqp10Frame(0, 0, { $type: 'begin', nextOutgoingId: 0, incomingWindow: 1, outgoingWindow: 1 }),
        ]),
      );
      expect(await client.closed).to.be.true;
      expect(ctx.server.connections.has(connection)).to.be.false;
      connection.send(0, { $type: 'close' });
    });
  });
});
