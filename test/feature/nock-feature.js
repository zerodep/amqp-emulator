import nock from 'nock';

import { startServer, consumeMessages } from '../helpers/index.js';

// nock intercepts HTTP at the http.ClientRequest layer; the emulator speaks raw AMQP over
// TCP, which must stay untouched. This guards against a future nock release starting to
// intercept or block raw net sockets, which would silently break anyone whose test suite
// runs the emulator alongside nock with net connect disabled.
Feature('nock compatibility', () => {
  Scenario('the emulator round trips while nock is active with net connect disabled', () => {
    let ctx, connection, channel;
    after(async () => {
      await ctx?.stop();
      nock.cleanAll();
      nock.enableNetConnect();
      nock.restore();
    });

    Given('nock is active and forbids all real net connections', () => {
      nock.disableNetConnect();
      expect(nock.isActive()).to.be.true;
    });

    And('a running emulator', async () => {
      ctx = await startServer();
    });

    When('a client connects and publishes over raw TCP', async () => {
      connection = await ctx.connect();
      channel = await connection.createChannel();
      await channel.assertQueue('nock-q');
      channel.sendToQueue('nock-q', Buffer.from('through-nock'));
    });

    Then('the message is delivered, unaffected by nock', async () => {
      const [message] = await consumeMessages(channel, 'nock-q', 1, { noAck: true });
      expect(message.content.toString()).to.equal('through-nock');
    });
  });
});
