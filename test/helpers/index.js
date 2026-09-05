import amqplib from 'amqplib';

import { AmqpServer } from '@0dep/amqp-emulator';

/**
 * Start an emulator on a random port and track clients so tests can tear everything down
 * @param {import('@0dep/amqp-emulator').ServerOptions} [options]
 */
export async function startServer(options) {
  const server = new AmqpServer({ heartbeat: 0, ...options });
  const errors = [];
  server.on('error', (err) => errors.push(err));
  await server.listen();
  const connections = [];

  return {
    server,
    errors,
    get url() {
      return server.url;
    },
    /**
     * @param {string} [url]
     * @param {object} [socketOptions]
     */
    async connect(url = server.url, socketOptions) {
      const connection = await amqplib.connect(url, socketOptions);
      connection.on('error', () => {});
      connection.once('close', () => {
        const idx = connections.indexOf(connection);
        if (idx > -1) connections.splice(idx, 1);
      });
      connections.push(connection);
      return connection;
    },
    async stop() {
      for (const connection of connections.splice(0)) {
        await Promise.race([connection.close().catch(() => {}), tick(200)]);
      }
      await server.close();
    },
  };
}

/**
 * Resolve with the next event emitted
 * @param {import('node:events').EventEmitter} emitter
 * @param {string} eventName
 */
export function once(emitter, eventName) {
  return new Promise((resolve) => emitter.once(eventName, (...args) => resolve(args.length > 1 ? args : args[0])));
}

/**
 * Consume `count` messages from a queue, cancel the consumer and resolve with the messages.
 * Messages are left unacked unless consumed with noAck.
 * @param {import('amqplib').Channel} channel
 * @param {string} queue
 * @param {number} [count]
 * @param {import('amqplib').Options.Consume} [options]
 * @returns {Promise<import('amqplib').ConsumeMessage[]>}
 */
export function consumeMessages(channel, queue, count = 1, options) {
  /** @type {import('amqplib').ConsumeMessage[]} */
  const messages = [];
  return new Promise((resolve, reject) => {
    let consumerTag;
    let done = false;
    channel
      .consume(
        queue,
        (message) => {
          if (!message) return;
          // deliveries already in flight after the count was reached go back to the queue
          if (done) return options?.noAck ? undefined : channel.nack(message, false, true);
          messages.push(message);
          if (messages.length < count) return;
          done = true;
          const finish = () => resolve(messages);
          if (consumerTag) channel.cancel(consumerTag).then(finish, reject);
          else finish();
        },
        options,
      )
      .then((ok) => {
        consumerTag = ok.consumerTag;
        if (done) channel.cancel(consumerTag).then(() => resolve(messages), reject);
      })
      .catch(reject);
  });
}

/** Wait for the event loop to settle */
export function tick(ms = 10) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
