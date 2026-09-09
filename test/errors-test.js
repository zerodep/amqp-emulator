import { AmqpError, constants } from 'amqp-emulator';

describe('AmqpError', () => {
  it('formats known reply codes with their name', () => {
    const err = new AmqpError(constants.NOT_FOUND, "NOT_FOUND - no queue 'q' in vhost '/'");
    expect(err.message).to.equal(`404 (NOT-FOUND) with message "NOT_FOUND - no queue 'q' in vhost '/'"`);
    expect(err.killConnection).to.be.false;
  });

  it('formats unknown reply codes as UNKNOWN', () => {
    const err = new AmqpError(999, 'custom', { connection: true });
    expect(err.message).to.equal('999 (UNKNOWN) with message "custom"');
    expect(err.killConnection).to.be.true;
  });
});
