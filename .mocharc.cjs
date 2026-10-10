module.exports = {
  ui: 'mocha-scenarios',
  ignore: ['test/helpers/**'],
  require: 'chai/register-expect.js',
  recursive: true,
  exit: true,
  timeout: 3000,
};
