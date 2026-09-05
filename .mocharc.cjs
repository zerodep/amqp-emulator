module.exports = {
  ui: 'mocha-cakes-2',
  spec: ['test/**/*-feature.js', 'test/**/*-test.js'],
  require: 'chai/register-expect.js',
  recursive: true,
  exit: true,
  timeout: 3000,
};
