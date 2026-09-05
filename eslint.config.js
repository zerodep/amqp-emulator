import js from '@eslint/js';
import globals from 'globals';

const rules = {
  'dot-notation': [2, { allowKeywords: true }],
  'eol-last': 2,
  eqeqeq: 2,
  'no-console': 2,
  'no-eval': 2,
  'no-fallthrough': 'off',
  'no-implied-eval': 2,
  'no-multiple-empty-lines': ['error', { max: 1, maxEOF: 0, maxBOF: 0 }],
  'no-prototype-builtins': 2,
  'no-return-assign': 2,
  'no-trailing-spaces': 2,
  'no-undef': 2,
  'no-unused-vars': ['error', { ignoreRestSiblings: true }],
  'no-var': 2,
  'prefer-arrow-callback': ['error', { allowNamedFunctions: true }],
  'prefer-const': ['error', { destructuring: 'all' }],
  'require-await': 2,
  semi: [2, 'always'],
  yoda: [2, 'never'],
};

export default [
  js.configs.recommended,
  { rules },
  {
    files: ['**/*.js', '**/*.cjs'],
    languageOptions: {
      ecmaVersion: 2025,
      sourceType: 'module',
      globals: { ...globals.node, ...globals.es2021 },
    },
  },
  {
    files: ['**/*.cjs'],
    languageOptions: { sourceType: 'commonjs' },
  },
  {
    files: ['test/**/*.js'],
    languageOptions: {
      globals: {
        ...globals.mocha,
        expect: 'readonly',
        Feature: 'readonly',
        Scenario: 'readonly',
        Given: 'readonly',
        When: 'readonly',
        Then: 'readonly',
        And: 'readonly',
        But: 'readonly',
        beforeEachFeature: 'readonly',
        afterEachFeature: 'readonly',
        beforeEachScenario: 'readonly',
        afterEachScenario: 'readonly',
      },
    },
    rules: { 'no-unused-expressions': 0 },
  },
  {
    ignores: ['coverage/**/*', 'node_modules/**/*', 'tmp/*', 'src/defs.js'],
  },
];
