// Generates src/amqp10/defs.js from the OASIS AMQP 1.0 core spec XML parts in spec/.
// Usage: node scripts/generate-amqp10-defs.js > src/amqp10/defs.js
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const parts = ['types', 'transport', 'messaging', 'security', 'transactions'];

/** @type {Record<string, string>} attribute string -> object */
function attrs(tag) {
  /** @type {Record<string, string>} */
  const result = {};
  for (const [, key, value] of tag.matchAll(/([\w-]+)="([^"]*)"/g)) result[key] = value;
  return result;
}

function camel(dashed) {
  const [first, ...rest] = dashed.split('-');
  return first + rest.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('');
}

/** @type {Array<{ name: string, source: string, provides: string[], descriptor: number, symbol: string, fields: Array<{ name: string, type: string, mandatory: boolean, multiple: boolean, requires?: string }> }>} */
const composites = [];
/** @type {Array<{ name: string, source: string, choices: Array<[string, string]>, descriptor?: number, symbol?: string }>} */
const restricted = [];
/** @type {Array<{ name: string, encodings: Array<{ name: string, code: number, category: string, width: number }> }>} */
const primitives = [];

for (const part of parts) {
  const xml = readFileSync(join(here, '..', 'spec', `amqp-core-${part}-v1.0.xml`), 'utf8');
  // types are either <type ...>...</type> or self closing <type ... />
  for (const [, selfClosing, open, body] of xml.matchAll(/<type ([^>]*)\/>|<type ([^>]*)>([\s\S]*?)<\/type>/g)) {
    const type = attrs(selfClosing ?? open);
    if (selfClosing) {
      if (type.class === 'restricted') restricted.push({ name: type.name, source: type.source, choices: [] });
      continue;
    }
    if (type.class === 'composite') {
      const descriptor = attrs(body.match(/<descriptor [^>]*\/>/)[0]);
      const [domain, id] = descriptor.code.split(':').map((n) => Number.parseInt(n, 16));
      const fields = [...body.matchAll(/<field ([^>]*)\/?>/g)].map(([, f]) => {
        const field = attrs(f);
        return {
          name: camel(field.name),
          type: field.type,
          mandatory: field.mandatory === 'true',
          multiple: field.multiple === 'true',
          ...(field.requires ? { requires: field.requires } : {}),
        };
      });
      composites.push({
        name: type.name,
        source: type.source,
        provides: type.provides ? type.provides.split(',').map((s) => s.trim()) : [],
        descriptor: domain * 2 ** 32 + id,
        symbol: descriptor.name,
        fields,
      });
    } else if (type.class === 'restricted') {
      const choices = [...body.matchAll(/<choice ([^>]*)\/>/g)].map(([, c]) => {
        const choice = attrs(c);
        return /** @type {[string, string]} */ ([choice.name, choice.value]);
      });
      const described = body.match(/<descriptor [^>]*\/>/);
      /** @type {(typeof restricted)[number]} */
      const entry = { name: type.name, source: type.source, choices };
      if (described) {
        const descriptor = attrs(described[0]);
        const [domain, id] = descriptor.code.split(':').map((n) => Number.parseInt(n, 16));
        entry.descriptor = domain * 2 ** 32 + id;
        entry.symbol = descriptor.name;
      }
      restricted.push(entry);
    } else if (type.class === 'primitive') {
      const encodings = [...body.matchAll(/<encoding ([^>]*)\/>/g)].map(([, e]) => {
        const enc = attrs(e);
        return { name: enc.name || type.name, code: Number.parseInt(enc.code, 16), category: enc.category, width: Number(enc.width || 0) };
      });
      primitives.push({ name: type.name, encodings });
    }
  }
}

const out = [];
const w = (s = '') => out.push(s);

w('// Generated from spec/amqp-core-*-v1.0.xml by scripts/generate-amqp10-defs.js. Do not edit.');
w();
w("/** @type {Record<string, import('types').Amqp10Composite>} composite (described list) types by name */");
w('export const composites = {');
for (const c of composites) {
  w(`  '${c.name}': {`);
  w(`    name: '${c.name}',`);
  w(`    descriptor: ${c.descriptor},`);
  w(`    symbol: '${c.symbol}',`);
  w(`    source: '${c.source}',`);
  w(`    provides: ${JSON.stringify(c.provides)},`);
  w(`    fields: [`);
  for (const f of c.fields) w(`      ${JSON.stringify(f)},`);
  w('    ],');
  w('  },');
}
w('};');
w();
w("/** @type {Map<number, import('types').Amqp10Composite>} */");
w('export const compositesByDescriptor = new Map(Object.values(composites).map((c) => [c.descriptor, c]));');
w();
w("/** @type {Record<string, import('types').Amqp10Restricted>} restricted types by name, source is the underlying primitive */");
w('export const restricted = {');
for (const r of restricted) {
  const described = r.descriptor !== undefined ? `, descriptor: ${r.descriptor}, symbol: '${r.symbol}'` : '';
  w(`  '${r.name}': { name: '${r.name}', source: '${r.source}', choices: ${JSON.stringify(r.choices)}${described} },`);
}
w('};');
w();
w("/** @type {Map<number, import('types').Amqp10Restricted>} restricted types carrying a descriptor, i.e. message sections */");
w(
  'export const restrictedByDescriptor = new Map(Object.values(restricted).filter((r) => r.descriptor !== undefined).map((r) => [r.descriptor, r]));',
);
w();
w("/** @type {Record<string, import('types').Amqp10Primitive>} primitive types with their wire encodings */");
w('export const primitives = {');
for (const p of primitives) {
  w(`  '${p.name}': { name: '${p.name}', encodings: ${JSON.stringify(p.encodings)} },`);
}
w('};');

process.stdout.write(out.join('\n') + '\n');
