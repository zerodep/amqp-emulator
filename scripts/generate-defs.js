// Generates src/defs.js from the RabbitMQ AMQP 0-9-1 spec JSON.
// Usage: node scripts/generate-defs.js > src/defs.js
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const spec = JSON.parse(readFileSync(join(here, '..', 'spec', 'amqp-rabbitmq-0.9.1.json'), 'utf8'));

const domains = Object.fromEntries(spec.domains);

function camel(dashed) {
  const [first, ...rest] = dashed.split('-');
  return first + rest.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join('');
}

const out = [];
const w = (s = '') => out.push(s);

w('// Generated from spec/amqp-rabbitmq-0.9.1.json by scripts/generate-defs.js. Do not edit.');
w();
w('export const constants = {');
for (const c of spec.constants) {
  w(`  ${c.name.replace(/-/g, '_')}: ${c.value},`);
}
w('};');
w();
w('/** @type {Record<number, string>} */');
w('export const constantNames = {');
for (const c of spec.constants) {
  if (c.value >= 200) w(`  ${c.value}: '${c.name}',`);
}
w('};');
w();
w("/** @type {Record<import('types').AmqpMethodName, import('types').MethodDef>} */");
w('export const methods = {');
for (const clazz of spec.classes) {
  for (const method of clazz.methods) {
    const name = `${clazz.name}.${method.name}`;
    const id = (clazz.id << 16) | method.id;
    let bitRun = 0;
    for (const a of method.arguments) {
      bitRun = (a.type || domains[a.domain]) === 'bit' ? bitRun + 1 : 0;
      if (bitRun > 8) throw new Error(`${clazz.name}.${method.name} has more than 8 consecutive bits, the codec does not support that`);
    }
    const args = method.arguments.map((a) => {
      const type = a.type || domains[a.domain];
      return `['${camel(a.name)}', '${type}', ${JSON.stringify(a['default-value'] ?? null)}]`;
    });
    w(`  '${name}': {`);
    w(`    name: '${name}',`);
    w(`    id: ${id},`);
    w(`    classId: ${clazz.id},`);
    w(`    methodId: ${method.id},`);
    w(`    synchronous: ${!!method.synchronous},`);
    w(`    content: ${!!method.content},`);
    w(`    args: [${args.join(', ')}],`);
    w('  },');
  }
}
w('};');
w();
w("/** @type {Record<number, import('types').PropertiesDef>} */");
w('export const properties = {');
for (const clazz of spec.classes) {
  if (!clazz.properties?.length) continue;
  const fields = clazz.properties.map((p) => `['${camel(p.name)}', '${p.type || domains[p.domain]}']`);
  w(`  ${clazz.id}: { name: '${clazz.name}', classId: ${clazz.id}, fields: [${fields.join(', ')}] },`);
}
w('};');
w();
w("/** @type {Map<number, import('types').MethodDef>} */");
w('export const methodsById = new Map(Object.values(methods).map((m) => [m.id, m]));');

process.stdout.write(out.join('\n') + '\n');
