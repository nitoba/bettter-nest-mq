import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

// Final one-time corrections after the dependency guide was generated. Removed before merge.
function replace(path, before, after) {
  const source = readFileSync(path, 'utf8')
  if (!source.includes(before)) {
    assert.ok(source.includes(after), `Missing documentation anchor: ${path}`)
    return
  }
  assert.equal(source.split(before).length - 1, 1, `Ambiguous documentation anchor: ${path}`)
  writeFileSync(path, source.replace(before, after))
}

replace('README.md', 'bun add pg@^8.16.3 better-effect-mq-postgres@0.1.3 better-effect-mq-outbox@0.1.3', 'bun add pg@^8.16.3')
replace('README.md', 'Consumers of the optional PostgreSQL subpath install its peers alongside the local package tarball:', 'Consumers of the PostgreSQL subpath install only the selected native driver alongside the local package tarball; internal engine adapters install automatically:')
replace('docs/architecture.md', 'distributed-control APIs, custom retry providers, MQ enhancers/events', 'custom retry providers, MQ enhancers/events')
const commands = readFileSync('README.md', 'utf8').split('\n').filter((line) => line.startsWith('bun add '))
assert.ok(commands.length > 0)
for (const command of commands) assert.doesNotMatch(command, /better-effect|better-result/)
