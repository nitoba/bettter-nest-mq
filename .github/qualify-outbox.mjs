import assert from 'node:assert/strict'
import { readFileSync, writeFileSync, existsSync } from 'node:fs'
const engine = 'src/engine/engine-session.ts'
let text = readFileSync(engine, 'utf8')
if (text.includes('const publisher = this.hasPublisher')) {
  const start = text.indexOf('      const publisher = this.hasPublisher')
  const end = text.indexOf('      this.runtime =', start)
  assert.ok(start > 0 && end > start)
  text = `${text.slice(0, start)}      // The layer is inert. Only hasPublisher controls acquisition/activation below.
      const publisher = outboxPublisherLayer(
        outboxBindings.map((binding) => binding.name),
        bindings.map((binding) => binding.name),
        this.outboxOptions
      )
${text.slice(end)}`
  writeFileSync(engine, text)
}
const tests = 'tests/unit/outbox-contracts.test.ts'
text = readFileSync(tests, 'utf8')
writeFileSync(tests, text.replace("expect(record.request.id).toBe('explicit-job')", "assert.equal(record.request.id, 'explicit-job')"))
const packages = 'scripts/test-package.ts'
text = readFileSync(packages, 'utf8')
if (existsSync('tests/package/consumer/outbox.ts') && !text.includes("'json-fidelity', 'outbox'")) {
  const previous = "['codec', 'postgres', 'execution', 'controls', 'json-fidelity']"
  assert.equal(text.split(previous).length, 2)
  writeFileSync(packages, text.replace(previous, "['codec', 'postgres', 'execution', 'controls', 'json-fidelity', 'outbox']"))
}
