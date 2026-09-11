import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './update-controls-docs.mjs'
const test = 'tests/integration/queue-controls.test.ts'
let text = readFileSync(test, 'utf8')
if (!text.includes('controlReferenceStore')) {
  assert.equal(text.split('const store = MemoryJobStore.make()').length, 2)
  text = text.replace('const store = MemoryJobStore.make()', 'const store = controlReferenceStore()')
  text = `import { controlReferenceStore } from '../fixtures/control-store.ts'\n${text}`
}
writeFileSync(test, text.replace('  MemoryJobStore,\n', ''))
const script = 'scripts/test-package.ts'
text = readFileSync(script, 'utf8')
if (!text.includes("'execution', 'controls'")) {
  assert.equal(text.split("['codec', 'postgres', 'execution']").length, 2)
  writeFileSync(script, text.replace("['codec', 'postgres', 'execution']", "['codec', 'postgres', 'execution', 'controls']"))
}
const parent = 'tests/package/consumer/controls.ts'
text = readFileSync(parent, 'utf8')
assert.ok(text.includes('DISTRIBUTED FAILURE DIAGNOSTICS'))
if (!text.includes('held beyond its initial lease')) {
  const anchor = '      const gRight = await global.right.enqueueMany('
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(anchor, `      // Prove renewal while the job is held beyond its initial lease, not just fast completion.
      await sleep(2_200)
      const renewed = await global.left.poll(gLeft)
      assert.equal(renewed?.state, 'active')
      assert.equal(renewed.deliveryCount, 1)
      const gRight = await global.right.enqueueMany(`)
}
if (!text.includes('process.stderr.write(chunk)')) {
  text = text.replace("    this.child.stderr?.on('data', (chunk: Buffer) => {", "    this.child.stderr?.on('data', (chunk: Buffer) => {\n      process.stderr.write(chunk)")
}
if (!text.includes('cycle < 3')) {
  const anchor = 'if (connectionString !== undefined) await verifyDistributed(connectionString)'
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(anchor, 'if (connectionString !== undefined) {\n  for (let cycle = 0; cycle < 3; cycle += 1) await verifyDistributed(connectionString)\n}')
}
writeFileSync(parent, text)
const worker = 'tests/package/consumer/controls-worker.ts'
text = readFileSync(worker, 'utf8')
if (!text.includes('ATTEMPT ABORT DIAGNOSTIC')) {
  const anchor = '  async run(payload: { key: string; gate: string }, context: JobExecutionContext) {'
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(anchor, `${anchor}
    context.signal.addEventListener('abort', () => {
      console.error('ATTEMPT ABORT DIAGNOSTIC', context.jobId, context.delivery, context.signal.reason)
    }, { once: true })`)
}
writeFileSync(worker, text)
