import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './outbox-public-types.mjs'
import './update-outbox-docs.mjs'

const consumer = 'tests/package/consumer/outbox.ts'
let text = readFileSync(consumer, 'utf8')
if (!text.includes('verifyOutboxSafety')) {
  text = `import { verifyOutboxSafety } from './outbox-safety.js'\n${text}`
  const anchor = 'if (connectionString !== undefined) await verifyOutbox(connectionString)'
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(anchor, `if (connectionString !== undefined) {
  await verifyOutbox(connectionString)
  await verifyOutboxSafety(connectionString)
}`)
}
if (!text.includes('class TargetEchoWorker')) {
  const target = "  @Process(TargetQueue, 'echo')"
  assert.equal(text.split(target).length, 2)
  text = text.replace(target, `}

@Injectable()
@Worker({ name: 'outbox-target-consumer', concurrency: 4, pollIntervalMs: 10 })
class TargetEchoWorker {
${target}`)
  assert.equal(text.split('providers: [EchoWorker]').length, 2)
  text = text.replace('providers: [EchoWorker]', 'providers: [EchoWorker, TargetEchoWorker]')
}
writeFileSync(consumer, text)
