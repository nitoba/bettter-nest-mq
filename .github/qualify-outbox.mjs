import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './outbox-public-types.mjs'

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
  writeFileSync(consumer, text)
}
