import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './update-controls-docs.mjs'

const parent = 'tests/package/consumer/controls.ts'
let text = readFileSync(parent, 'utf8')
if (!text.includes('holdForRenewal = false')) {
  const signature = 'async function verifyDistributed(connectionString: string): Promise<void> {'
  assert.equal(text.split(signature).length, 2)
  text = text.replace(
    signature,
    'async function verifyDistributed(connectionString: string, holdForRenewal = false): Promise<void> {'
  )
  const start = text.indexOf('      // Prove renewal while')
  const end = text.indexOf('      const gRight =', start)
  assert.ok(start > 0 && end > start)
  text = `${text.slice(0, start)}      if (holdForRenewal) {
        // One cycle holds beyond the initial lease; the other cycles preserve the original fast path.
        await sleep(2_200)
        const renewed = await global.left.poll(gLeft)
        assert.equal(renewed?.state, 'active')
        assert.equal(renewed.deliveryCount, 1)
      }
${text.slice(end)}`
  text = text.replace(
    'await verifyDistributed(connectionString)',
    'await verifyDistributed(connectionString, cycle === 1)'
  )
  writeFileSync(parent, text)
}
const script = 'scripts/test-package.ts'
text = readFileSync(script, 'utf8')
if (!text.includes('verifyOrdinaryRecovery')) {
  text = `import { verifyOrdinaryRecovery } from '../tests/postgres/dispatch-recovery.ts'\n${text}`
  const anchor = "const temporary = await mkdtemp(join(tmpdir(), 'better-nest-mq-consumer-'))"
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(
    anchor,
    `const recoveryDatabase = process.env.MQ_TEST_DATABASE_URL
if (recoveryDatabase !== undefined) await verifyOrdinaryRecovery(recoveryDatabase)

${anchor}`
  )
  writeFileSync(script, text)
}
