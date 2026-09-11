import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './update-controls-docs.mjs'
const test = 'tests/integration/queue-controls.test.ts'
let text = readFileSync(test, 'utf8')
if (!text.includes('controlReferenceStore')) {
  assert.equal(text.split('const store = MemoryJobStore.make()').length, 2)
  text = text.replace(
    'const store = MemoryJobStore.make()',
    'const store = controlReferenceStore()'
  )
  text = `import { controlReferenceStore } from '../fixtures/control-store.ts'\n${text}`
}
writeFileSync(test, text.replace('  MemoryJobStore,\n', ''))
const script = 'scripts/test-package.ts'
text = readFileSync(script, 'utf8')
if (!text.includes("'execution', 'controls'")) {
  assert.equal(text.split("['codec', 'postgres', 'execution']").length, 2)
  writeFileSync(
    script,
    text.replace(
      "['codec', 'postgres', 'execution']",
      "['codec', 'postgres', 'execution', 'controls']"
    )
  )
}
const parent = 'tests/package/consumer/controls.ts'
text = readFileSync(parent, 'utf8')
if (!text.includes('DISTRIBUTED FAILURE DIAGNOSTICS')) {
  const anchor = '  } finally {\n    await admin.query(`UPDATE "${schema}".gates SET opened=true`)'
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(
    anchor,
    `  } catch (cause) {
    console.error('DISTRIBUTED FAILURE DIAGNOSTICS', cause)
    const jobs = await admin.query(\`SELECT id,name,state,attempts_made,delivery_count,stalled_count,lease_owner,lease_expires_at_ms,updated_at_ms,last_settlement_token,last_settlement_outcome,failure FROM "\${schema}".better_effect_mq_jobs ORDER BY sequence\`)
    console.error('PERSISTED JOBS', JSON.stringify(jobs.rows))
    const attempts = await admin.query(\`SELECT job_id,attempt,delivery,outcome,worker_id,failure FROM "\${schema}".better_effect_mq_attempts ORDER BY job_id,attempt_sequence\`)
    console.error('PERSISTED ATTEMPTS', JSON.stringify(attempts.rows))
    const permits = await admin.query(\`SELECT * FROM "\${schema}".better_effect_mq_controlled_permits\`)
    console.error('PERSISTED PERMITS', JSON.stringify(permits.rows))
    const audit = await admin.query(\`SELECT * FROM "\${schema}".claim_audit\`)
    console.error('PERSISTED CLAIM AUDIT', JSON.stringify(audit.rows))
    throw cause
  } finally {
    await admin.query(\`UPDATE "\${schema}".gates SET opened=true\`)`
  )
  writeFileSync(parent, text)
}
