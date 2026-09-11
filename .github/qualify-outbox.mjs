import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './outbox-public-types.mjs'
import './update-outbox-docs.mjs'

const safety = 'tests/package/consumer/outbox-safety.ts'
let text = readFileSync(safety, 'utf8')
if (!text.includes('record.attemptsMade, record.attemptsMax')) {
  const anchor = 'assert.equal(record.attemptsMade, 1)'
  assert.equal(text.split(anchor).length, 2)
  text = text.replace(
    anchor,
    `assert.equal(record.attemptsMade, record.attemptsMax)
        assert.equal(record.failure?.retryable, true)`
  )
  writeFileSync(safety, text)
}
const docs = 'docs/outbox.md'
text = readFileSync(docs, 'utf8')
if (!text.includes('## Identity scope and operational qualifications')) {
  text += `
## Identity scope and operational qualifications

A missing destination route is classified as retryable by the pinned upstream publisher. It can recover when a later deployment supplies that route; without recovery, the record becomes failed after exhausting its publication attempt budget. The final failure can retain retryable=true even though no budget remains. This differs from an invalid prepared request, which is not made valid by retrying. The installed-package test verifies the missing-route budget independently of job execution attempts.

Automatic proposed job IDs use the logical source name, outbox ID and logical destination name. Distinct source databases/namespaces with the same logical aliases are not globally distinguished by that derivation. Use globally unique outbox IDs, such as UUIDs, or explicit globally scoped job IDs when multiple independent source deployments converge on one destination. Duplicate append within one source does not deduplicate business callbacks across sources.

The pinned Worker supervisor requires queue/name/version uniqueness inside each Worker Service even across connections. When source and destination expose the same queue/job/version, register separate Worker Services for them. This is checked before resources open; the integration does not silently split a worker and change its local concurrency budget.
`
  writeFileSync(docs, text)
}
