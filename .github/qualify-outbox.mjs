import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
import './outbox-public-types.mjs'

function replace(path, before, after) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(after)) return
  assert.equal(text.split(before).length - 1, 1, `Expected one anchor in ${path}: ${before}`)
  writeFileSync(path, text.replace(before, after))
}
replace('tests/package/consumer/outbox-safety.ts', 'assert.equal(record.failure?.retryable, true)', 'assert.equal(record.failure?.retryable, false)')
replace('docs/outbox.md', 'The final failure can retain retryable=true even though no budget remains.', 'After the budget is exhausted, the upstream publisher persists a terminal failure with retryable=false; attemptsMade equals attemptsMax.')
replace('docs/connections.md', 'The internal outbox package does not mean a Nest transactional-outbox facade is implemented.', 'The native transactional-outbox facade is enabled explicitly with outbox:true on a connection; see outbox.md for managed transactions and independent publisher roles.')
replace('docs/connections.md', 'One better-effect runtime per configured Nest application owns named JobStores, Clock and lazy Worker supervisors.', 'One better-effect runtime per configured Nest application owns named JobStores, Clock, lazy Worker supervisors and explicitly enabled outbox stores/publisher.')
replace('docs/connections.md', 'Other storage wrappers, flow/schedule/event/outbox resource bundles and true ORM transaction bridges remain planned.', 'Native PostgreSQL outbox resources now share their existing pool and runtime. Other storage wrappers, flow/schedule/event bundles and true ORM transaction bridges remain planned.')
replace('docs/execution.md', 'transactional outbox/ORM transaction bridges, other storage wrappers', 'ORM transaction bridges, other storage wrappers')
replace('docs/execution.md', 'Distributed QueueControls are available as documented in controls.md.', 'Distributed QueueControls are available as documented in controls.md. Native PostgreSQL outbox transactions and managed publication are available as documented in outbox.md; publisher and worker enablement are independent.')
replace('CONTRIBUTING.md', 'Future flow, schedule and outbox APIs must be real integrations, never simulated methods.', 'Native PostgreSQL transactional outbox is implemented in docs/outbox.md. Future flows, schedules and ORM transaction bridges must be real integrations, never simulated methods.')
replace('CONTRIBUTING.md', 'one runtime shared by stores and workers.', 'one runtime shared by stores, workers and the opt-in outbox publisher. Domain queries and outbox appends must use one actual transaction client, preserving native parsers, rollback, caught-failure poisoning and late-handle rejection. Never automatically retry a business callback after a failed or uncertain commit.')
const controls = 'docs/controls.md'
let text = readFileSync(controls, 'utf8')
text = text.replace('additional adapters, flows, schedules and transactional outbox remain separate milestones.', 'additional adapters, flows, schedules and ORM transaction bridges remain separate milestones. Native PostgreSQL outbox transactions are documented in outbox.md.')
writeFileSync(controls, text)
const roadmap = 'docs/roadmap.md'
text = readFileSync(roadmap, 'utf8').replace('execution ,', 'execution,').replace('docs/execution.md ,', 'docs/execution.md,')
writeFileSync(roadmap, text)
