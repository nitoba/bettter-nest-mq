import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

function update(path, marker, edit) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(marker)) return
  const updated = edit(text)
  assert.ok(updated.includes(marker), `Documentation marker missing for ${path}`)
  writeFileSync(path, updated)
}
function replaceOnce(text, before, after) {
  assert.equal(text.split(before).length - 1, 1, `Expected one documentation anchor: ${before}`)
  return text.replace(before, after)
}
update('README.md', '## Persistent schedules', (input) => {
  let text = replaceOnce(input, 'Core execution, distributed controls and the native PostgreSQL transactional outbox are implemented.', 'Core execution, distributed controls, native PostgreSQL transactional outbox and persistent schedules are implemented.')
  text = text.replaceAll('Flows, schedules, ORM transaction bridges', 'Flows, ORM transaction bridges')
  text = text.replaceAll('flows, schedules and ORM outbox bridges', 'flows and ORM outbox bridges')
  text = replaceOnce(text, 'See [transactional outbox]', 'See [persistent schedules](docs/schedules.md), [transactional outbox]')
  return replaceOnce(text, '## Transactional outbox', `## Persistent schedules

Declare recurring work on a job property with \`@Schedule({ key: 'daily', cron: '0 9 * * *', timeZone: 'America/Fortaleza', payload: { scope: 'all' } })\`, or use everyMs for an interval. Enable \`schedules: true\` on the PostgreSQL connection. The schedule store shares the existing pool, corrected JSON parser view and application runtime.

A coordinated deployment uses \`schedules: { mode: 'reconcile' }\`; normal replicas default to validation and do not overwrite missing/different definitions. Operator pauses, unchanged revisions and next-occurrence state survive reconciliation. \`execution.scheduler\` is independent of workers and outboxPublisher. Two independent schedulers coordinate each occurrence through PostgreSQL rather than duplicating enqueue calls.

Inject MqSchedulesService for typed upsert, get/list, pause/resume/remove and local scheduler status/sweep. Misfire/overlap rules follow the pinned engine protocol; catch-up is bounded per tick, and skip discards every due slot without a lateness threshold. Scheduled dispatch keys/per-key-limited destinations are explicitly rejected because this protocol version cannot persist those keys. Use an unkeyed coordinator job for keyed work. See [docs/schedules.md](docs/schedules.md) for complete examples and deployment semantics.

## Transactional outbox`)
})
update('docs/architecture.md', '## Implemented persistent scheduler', (input) => {
  let text = input.replaceAll('flows, schedules and ORM transaction bridges', 'flows and ORM transaction bridges')
  text = text.replace('Read outbox.md,', 'Read schedules.md, outbox.md,')
  return replaceOnce(text, 'Schedules will persist cron/interval/timezone/misfire/overlap decisions and reconcile safely during rolling deployments. Dynamic work belongs in coordinator jobs, not serialized functions or per-replica timers.', `## Implemented persistent scheduler

Schedule decorators and MqSchedulesService now compile JSON/schema/codec-validated definitions, provide explicit deployment validation/reconciliation and use the existing JobScheduler in the same runtime. PostgreSQL schedule records share the raw namespace, native pool and private parser view. Dynamic work belongs in coordinator jobs, not serialized functions or timer-per-replica enqueue calls.

Default replica startup validates definitions; a coordinated reconcile writer preserves pauses/cursors and never removes omissions. The scheduler role is independent of workers/publisher and drains admitted ticks before resource release. Independent-process PostgreSQL tests verify fenced occurrences, actual JSON results, timezone, catch-up and overlap behavior. The pinned record has no dispatchKey: derived-key/per-key destinations are rejected. Its skip policy discards due slots without a grace threshold. See schedules.md for those explicit limits and administration safety.

## Remaining flow and transaction integration`)
})
update('AGENTS.md', '## Persistent scheduling rules', (input) => {
  let text = input.replace('Read README.md,', 'Read docs/schedules.md, README.md,')
  text = text.replaceAll('Flows, schedules, ORM transaction bridges', 'Flows, ORM transaction bridges')
  return text + `
## Persistent scheduling rules

Schedule decorators, schedule administration and PostgreSQL JobScheduler integration are implemented. Preserve the raw connection namespace, same-pool JSON parser view and single runtime. Validate definitions and static payloads before consumers start; normal replicas are read-only and deployment reconciliation must preserve pauses, cursors and omissions.

Never replace atomic upstream tick/occurrence fencing with local timers. Keep scheduler/worker/outbox roles independent and drain admitted ticks before resources close. The pinned protocol cannot persist dispatch keys: reject derived-key/per-key destinations instead of silently losing keys. Catch-up is capped at 256 per tick; skip drops every observed due slot with no lateness tolerance. Do not advertise different semantics.

Qualify schedules through installed tarballs with independent PostgreSQL scheduler processes under both consumer compilers/runtimes. Keep tests for scalar/null/date payloads, drift, paused definitions, invalid preflight, unsafe timer configuration and shutdown. Do not rerun one-shot source patch scripts from formatter workflows; remove temporary development helpers before integration.
`
})
update('CONTRIBUTING.md', '## Schedule contributions', (input) => input + `
## Schedule contributions

Persistent scheduling is available; read docs/schedules.md before changing recurrence or administration. The PostgreSQL package fixture starts two independent scheduler processes and validates occurrence fencing, JSON fidelity, timezone/misfire/overlap, operator pause and later worker execution. Maintain these tests with the existing outbox/controls matrix.

Schema migrations remain explicit. Normal startup validates definitions; reconcile is coordinated deployment authority, not an automatic multi-writer election. Preserve pause/revision/cursor state and drain ticks on shutdown. Per-key scheduled dispatch and arbitrary dynamic functions are not supported by the pinned protocol. Flows and external ORM transaction bridges remain planned.
`)
update('CHANGELOG.md', '### Persistent schedules', (input) => {
  let text = input.replace('Still pending: flows, schedules, transactional outbox,', 'Still pending: flows, external ORM transaction bridges,')
  return replaceOnce(text, '## Unreleased', `## Unreleased

### Persistent schedules

- Add repeatable Schedule decorators, cron/interval/timezone policies and schema/codec-validated static payloads.
- Add MqSchedulesService with typed upsert, contract-scoped get/list/pause/resume/remove, reconcile reports and local scheduler status/sweep.
- Reuse upstream fenced occurrences and PostgreSQL schedule storage in the existing runtime/pool/parser boundary.
- Separate scheduler, worker and outbox roles; preserve operator pauses, unchanged revisions, cursors and omitted definitions.
- Validate declarations before resource writes and reject unsupported dispatch-key/per-key destinations, invalid options and unsafe timer delays explicitly.
- Add independent-process installed PostgreSQL tests for JSON/null/date values, occurrence deduplication, timezone, misfire/overlap and post-scheduler worker execution.
- Add startup-drift/rollback and in-flight tick shutdown regressions. No dependency version, migration envelope or required consumer peer changes.
`)
})
for (const path of ['docs/contracts.md', 'docs/connections.md', 'docs/execution.md', 'docs/controls.md', 'docs/outbox.md']) {
  update(path, '## Persistent schedule integration', (text) => text
    .replaceAll('flows, schedules,', 'flows,')
    .replaceAll('Flows, schedules,', 'Flows,')
    .replaceAll('flows, schedules and', 'flows and')
    .replaceAll('flows and schedules remain', 'flows remain')
    + `
## Persistent schedule integration

Persistent Schedule declarations and MqSchedulesService are now implemented; see schedules.md for the supported API and exact recurrence semantics. Schedule stores opt in with postgres({ schedules: true }), sharing the existing pool, private JSON view and stable namespace. Definition deployment/validation and scheduler execution are independent from workers and the outbox publisher.

This does not add flows or external ORM transactions. The pinned schedule protocol cannot carry dispatch keys, so keyed/per-key-limited schedules reject explicitly. Normal startup preserves operator pauses and validates deployed definitions; explicit reconciliation is a coordinated administrative operation, not a cross-store transaction.
`)
}
