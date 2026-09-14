import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, changes) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of changes) {
    assert.equal(text.split(before).length - 1, 1, `Expected one anchor in ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('tests/package/consumer/outbox-retry.ts', [
  [
    'const Value = z.union([z.string(), z.null(), z.object({ text: z.string() })])',
    `// Test-only switch simulates a destination schema changed after a publication failed.
let rejectRevalidation = false
const Value = z.union([z.string(), z.null(), z.object({ text: z.string() })]).refine(
  (value) => !(rejectRevalidation && value === 'revalidate')
)`
  ],
  ["'corrupt', 'draining', 'delayed']", "'corrupt', 'draining', 'delayed', 'already-enqueued']"],
  [
    "        const job = await queue.echo.prepare(inputs[index] ?? 'control', {",
    "        const input = inputs[index]\n        const payload = input === undefined ? (id === 'corrupt' ? 'revalidate' : 'control') : input\n        const job = await queue.echo.prepare(payload, {"
  ],
  [
    `        // Preserve an actual null rather than replacing it with the control value.
        const prepared =
          index === 1
            ? await queue.echo.prepare(null, {
                jobId: \`job-\${id}\`,
                retry: { attempts: 4, backoff: { type: 'fixed', delayMs: 10 } }
              })
            : job`,
    `        if (id === 'already-enqueued') {
          // A prior attempt could have reached the target without an acknowledged source.
          await queue.echo.enqueue(payload, { jobId: \`job-\${id}\`, retry: { attempts: 4, backoff: { type: 'fixed', delayMs: 10 } } })
        }`
  ],
  ['{ id, job: prepared, attempts: 2 }', '{ id, job, attempts: 2 }'],
  [
    '      assert.equal(jobsBefore, 0)',
    "      assert.equal(jobsBefore, 1, 'The uncertain-publication case already exists at its target')"
  ],
  [
    "      const corrupted = await record(service, 'corrupt')",
    `      const existing = await record(service, 'already-enqueued')
      accepted.push(await service.retryFailed('source', existing.id, { expected: expected(existing), attempts: 1 }))
      const corrupted = await record(service, 'corrupt')
      rejectRevalidation = true
      try {
        await assert.rejects(service.retryFailed('source', corrupted.id, { expected: expected(corrupted), attempts: 1 }))
        assert.deepEqual(await record(service, corrupted.id), corrupted, 'A newly incompatible schema must reject before changing the record')
      } finally { rejectRevalidation = false }`
  ],
  [
    "      assert.equal((await record(service, 'corrupt')).state, 'failed')",
    `      // The native reader correctly rejects a digest that no longer matches the
      // tampered request. Inspect raw state instead of requiring a corrupt read to succeed.
      await assert.rejects(service.get('source', 'corrupt'), MqOutboxException)
      const invalid = await admin.query<{ state: string; attempts_max: number }>(\`SELECT state, attempts_max FROM "\${schema}".better_effect_mq_outbox WHERE id='corrupt'\`)
      assert.equal(invalid.rows[0]?.state, 'failed')
      assert.equal(invalid.rows[0]?.attempts_max, corrupted.attemptsMax)`
  ],
  [
    '      for (const [index, item] of accepted.entries()) {',
    '      for (const item of accepted) {'
  ],
  [
    "        assert.deepEqual(result, index < inputs.length ? inputs[index] : 'control')",
    '        assert.deepEqual(result, item.request.payload)'
  ],
  [
    'PASS post-restart publication and processing preserve job IDs/JSON/dispatch keys without rerunning business writes',
    'PASS post-restart recovery preserves IDs/JSON/dispatch keys, reuses an existing target job and never reruns business writes'
  ]
])
patch('README.md', [
  [
    'event-assisted result waits and managed Kysely outbox queries are implemented.',
    'event-assisted result waits, managed Kysely outbox queries and guarded failed-publication recovery are implemented.'
  ],
  [
    'See [Kysely outbox transactions]',
    'See [failed outbox recovery](docs/outbox-recovery.md), [Kysely outbox transactions]'
  ],
  [
    '## Kysely outbox transactions',
    `## Recover failed outbox publications

Inject MqOutboxService and inspect a failed record before calling \`retryFailed(source, id, { expected: { updatedAtMs, attemptsMade, attemptsMax }, attempts })\`. The expected values protect against stale/concurrent administration. PostgreSQL requeues only an unchanged failed record; pending, active and published rows are not reset.

Recovery preserves the original prepared job ID, payload, target, dispatch key, last failure and lifetime attempt counter. It grants an explicit number of additional publication attempts, not job-execution attempts, and never reruns the business transaction. Optional runAtMs delays publication. The existing publisher performs forwarding, and admitted administrative SQL drains before store shutdown. See [docs/outbox-recovery.md](docs/outbox-recovery.md) for authorization, uncertain-write and at-least-once boundaries.

## Kysely outbox transactions`
  ]
])
patch('docs/roadmap.md', [
  [
    'event-assisted result waits and managed Kysely outbox queries** are implemented.',
    'event-assisted result waits, managed Kysely outbox queries and guarded failed-outbox recovery** are implemented.'
  ],
  [
    'docs/event-waits.md and docs/kysely-outbox.md.',
    'docs/event-waits.md, docs/kysely-outbox.md and docs/outbox-recovery.md.'
  ],
  [
    `## M6c — External ORM enrollment and administration — pending

Enrollment of caller-owned Kysely transactions, TypeORM/Prisma bridges, other adapters, failed-record reset/retry and richer recovery administration remain separate work. Preserve verified transaction identity, uncertain-commit semantics and borrowed ownership. A managed Kysely callback does not prove ownership of an arbitrary external transaction. Application outbox remains distinct from flow coordination.`,
    `## M6c — Guarded failed-publication recovery — implemented

MqOutboxService.retryFailed requires an inspected version/counter guard and an explicit new publication budget. PostgreSQL performs a conditional failed-to-pending transition that compares the complete original request, routing and failure state. It never steals an active lease or resets published records, preserves cumulative attempts and the last failure, and revalidates the registered destination schema and dispatch key. Retry does not rerun business SQL or alter the job's execution budget. Admitted administrative writes drain before resources close. See outbox-recovery.md.

Tests cover concurrent administrators, stale guards, old publisher tokens, corrupt and newly incompatible payloads, delayed eligibility, an already-enqueued target job, shutdown with a blocked UPDATE and actual publication/processing after restart. The current capability is PostgreSQL-only and requires no schema or dependency change.

## M6d — External ORM enrollment and further administration — pending

Enrollment of caller-owned Kysely transactions, TypeORM/Prisma bridges, other adapters, audit history, pruning and bulk administration remain separate work. Preserve verified transaction identity, uncertain-commit semantics and borrowed ownership. A managed Kysely callback does not prove ownership of an arbitrary external transaction. Application outbox remains distinct from flow coordination.`
  ],
  [
    'M6a native outbox + M6b managed Kysely implemented; external ORM enrollment pending',
    'M6a native outbox + M6b Kysely + M6c recovery implemented; external ORM enrollment pending'
  ]
])
patch('docs/outbox.md', [
  [
    'A failed record is visible through diagnostics; this first slice does not export an automatic reset/retry-failed administration method.',
    'Failed records can be inspected and recovered explicitly with MqOutboxService.retryFailed; see outbox-recovery.md for version guards, preserved attempts and required destination validation. Recovery is never automatic.'
  ],
  [
    'configuration, preparation, availability, read, append, conflict and transaction misuse',
    'configuration, preparation, availability, read, append, conflict, retry and transaction misuse'
  ],
  [
    'The native PostgreSQL transaction boundary is the first outbox implementation. ORM transaction bridges, additional drivers, richer administrative recovery controls, flows and persistent schedules remain separate work.',
    'Native PostgreSQL outbox, managed Kysely queries, guarded failed-publication retry, flows and persistent schedules are implemented. Caller-owned ORM transaction enrollment, additional drivers, audit history and bulk administration remain separate work.'
  ],
  [
    'This does not add flows or external ORM transactions.',
    'Schedules remain distinct from the separately implemented flows and do not enroll external ORM transactions.'
  ]
])
patch('CHANGELOG.md', [
  [
    '## Unreleased',
    `## Unreleased

### Guarded failed-outbox recovery

- Add MqOutboxService.retryFailed with required expected-state/counter guards and an explicit publication attempt budget.
- Requeue only unchanged failed PostgreSQL records; preserve prepared requests, target IDs, dispatch keys, last failures and lifetime attempts.
- Revalidate destination contracts before retry, reject old publisher leases and drain admitted recovery SQL before resource disposal.
- Add native lease/concurrency regressions and installed PostgreSQL tests for publisher exhaustion, rollback-free recovery, stale admins, schema drift, corrupted records, delayed eligibility and target deduplication.
- Keep dependency versions, stored schemas and consumer peer requirements unchanged. No automatic retry scanner or npm publication.
`
  ]
])
const agents = readFileSync('AGENTS.md', 'utf8')
assert.ok(!agents.includes('## Failed-publication recovery'))
staged.set(
  'AGENTS.md',
  agents +
    `\n\n## Failed-publication recovery\n\nRead docs/outbox-recovery.md before changing retry administration. retryFailed is a guarded failed-to-pending PostgreSQL transition, not an active lease takeover or a domain transaction replay. Preserve the stable prepared request, dispatch key, target, creation time, cumulative attempts and last failure. The new attempt budget belongs to publication only.\n\nRequire inspected expected fields, revalidate registered destination contracts and compare full persisted content atomically. Do not rely only on the upstream request digest. Unsupported adapters must reject; uncertain SQL results must not be replayed automatically. Drain admitted retry SQL before native store disposal and leave borrowed pools open. Keep real concurrent-administrator, old-lease, schema/digest, delayed, existing-target and shutdown regressions in the installed-package matrix.\n`
)
for (const [path, text] of staged) writeFileSync(path, text)
