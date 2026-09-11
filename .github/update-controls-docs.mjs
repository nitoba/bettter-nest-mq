import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

function replace(path, before, after) {
  const source = readFileSync(path, 'utf8')
  if (source.includes(after)) return
  assert.equal(
    source.split(before).length - 1,
    1,
    `Documentation anchor not found exactly once: ${path}: ${before}`
  )
  writeFileSync(path, source.replace(before, after))
}

replace(
  'README.md',
  'Status: M3 core execution is implemented.',
  'Status: M3 core execution and M3.1a distributed controls are implemented.'
)
replace(
  'README.md',
  'See [execution](docs/execution.md)',
  'See [distributed controls](docs/controls.md), [execution](docs/execution.md)'
)
replace(
  'README.md',
  'Named custom retry providers, distributed-control decorators, durable events',
  'Named custom retry providers, durable events'
)
replace(
  'README.md',
  '## Declare a queue and a worker',
  `## Distributed controls

Queues can now declare limits shared by workers across processes:

\`\`\`ts
@QueueControls({
  globalConcurrency: 10,
  perKeyConcurrency: 2,
  rateLimit: { max: 100, durationMs: 1_000 }
})
\`\`\`

Import QueueControls from better-nest-mq and apply it to the same QueueService as @Queue. Jobs derive a typed dispatch key through \`this.job({ payload, result, dispatchKey: (value) => value.tenantId })\`. Per-key declarations require a key for every new publication; callers cannot override a derived key with a different value.

Ordinary applications default to read-only policy validation. Apply policies in one coordinated deployment process using \`controls: { mode: 'reconcile', group: 'your-deployment' }\`; replicas use validate mode and the same group. Missing/different policies fail startup before consumers start. Reconciliation never disables omitted queues and unchanged policies retain their revision. This is not a cross-database deployment transaction or a leader-election protocol.

Global/per-key permits and fixed-window admissions use the existing controlled-store protocol. Local Worker/Process concurrency remains a separate bound. See the complete syntax, deployment example, guarantees and limitations in [docs/controls.md](docs/controls.md).

## Declare a queue and a worker`
)

replace(
  'docs/execution.md',
  'Global/per-key concurrency, rate-limit decorators and runtime policy providers remain the next extension; no local semaphore is advertised as distributed coordination.',
  'Global/per-key concurrency and rate-limit declarations are implemented through storage-backed QueueControls; see controls.md for explicit policy deployment and validation. Runtime custom retry providers remain a separate extension. Local limits are never presented as distributed coordination.'
)
replace(
  'docs/execution.md',
  'other storage wrappers, distributed control APIs and durable event subscriptions remain separate milestones.',
  'other storage wrappers and durable event subscriptions remain separate milestones. Distributed QueueControls are available as documented in controls.md.'
)
replace(
  'docs/connections.md',
  'distributed-control APIs remain a planned extension.',
  'QueueControls now exposes global/per-key concurrency and rate-limit declarations through the controlled-store protocol; see controls.md for qualification and deployment rules.'
)
replace(
  'docs/architecture.md',
  'distributed-control APIs, custom retry providers, MQ enhancers/events',
  'custom retry providers, MQ enhancers/events'
)
replace(
  'docs/architecture.md',
  'Distributed controls must use storage-backed coordination and multiple-replica tests, not local semaphores.',
  'Distributed controls are implemented through QueueControls metadata and an internal protocol-dispatch view over the existing raw store. Global/per-key concurrency and fixed-window admission call the upstream controlled operations; no local semaphore or new lease algorithm is substituted. The raw persistence token stays stable. Replica startup validates persisted policy read-only; explicit coordinated deployment reconciles without disabling omissions. See controls.md for group checks, partial multi-store deployments, typed dispatch keys and separate-process PostgreSQL qualification.'
)
replace(
  'AGENTS.md',
  'Read README.md and docs/contracts.md,',
  'Read docs/controls.md, README.md and docs/contracts.md,'
)
replace(
  'AGENTS.md',
  'M3 core producer/worker execution are implemented.',
  'M3 core producer/worker execution plus M3.1a distributed controls are implemented.'
)
replace(
  'AGENTS.md',
  '- Local concurrency is not distributed concurrency. Custom retry providers, distributed-control decorators and event-based waits must not be advertised before implementation.',
  `- Local concurrency is not distributed concurrency. QueueControls uses the upstream controlled-store claim/settlement/release/recovery protocol; never substitute process-local limits or plain claims for an enabled policy.
- Policy validation is read-only by default. Reconcile requires one coordinated deployment authority; it never disables omitted queues, silently overrides a foreign group or claims atomic cross-store rollback. Validate all adapter capabilities before policy writes and apply policy before workers start.
- Keep the raw nestjs/<name> persistence token stable; the operation-view token introduces no new database namespace or pool. Preserve typed decoded dispatch keys, reject conflicting overrides and require keys for per-key declarations at all publication boundaries.
- Custom retry providers and event-based waits remain pending. Test distributed guarantees with independent processes against PostgreSQL, not only the explicitly test-only shared memory reference fixture.`
)
replace(
  'CONTRIBUTING.md',
  'Future flow, schedule, outbox and distributed-control APIs must be real integrations, never simulated methods.',
  'Distributed QueueControls are implemented and documented in docs/controls.md. Future flow, schedule and outbox APIs must be real integrations, never simulated methods. Keep normal policy validation read-only and qualify shared limits with actual independent PostgreSQL worker processes.'
)
replace(
  'CHANGELOG.md',
  '## Unreleased',
  `## Unreleased

### M3.1a — Distributed controls

- Add QueueControls declarations for global concurrency, per-dispatch-key concurrency and fixed-window admission rates.
- Add typed dispatchKey callbacks on decoded payloads, conflict/reserved-key checks and required-key validation before publication, preparation or batch writes.
- Add default read-only policy validation and explicit coordinated reconciliation with group checks, unchanged revisions and omission safety.
- Add MqQueueControlsService with facade-only snapshots and deployment reports.
- Route claims, settlement, release, cancellation and stalled recovery through existing controlled-store operations without changing raw persistence tokens or opening another pool/runtime.
- Add real PostgreSQL package qualification with independent Node worker processes, audited claims, fixed-window identities and cancellation permit reuse; parent consumers compile with TS6/7 and run Node/Bun.
- Preserve optional dependencies, the 20 upstream tooling files and read-only retained CI.
`
)
replace(
  'CHANGELOG.md',
  'additional drivers, distributed-control APIs, named custom retry providers',
  'additional drivers, named custom retry providers'
)
