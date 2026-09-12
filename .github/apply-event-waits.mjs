import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, replacements) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of replacements) {
    assert.equal(text.split(before).length - 1, 1, `Unexpected anchor count: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('tests/integration/event-waits.test.ts', [[
  "  Object.defineProperty(events, 'awaitEvents', {",
  "  Object.defineProperty(events, 'awaitEvents', {\n    configurable: true,"
]])
patch('README.md', [
  ['named retry providers and explicit MQ enhancers are implemented.', 'named retry providers, explicit MQ enhancers and event-assisted result waits are implemented.'],
  ['See [retry providers and MQ enhancers]', 'See [event-assisted result waits](docs/event-waits.md), [retry providers and MQ enhancers]'],
  ['## Retry providers and MQ enhancers', `## Event-assisted result waits

Enable \`postgres({ events: true, ...connectionOptions })\` in the app waiting for results, then select \`job.awaitResult(id, { strategy: 'events', pollFallbackMs: 5000, timeoutMs: 30000 })\`. Polling remains the default. The reader shares the existing pool/runtime and raw namespace; its internal operation alias does not create another durable address.

Events are wake-up hints. The engine rereads persisted job results, handles registration races and retains bounded fallback for lost hints/reader failures. Wait timeout/abort does not cancel the job. Missing explicit reader configuration is rejected. Native PostgreSQL event readers may themselves poll; this is not a zero-polling or reduced-load guarantee. See [event waits](docs/event-waits.md) for deployment, options and the distinction from resumable subscriptions.

## Retry providers and MQ enhancers`]
])
patch('docs/roadmap.md', [
  ['**named retry providers and explicit MQ enhancers** are implemented.', '**named retry providers, explicit MQ enhancers and event-assisted result waits** are implemented.'],
  ['docs/flows.md and docs/execution-extensions.md.', 'docs/flows.md, docs/execution-extensions.md and docs/event-waits.md.'],
  [`## M3.1c — Durable events and further failure qualification — pending

Add durable-event waits with cursor/recovery semantics. Extend crash/lease-loss and mid-flight policy-change qualification. Do not substitute process-local middleware or observability callbacks for a durable event log.`,
   `## M3.1c — Event-assisted result waits — implemented

Opt-in PostgreSQL event readers share the raw namespace, native pool and existing runtime. Job awaitResult/execute support the native events strategy with bounded polling fallback, race-safe result rereads, timeout/abort isolation and no consumer-visible engine tokens. Reader configuration is explicit; runtime event failures can degrade to job polling. See event-waits.md. Native event-log polling remains adapter-specific; no push-only performance guarantee is claimed.

## M3.1d — Durable subscriptions and further failure qualification — pending

Add resumable subscription APIs with explicit cursor/checkpoint/retention/recovery semantics. Extend crash/lease-loss and mid-flight policy-change qualification. Do not substitute process-local observers for a durable log or treat ephemeral result waits as acknowledged subscriber checkpoints.`],
  ['M3.1b retry / MQ enhancers implemented; M3.1c durable events pending', 'M3.1b retry / MQ enhancers + M3.1c event waits implemented; subscriptions pending']
])
patch('CHANGELOG.md', [['## Unreleased', `## Unreleased

### Event-assisted result waits

- Add opt-in PostgreSQL event readers sharing the existing pool, raw namespace and application runtime.
- Expose the native event-result strategy through typed Promise options without public engine tokens; retain polling as the default.
- Validate per-strategy intervals and timer bounds, require configured readers and preserve caller timeout/abort isolation.
- Add real Nest regressions for lost hints, failing readers, startup rollback and shutdown; add installed PostgreSQL consumers for JSON/codecs, persistence and namespace matching.
- Do not change persistence activation to required, add dependencies, install subscriptions or publish a package release.
`]])
for (const [path, text] of staged) writeFileSync(path, text)
for (const path of ['docs/execution.md', 'docs/connections.md', 'docs/architecture.md', 'docs/execution-extensions.md', 'AGENTS.md']) {
  const text = readFileSync(path, 'utf8')
  assert.ok(!text.includes('## Event-assisted wait integration'))
  writeFileSync(path, text + `\n\n## Event-assisted wait integration\n\nEvent-assisted awaitResult/execute is implemented; see docs/event-waits.md (event-waits.md from this directory) for the API and exact scope. Configure postgres({ events: true }) on the waiting application and select strategy: 'events' with pollFallbackMs. Polling remains the default. The raw event token shares the job namespace; only an in-runtime operation alias is added. No second pool/runtime, automatic migrations or required-writer activation is introduced.\n\nEarlier references to polling-only result waits are superseded by this integration. Runtime reader errors/lost hints use the existing bounded fallback; missing explicit reader configuration still fails. Timeout and abort do not cancel durable work. Keep tests for shutdown, parser fidelity and installed consumers. Resumable subscriptions, checkpoint APIs and retention administration remain pending; this is not a promise of zero polling.\n`)
}
