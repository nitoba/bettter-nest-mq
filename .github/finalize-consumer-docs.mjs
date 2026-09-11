import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

// One-time reviewed documentation edits; this helper is removed before the PR is merged.
function replace(path, before, after) {
  const source = readFileSync(path, 'utf8')
  if (source.includes(after)) return
  assert.equal(
    source.split(before).length - 1,
    1,
    `Expected one documentation anchor: ${path}: ${before}`
  )
  writeFileSync(path, source.replace(before, after))
}
function append(path, header, content) {
  const source = readFileSync(path, 'utf8')
  if (!source.includes(header))
    writeFileSync(path, `${source.trimEnd()}\n\n${header}\n\n${content}\n`)
}

replace(
  'README.md',
  'bun add pg@^8.16.3 better-effect-mq-postgres@0.1.3 better-effect-mq-outbox@0.1.3',
  'bun add pg@^8.16.3'
)
replace(
  'README.md',
  "The outbox peer is required by the upstream adapter's dependency graph; it does not enable a Nest transactional outbox API. The root entry point works without PostgreSQL or Zod integration packages installed.",
  'The engine and its PostgreSQL/outbox adapters are normal internal dependencies, installed automatically with this library. Nest consumers do not install better-effect, better-result or any better-effect-mq package manually. Only the chosen native driver/schema library is application-facing. The root remains usable without loading pg or Zod, and the internal outbox dependency does not enable the pending Nest transactional-outbox API. See [dependency ownership](docs/dependencies.md).'
)
replace(
  'docs/connections.md',
  "Import postgres from better-nest-mq/postgres. This subpath needs pg, its TypeScript types when compiling, better-effect-mq-postgres@0.1.3 and the adapter's better-effect-mq-outbox@0.1.3 peer. The peer does not mean a Nest transactional outbox facade is implemented.",
  'Import postgres from better-nest-mq/postgres. Consumers select pg and its TypeScript types; better-effect-mq-postgres and better-effect-mq-outbox are automatically installed internal dependencies of this library, not peers the application must manage. The internal outbox package does not mean a Nest transactional-outbox facade is implemented. See dependencies.md for the complete installation boundary.'
)
replace(
  'AGENTS.md',
  '- Keep optional Zod/pg/adapter imports out of the root. Verify installed tarballs with optional peers genuinely absent, then present.',
  '- Keep Zod/pg and adapter runtime imports out of the root. better-effect, better-result, better-effect-mq and the internal PostgreSQL/outbox adapters are normal dependencies, never consumer peer obligations. Packed consumers must not explicitly declare those internal packages. Verify root usage without pg/Zod, then their selected integration subpaths.'
)

replace(
  'docs/architecture.md',
  'distributed-control APIs, custom retry providers, MQ enhancers/events',
  'custom retry providers, MQ enhancers/events'
)
append(
  'docs/architecture.md',
  '## Internal dependency ownership',
  'The engine and its PostgreSQL/outbox adapters are installed as normal dependencies managed by this library. Nest applications do not need their own Effect/Result setup or manual adapter version selection. Only Nest/support peers and chosen native/schema integrations remain application-facing. Root isolation means no eager optional-driver loading, not the absence of internal packages from the dependency tree. See dependencies.md and controls.md for the qualified consumer and distributed-policy boundaries.'
)
append(
  'docs/connections.md',
  '## Known adapter qualification item',
  'Issue #5 tracks a separately observed scalar-string JSON payload failure in the pinned PostgreSQL adapter. Current PostgreSQL end-to-end examples and controlled-claim tests use object payloads; they do not establish universal scalar JSON persistence. Do not treat this unreleased version as fully production-qualified or silently change persisted payload envelopes to conceal that issue.'
)
append(
  'docs/controls.md',
  '## Heartbeat clock race and dependency ownership',
  'Controlled mutations can be rejected when a heartbeat commits a newer updatedAt after the supervisor samples now. The bridge refreshes only the upstream explicit stale-clock rejection, at most three times. It retains the job ID, original lease token and handler result, rechecks current wall time and lease validity in the store, and preserves a retry delay when moving its scheduled time. It never replays the handler or retries ambiguous writes/network failures. Deterministic memory and PostgreSQL tests verify completion, duplicate acknowledgments, active cancellation and expired/replaced lease fencing. Engine/adapter packages are now normal internal dependencies; consumers install only their chosen pg/Zod integrations as documented in dependencies.md.'
)
append(
  'CONTRIBUTING.md',
  '## Internal engine packages',
  "Do not add better-effect, better-result, better-effect-mq or the internal PostgreSQL/outbox adapters to external consumer manifests or this package's peerDependencies. They are normal dependencies maintained by better-nest-mq. Root-only tarball tests remove pg/Zod, not required internal adapters; integration tests then select only the native/schema peers. Preserve the explicit stale-clock regression, bounded mutation refresh and lease-fencing tests."
)
append(
  'CHANGELOG.md',
  '### PR #4 completion and packaging correction',
  '- Correct the controlled heartbeat/settlement race with bounded refresh of explicitly rejected stale-clock mutations, without replaying handlers or granting expired/replaced leases.\n- Cover real PostgreSQL clock races, duplicate acknowledgments, cancellation, retry delays and bounded retry safety.\n- Move the PostgreSQL/outbox adapters to normal internal dependencies alongside the engine; consumers no longer install any better-effect package manually.\n- Test actual tarballs whose application manifest has no internal engine/adapter dependencies.\n- Retain issue #5 as a separate scalar-payload qualification item and keep the package unreleased.'
)

const scopes = [
  'README.md',
  'docs/connections.md',
  'docs/architecture.md',
  'docs/controls.md',
  'docs/roadmap.md',
  'docs/execution.md',
  'docs/contracts.md',
  'CONTRIBUTING.md'
]
for (const path of scopes) {
  let text = readFileSync(path, 'utf8')
  text = text
    .replaceAll('optional Zod/pg/adapter peers', 'optional Zod/pg peers')
    .replaceAll('optional Zod/pg/adapter packages', 'optional Zod/pg packages')
    .replaceAll('optional Zod/database modules', 'optional Zod/native-driver modules')
    .replaceAll('an upstream outbox peer', 'the internal upstream outbox dependency')
    .replaceAll('the current upstream outbox peer', 'the current internal outbox dependency')
    .replaceAll('The current upstream outbox peer', 'The current internal outbox dependency')
  writeFileSync(path, text)
}
