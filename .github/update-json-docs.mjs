import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

function replace(path, before, after) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(after)) return
  assert.equal(text.split(before).length - 1, 1, `Expected one documentation anchor: ${path}`)
  writeFileSync(path, text.replace(before, after))
}

replace('README.md', 'See [distributed controls](docs/controls.md)', 'See [PostgreSQL JSON fidelity](docs/postgres-json.md), [distributed controls](docs/controls.md)')
replace('README.md', '## Distributed controls', `## PostgreSQL JSON correction

Issue #5 is addressed by adapter-only JSON result normalization. Scalar strings keep their type, including strings that resemble JSON, and a valid JSON null result is no longer confused with missing SQL NULL. No job schema, stored envelope, pool ownership, internal dependency requirement or migration changes are needed. The regression matrix covers ordinary and controlled queues, retries, typed failures, batches, preparation and reads after application restart. See [the compatibility and existing-record notes](docs/postgres-json.md).

## Distributed controls`)
replace('docs/connections.md', '## Known adapter qualification item\n\nIssue #5 tracks a separately observed scalar-string JSON payload failure in the pinned PostgreSQL adapter. Current PostgreSQL end-to-end examples and controlled-claim tests use object payloads; they do not establish universal scalar JSON persistence. Do not treat this unreleased version as fully production-qualified or silently change persisted payload envelopes to conceal that issue.', `## JSON fidelity and native parser isolation

Issue #5 is corrected by a private, non-owning adapter pool/client view. JSON and JSONB fields reach the pinned adapter as encoded text, so its decoder parses exactly once and preserves strings, JSON null and other valid JSON values. Native application queries, custom parsers, global pg configuration and pool ownership remain unchanged. The format on disk is unchanged and needs no migration.

The real database/package matrix covers 18 JSON values in ordinary and controlled queues, including single/decoded/batch publication, preparation, results, typed failures, retries and post-restart reads. Native driver regressions also check transaction identity, rollback, LISTEN notifications, SQL query failures and active connection termination. Correctly stored data remains readable; previously misinterpreted business results are not automatically repaired or replayed. See [PostgreSQL JSON fidelity](postgres-json.md) for the pinned compatibility boundary and operational precautions.`)
replace('CHANGELOG.md', '## Unreleased', `## Unreleased

### PostgreSQL JSON fidelity — issue #5

- Preserve scalar string payloads/results without reinterpreting JSON-looking text as another type.
- Distinguish valid JSON null results from missing SQL NULL, including persisted attempt results.
- Add private per-query JSON/JSONB parser normalization without mutating native/global parsers, stored JSON, pool ownership or consumer dependencies.
- Preserve native client disposal on query failure and handle active connection errors without an unhandled process error.
- Qualify 18 JSON values across ordinary/controlled queues, owned/borrowed/custom-parser pools, batches, preparation, retries, typed failures and application restarts through installed tarballs.
- Retain direct native transaction/rollback/notification/disconnect regressions in read-only PostgreSQL CI. No migration or automatic replay/data repair is performed.
`)
replace('AGENTS.md', 'Read docs/controls.md, README.md', 'Read docs/postgres-json.md, docs/controls.md, README.md')
replace('AGENTS.md', '- Startup validates schema, never applies migrations.', '- The pinned PostgreSQL adapter expects encoded JSON text. Preserve its private per-query parser boundary; never mutate global/native pg parsers or introduce payload envelopes to conceal scalar/null bugs. Rerun JSON fidelity and native query-error/transaction/listener regressions before changing or upgrading this boundary.\n- Startup validates schema, never applies migrations.')
replace('docs/roadmap.md', '## M0 — Foundation — implemented', `## PostgreSQL JSON fidelity — corrected

Issue #5 now has public and installed-package regressions for scalar strings, JSON-looking strings, numbers, booleans, null, arrays and objects. The adapter-only parser boundary preserves SQL NULL distinction, native application parsing and pool ownership without changing persisted envelopes. Ordinary/controlled queues, retries, typed failures, batches, preparation and post-restart reads are covered. See docs/postgres-json.md; this correction does not implement the pending flow/schedule/outbox features below.

## M0 — Foundation — implemented`)
