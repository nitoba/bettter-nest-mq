# Durable Flows Implementation Plan — blocked checkpoint

**Goal:** Deliver Nest Flow/FanOut/Collect through the existing durable engine and PostgreSQL adapter.

**Branch:** feat/durable-flows, PR #9, based on main b35f4208b8f55db97d21296a86dacaec21412615.

**Status:** Candidate implementation exists, but PostgreSQL qualification failed. Do not merge. The stable main branch remains unchanged. Blocker: [better-effect #387](https://github.com/nitoba/better-effect/issues/387).

**Spec:** docs/superpowers/specs/2026-09-11-durable-flows-design.md. Candidate API and exact limitations: docs/flows.md.

## Preserved constraints

Use the upstream manifest, report/relay, lease and collection protocol; no replacement state machine, fabricated Process handler, implicit memory fallback or extra Runtime/pool. Keep typed Nest/Promise APIs, internal dependency ownership, raw persistence identities, explicit migrations and the 20 original tooling hashes. No npm publication or production deployment is authorized by this checkpoint.

## Completed implementation and observed reference checks

- [x] Observe the new public API regression fail against 242 passing baseline tests.
- [x] Inspect exact published Flow/FlowStore/Worker APIs and PostgreSQL namespace/JSON boundaries.
- [x] Add immutable typed job references, finite child plans, Flow/FanOut/Collect metadata and schema/codec validation.
- [x] Integrate actual Nest phase providers and scoped dependencies, rejecting ambiguous identities, invalid parameters and unsupported enhancer metadata.
- [x] Add bounded phase-owned result reads, issued-cursor checks and draining of admitted reads on close.
- [x] Add the opt-in flow resource to the existing connection/runtime with separate non-owning decoded-JSON parsing and stable raw namespaces.
- [x] Implement administration and attach definitions/phases to upstream workers without fabricated handlers.
- [x] Verify reference-store flow execution at concurrency 1, empty manifests, dates/scoped phases, invalid manifests, missing resources and reader/identity safety.
- [x] Verify both source compiler checks, 261 unit/Nest tests, formatting, build, type-aware lint and publint at 89c66e8 in run 34616650927.

These checks do not establish PostgreSQL flow correctness. The same run failed the installed database scenario.

## Confirmed database blocker

- [x] Add tests/postgres/flow-protocol.ts without Nest or facade imports.
- [x] Run native migrations and native JobStore/FlowStore fan-out against PostgreSQL using the exact published dependencies.
- [x] Confirm waiting-children persistence succeeds while getJob, heartbeat and release return JobDefinitionError for unsupported state.
- [x] Preserve the failing run 34616650978 and the executable reproduction in upstream issue #387.
- [x] Keep failures visible instead of coercing states, skipping assertions or claiming completion.

The facade's materialized-flow reads use FlowStore directly, but this does not repair upstream heartbeat/release. A coherent v2 adapter/Worker path is required; silently widening the frozen v1 contract is not an approved fix. The changed dependency/release path must be decided explicitly once the upstream correction is tested.

## Remaining verification and integration

- [ ] Resolve upstream #387 and verify compatible published or otherwise explicitly approved dependency artifacts.
- [ ] Make the pure protocol reproduction pass, including lost-lease classification and protection against reclaiming suspended parents.
- [ ] Pass actual worker hand-off and graceful shutdown with concurrency 1 against PostgreSQL.
- [ ] Pass the written installed-consumer scenarios: crash after manifest persistence, two-process restart, stable child IDs, JSON/date collection, nesting, continue/fail and cooperative cancellation.
- [ ] Complete the full existing PostgreSQL controls, outbox, schedules, parser/ownership and package regression matrix on the same final head.
- [ ] Perform final code review and exact-head read-only Node22/24 and PostgreSQL CI before merge; verify main after any eventual integration.

## Handoff cleanup

Temporary write-enabled development/formatter workflows and the isolated debug package runner are removed at this checkpoint. The dedicated published-protocol regression remains read-only and failing for the known defect. Normal repository CI remains enabled; no failure is marked as expected success or hidden with continue-on-error.

Candidate source code and tests are preserved in PR #9. No main-branch merge, package release, production deployment or claim of full flow parity has been made. Documentation must keep the work-in-progress warning until database qualification succeeds.
