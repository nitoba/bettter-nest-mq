# Durable Schedules Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans and verify each task before reporting completion.

**Goal:** Deliver Nest-decorated persistent schedules using the existing scheduler and PostgreSQL store.

**Architecture:** Queue job-property metadata is compiled and schema-validated before startup. An opt-in schedule store shares the existing native pool and raw namespace; a coordinator manages definition validation/deployment and upstream scheduler lifecycle within the same runtime.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, pinned better-effect 0.14.0 / better-effect-mq 0.1.2 / PostgreSQL adapter 0.1.3, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-11-durable-schedules-design.md

## Global constraints

- Work on feat/durable-schedules; integrate only after exact-head CI passes.
- Internal engine packages remain dependencies, not consumer-installed peers. No new runtime/pool per feature or process-global resources.
- No automatic schema migrations, silent memory fallback, timer-per-replica enqueue or duplicate occurrence algorithm.
- Preserve raw connection identity and JSON parser/null corrections. Schedule policies never silently bypass per-key control requirements.
- Use one coordinated reconcile writer; ordinary startup validates and never deletes omissions. No cross-store rollback promise.
- Retain TS6/7, Bun, actual Node/Bun packed consumers, real PostgreSQL and exact 20-file tooling provenance.

## Task 1 — Baseline and published contract inspection

Files: tests/unit/schedules-public-api.test.ts, temporary .github/workflows/schedules-development.yml.

- [ ] Run `bun test`: expect existing tests to pass and the missing Schedule export regression to fail.
- [ ] Read project guides and exact installed scheduler/store/registry types; inspect raw namespace/layer construction and atomic tick behavior.

## Task 2 — Metadata and compilation

Files: src/schedules/types.ts, decorator.ts, errors.ts; src/engine/schedule-compiler.ts; tests/unit and tests/types.

- [ ] Add regressions for invalid cron/interval/timezone, duplicate addresses, input payload copying, inheritance and unsupported combinations.
- [ ] Implement Schedule options and metadata, then compile registered job properties through validateSchema/encodeSchema and resolved policies.
- [ ] Ensure every schedule validates before resource writes. Run both TypeScript compiler checks and targeted tests.

## Task 3 — Resource and lifecycle integration

Files: src/integrations/postgres schedule resource, src/engine schedule plan/session/host, module configuration/service.

- [ ] Test opt-in validation, missing capabilities, contract-only use and independent scheduler role.
- [ ] Add the existing PostgreSQL schedule store to the acquired connection bundle with the private JSON pool view and stable raw token.
- [ ] Add one lazy upstream scheduler layer to the same runtime; warm stores, synchronize definitions, then activate consumers. Stop/drain ticks before pool release and cover startup rollback.

## Task 4 — Definition administration and distributed qualification

Files: src/schedules/service.ts, src/engine/schedules.ts; tests/package/consumer schedule fixtures and script integration.

- [ ] Expose scoped get/list/upsert/pause/resume/remove, validate/reconcile reports and scheduler status/sweep through facade-only Promises.
- [ ] Verify pause/revision/next-run preservation, invalid input producing no writes, stable occurrence IDs and restart safety against real PostgreSQL.
- [ ] Start independent scheduler child processes with the actual packed package, proving one enqueue per occurrence and overlap/misfire behavior without local mocks.

## Task 5 — Review and integration

Files: README.md, docs/schedules.md, roadmap/architecture/dependency guides, AGENTS.md, CHANGELOG.md.

- [ ] Run complete quality gates, existing outbox/JSON/controls regressions and installed schedule consumers with TS6/7 and Node/Bun.
- [ ] Document exact semantics and limitations. Remove temporary write-enabled tooling and verify retained read-only CI on the final commit.
- [ ] Merge using expected-head protection, verify main and report the actual change set/results without claiming full feature parity.
