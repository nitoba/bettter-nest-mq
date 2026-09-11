# PostgreSQL JSON Fidelity Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement each regression/fix cycle and verify the exact final commit.

**Goal:** Correct issue #5 without changing queue contracts, stored JSON or borrowed pg behavior.

**Architecture:** A narrow non-owning adapter-only driver view normalizes JSON result representation when the pinned adapter expects encoded JSON text. Native connections, leases, SQL and ownership stay in the existing engine/adapter.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, better-effect-mq 0.1.2, PostgreSQL adapter 0.1.3, pg, PostgreSQL 16, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-postgres-json-fidelity-design.md

## Global constraints

- Work on fix/postgres-json-fidelity based on fcd4be44248545663fcb6201f44458d113692fa0.
- All better-effect/better-result/engine/adapter dependencies stay internal normal dependencies.
- No extra runtime/pool, global pg parser mutation, borrowed-resource mutation, payload envelopes, npm publication or automatic migration.
- Preserve strict TypeScript 6/7 checks and all 20 upstream tooling files.

## Task 1 — Observe the regression

Files: tests/postgres/json-fidelity.ts and branch-only PostgreSQL workflow.

- [ ] Run the current bun test baseline and inspect the installed adapter's parseJson and pg parser contracts.
- [ ] Execute a real Nest QueueService round trip for every JSON primitive plus arrays/objects, collecting failures by boundary.
- [ ] Confirm failures are value-conversion defects rather than schema/dependency/setup errors. Print raw SQL/driver observations only for the synthetic fixture data.

```sh
bun install --frozen-lockfile
bun test
bun tests/postgres/json-fidelity.ts
```

## Task 2 — Fix the driver boundary

Files: src/integrations/postgres-json-pool.ts, src/integrations/postgres.ts, tests/unit/postgres-json-pool.test.ts.

- [ ] Add focused tests for parser selection, SQL NULL/JSON null separation and borrowed client immutability before introducing the adapter view.
- [ ] Supply raw JSON text to the upstream decoder using per-query parser options; delegate all non-JSON parsing and native resource/listener methods correctly.
- [ ] Reuse the same native pool and client for all transactions, preserve reservations and cleanup, and verify the baseline regression now passes.

## Task 3 — Qualify the public package

Files: tests/package/consumer/json-fidelity.ts, its compiler config, scripts/test-package.ts, docs/postgres-json.md, README.md, CHANGELOG.md, issue #5.

- [ ] Test single/decoded/batch publication, prepare without publishing, JSON input/output and typed failure content through installed tarballs.
- [ ] Cover ordinary and controlled queues, owned/borrowed pools, post-restart reads and unmodified application parsing.
- [ ] Inspect actual PostgreSQL JSON types and distinguish stored JSON null from missing/SQL-null result state.
- [ ] Run bun run check, real PostgreSQL integration and packed consumers with TypeScript 6/7 under Node/Bun.
- [ ] Document the pinned compatibility boundary and observed results; remove temporary workflows, verify the final read-only CI, then merge with expected-head protection.
