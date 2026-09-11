# PostgreSQL JSON Fidelity Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for each regression/fix cycle and verify the exact final commit before integration.

**Goal:** Correct issue #5 without changing job contracts, stored JSON or native application behavior.

**Architecture:** A private, non-owning adapter pool/client view supplies encoded JSON text through per-query type parsers to the pinned adapter decoder. Native resources, SQL and queue protocols remain upstream-owned.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, better-effect-mq 0.1.2, PostgreSQL adapter 0.1.3, pg, PostgreSQL 16 and unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-postgres-json-fidelity-design.md

## Constraints

- Branch: fix/postgres-json-fidelity, based on fcd4be44248545663fcb6201f44458d113692fa0.
- Engine/Result/adapter packages remain normal internal dependencies, not consumer peer obligations.
- No extra pool/runtime, global parser mutation, borrowed-resource mutation, payload envelope, automatic migration or npm publication.
- Preserve TypeScript 6/7, the generated Bun lockfile, the 20 tooling hashes and all existing regressions.

## Implemented and exercised

- [x] Observe the public regression against the original 176-test baseline: ten of eighteen PostgreSQL JSON cases fail, including silent string reinterpretation and missing null results.
- [x] Confirm native pg already decodes JSONB while the pinned adapter parses every returned string and treats native null as absent.
- [x] Add parser/type/ownership tests and implement the adapter-only per-query JSON/JSONB result boundary.
- [x] Preserve SQL NULL versus JSON null, non-JSON native parsing, custom application parsers, stable listener reservations, transactions and notifications.
- [x] Verify all eighteen public JSON values and expand actual installed-package consumers to ordinary/controlled queues and owned/borrowed/custom-parser pools.
- [x] Exercise single/decoded/batch publication, prepare without publishing, pending results, retries, typed failures and reads after producer/worker/reader contexts are recreated.
- [x] Inspect PostgreSQL JSON types/equality and SQL NULL state directly rather than inferring durability from returned IDs.
- [x] Observe a native-query-disposal regression, then discard clients after failed one-shot queries and handle checked-out client error events while preserving the original rejection.
- [x] Run direct transaction/rollback/LISTEN tests and deliberate active-backend termination under Node and Bun.
- [x] Add the raw driver/public/disconnection regressions to retained read-only PostgreSQL CI and keep the expanded tarball matrix in test:package.
- [x] Update current documentation and explicitly distinguish this correction from automatic repair of previously misinterpreted results.
- [x] Remove the branch-only write-enabled workflow and temporary documentation updater before final integration.

## Observed evidence

Run 34555896339, job 103128397946, observed the original public failure with the previous 176 tests passing. Run 34557075316 passed the full quality gate at formatted head f20e5594001a575b48ab894bf5b44ae141683a4c: 191 tests, TypeScript 6.0.3/7.0.2, exact tooling integrity and real installed-package JSON consumers under Node/Bun with all three pool modes.

Review added a deterministic query-error client-disposal regression, observed failing in run 34557345247, job 103132706622. The follow-up driver correction passed its direct JSON/transaction/listener/disconnection step under Node and Bun in run 34557436781. These earlier runs are evidence of the red/green cycles, not permission to skip final verification.

## Final delivery gate

The final cleanup/documentation commit must pass all retained read-only CI jobs: Node 22, Node 24 and the real PostgreSQL integration/package job. Merge using the verified expected head only, confirm main again, and close issue #5 with the final run/merge references. No publication or production deployment is authorized by this plan.

Existing correctly stored JSON needs no migration. Already misinterpreted business results cannot be automatically reconstructed and are not silently replayed. Flows, schedules, transactional outbox and other remaining features retain their existing roadmap status.
