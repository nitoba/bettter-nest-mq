# Distributed Controls Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans for this approved continuation and verify each deliverable before claiming completion.

**Goal:** Add global/per-key concurrency and rate limits enforced by the actual store across independent Nest worker processes.

**Architecture:** Queue metadata compiles to the upstream persistent controls registry. A root-owned coordinator validates capabilities, applies explicitly authorized policy deployments and compares ordinary replica declarations before consumers start. Typed dispatch-key callbacks run on decoded payloads at every publication boundary.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, better-effect 0.14.0, better-effect-mq 0.1.2, PostgreSQL adapter 0.1.3, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-distributed-controls-design.md

## Global constraints

- Work on feat/distributed-controls; main remains unchanged until final verification.
- No new engine, local distributed-limit substitute, runtime per worker or implicit memory fallback.
- Default validation is read-only; policy mutation requires explicit reconcile mode. Never disable omitted queues.
- Preserve strict TS6/7 checks, frozen Bun installation and all 20 upstream tooling hashes.
- No npm release, automatic migration, infrastructure deployment or repository settings changes.

## Task 1 — Baseline and protocol inspection

Files: tests/unit/controls-public-api.test.ts and the temporary branch inspection workflow.

- [ ] Run `bun test` and confirm the missing QueueControls export is the only new failure.
- [ ] Read current repository documentation and inspect published controlled-store and worker dispatch behavior before choosing any integration cast.

## Task 2 — Declarations and key contracts

Files: src/controls/types.ts, src/controls/decorator.ts, src/contracts/job-definition.ts, src/contracts/queue-definition.ts; tests/unit/controls-contracts.test.ts and tests/types/controls.types.ts.

- [ ] Test positive finite limits, immutable options, duplicate metadata, subclass isolation and a typed callback on decoded codec payloads.
- [ ] Implement QueueControls metadata and dispatchKey derivation. Reject empty, reserved, overlong or NUL-containing keys and conflicting explicit overrides before store writes.
- [ ] Test prepare and enqueueMany using the same key rules, including one invalid item producing no batch write.

## Task 3 — Persistent policy coordinator

Files: src/controls/service.ts, src/engine/queue-controls.ts, module options/definition and engine host.

- [ ] Test default validate mode, missing/mismatched records, explicit reconciliation, group conflicts and adapters missing real controlled capabilities.
- [ ] Compile the existing upstream controls registry and call its adapter operations inside the existing session admission boundary.
- [ ] Complete all capability checks before policy writes and apply policy before clients/workers activate. Preserve cleanup on failure without claiming cross-store transactional rollback.
- [ ] Return safe snapshots and unchanged revisions; omitted policies remain persisted.

## Task 4 — Real distributed qualification

Files: tests/package/consumer/controls.ts, worker-process fixture, compiler configs and scripts/test-package.ts.

- [ ] Execute independent worker processes against a random PostgreSQL schema through the actual packed library.
- [ ] Assert shared global limits, per-key progress, rate-window admission, policy persistence/revision stability, mismatch rejection and permit reuse after cancellation/completion.
- [ ] Run the retained Node22/24 quality checks and PostgreSQL packed-consumer job with TypeScript 6/7 and Node/Bun.

## Task 5 — Handoff and integration

Files: README.md, docs/controls.md, roadmap/architecture/AGENTS and CHANGELOG.

- [ ] Document implemented syntax, coordinated policy deployment, fixed-window semantics, durable connection identity and remaining features.
- [ ] Remove temporary branch-only workflow, inspect final diff and verify the exact final commit in read-only CI.
- [ ] Merge with expected-head protection and verify main. Report actual results, not merely configured tests.
