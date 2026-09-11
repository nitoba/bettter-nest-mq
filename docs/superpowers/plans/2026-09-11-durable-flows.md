# Durable Flows Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans task-by-task, then verification-before-completion.

**Goal:** Deliver durable Nest Flow/FanOut/Collect using the upstream PostgreSQL protocol.

**Architecture:** Immutable typed job references and class/method metadata compile into upstream Flow definitions and lazy phase handlers. Enabled flow stores share the existing connection resources and runtime; every worker that participates receives definitions for durable report/relay/reconciliation.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0 (6.0.3/7.0.2), pinned better-effect 0.14.0, better-effect-mq 0.1.2, PostgreSQL adapter 0.1.3.

**Spec:** docs/superpowers/specs/2026-09-11-durable-flows-design.md

## Global constraints

- Branch feat/durable-flows from b35f4208b8f55db97d21296a86dacaec21412615; preserve 20 tooling hashes and strict type/lint rules.
- Public Nest/Promise types only; no additional required consumer peers, no fabricated handlers, no automatic migration or production deployment.
- Same physical pool/runtime and raw connection namespace; flow-specific decoded JSON must not affect ordinary job/outbox/schedule/native queries.
- Existing supervisor owns leases, flow manifests, child IDs, report outbox, reconciliation and cancellation. No replacement state machine.

## Task 1 — Protocol inspection and observed red baseline

Files: tests/unit/flows-public-api.test.ts, temporary .github/workflows/flows-development.yml.

- [ ] Run `bun install --frozen-lockfile && bun test`; assert the new public export test fails while 242 existing tests pass.
- [ ] Inspect installed exports, exact FlowStore/token/Worker contracts, raw namespaces and PostgreSQL flow JSON parsing before implementation.

## Task 2 — Typed contracts and phase compilation

Files: src/flows/types.ts, references.ts, decorators.ts, errors.ts; src/engine/flow-discovery.ts, flow-plan.ts, flow-results.ts; tests/unit and tests/types/flows.types.ts.

- [ ] Add metadata tests with `@Flow({ name: 'summary', parent: flowJob(Queues, 'summary'), children: [flowJob(Queues, 'item')], onChildFailure: 'continue' })`, and fail unregistered/non-job references before acquiring resources.
- [ ] Add negative types: `flowChildren(flowJob(Queues, 'item'), [{key:'x',payload:'wrong'}])` must reject incompatible input; result pages preserve referenced job and outcome discrimination.
- [ ] Resolve each actual class method through Nest, preserve scoped DI, validate phase parameter metadata and reject incompatible HTTP enhancers/accessors.
- [ ] Compile children through existing schema boundaries; reject missing dispatch-key support, unrepresentable delay and duplicate keys before fan-out writes.

## Task 3 — Resource integration and durable lifecycle

Files: src/integrations/postgres-flow-resource.ts, postgres-json-pool.ts; src/engine/connection-definition.ts, engine-session.ts, worker-discovery.ts, worker-plan.ts, mq-engine.host.ts.

- [ ] Add opt-in tests and a decoded per-query flow JSON parser regression preserving native overrides and existing encoded JobStore behavior.
- [ ] Add raw and operation flow tokens to the existing Runtime, acquire/probe complete resources before activation and pass flow definitions/handlers to actual Worker layers.
- [ ] Test waiting-children frees capacity, parent phases are not normal duplicate handlers, and scoped invocations/readers finish before shutdown releases storage.

## Task 4 — Administration and PostgreSQL package qualification

Files: src/flows/service.ts; src/engine/flows.ts; tests/integration/flows.test.ts; tests/package/consumer/flows.ts, flow-child.ts, flow-contracts.ts, tsconfig.flows.json; scripts/test-package.ts.

- [ ] Expose contract-scoped status and cascade cancellation with facade errors; existing parent producers publish and await durable results.
- [ ] Qualify multi-process restart between fan-out and collection, continue/fail, cancellation, nesting, empty manifests and schema/codec/JSON outcome pages using actual installed tarballs.
- [ ] Re-run the existing PostgreSQL controls/outbox/schedules/parser matrix; document unsupported combinations rather than masking them.

## Task 5 — Review and exact-head integration

Files: docs/flows.md, README.md, docs/roadmap.md, docs/architecture.md, AGENTS.md, CHANGELOG.md.

- [ ] Inspect the final diff and review every spec requirement, public declaration boundary and lifecycle failure path.
- [ ] Run `bun run check`, actual PostgreSQL scripts and all installed TS6/7 Node/Bun consumers.
- [ ] Remove branch-only development helpers; require retained read-only CI on the exact final head, then integrate PR and verify main.
