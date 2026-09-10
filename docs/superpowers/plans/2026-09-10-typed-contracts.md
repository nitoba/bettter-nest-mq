# Typed Contracts Implementation Plan

> For agentic workers: use the executing-plans workflow and verify each task before claiming completion.

**Goal:** Deliver M1 typed contracts and validated Nest registration without pretending the queue engine is already connected.

**Architecture:** Inert typed job descriptors hold Standard Schema contracts. Decorators hold immutable identity/policy metadata. A root-owned registry discovers actual Nest providers and atomically publishes the validated definitions.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, the unchanged upstream Oxlint/Oxfmt configuration, optional Zod 4 codecs.

**Spec:** docs/superpowers/specs/2026-09-10-typed-contracts-design.md

## Global constraints

- Keep public Effect/Result/Layer/Runtime types out of the package.
- Use generated bun.lock and retain both TypeScript compiler checks.
- Never weaken the vendored lint rules or simulate queue/storage behavior.
- Work on feat/typed-contracts and integrate only after final CI verification.

## Task 1 — Regression baseline

Files: tests/unit/contracts-public-api.test.ts and these design/plan documents.

- [ ] Assert the approved public exports with `expect(Object.keys(mq)).toContain(name)` against the actual bootstrap.
- [ ] Run the existing CI and confirm failure specifically because QueueService is absent, while baseline tests remain green.

## Task 2 — Schema and typed job contracts

Files: src/contracts/schema.ts, src/contracts/errors.ts, src/contracts/job-definition.ts, src/contracts/queue-service.ts, src/integrations/zod.ts; tests/unit/schema-contracts.test.ts and tests/types/contracts.types.ts.

- [ ] Test Standard Schema async success/issues/throwing vendors, typed payload/results/failures, explicit encode/decode round trips, corrupt JSON and lossy JSON values.
- [ ] Implement `defineCodec(schema, encoder)`, typed schema validation/encoding, inert `this.job({ payload, result, failure })`, and JobFailureException.
- [ ] Make tests reject repeated one-way transforms rather than silently persisting the wrong representation.

## Task 3 — Metadata and policy resolution

Files: src/contracts/policies.ts, src/contracts/decorators.ts, src/contracts/queue-definition.ts; tests/unit/queue-contracts.test.ts.

- [ ] Test stable identity, distinct versions/connections, duplicate/missing decorators, non-invoked getters, inherited metadata and independent subclass policies.
- [ ] Implement copy-on-write metadata and library/module/queue/job/decorator precedence with complete retry-policy replacement.
- [ ] Test invalid versions, attempts, durations, jitter and custom policy references.

## Task 4 — Real Nest registry

Files: src/module/mq.module.ts, src/module/mq.registry.ts, module options/configuration, tests/integration/queue-registration.test.ts.

- [ ] Test `MqModule.forFeature([QueueClass])`, real constructor DI, bootstrap discovery, alias deduplication, duplicate identity failure, scoped-provider rejection and application-context isolation.
- [ ] Build a complete temporary registry before swapping the snapshot and clear it on close.
- [ ] Preserve existing forRoot/forRootAsync/global and configuration tests.

## Task 5 — Package qualification and delivery

Files: package.json, bun.lock, tsdown.config.ts, scripts/test-package.ts, external consumer fixtures, README.md, CHANGELOG.md, docs/contracts.md and roadmap/status documentation.

- [ ] Make Standard Schema a production type dependency and Zod an optional peer/subpath; do not import Zod from the root.
- [ ] Exercise the packed library outside the workspace with real Nest DI, both TypeScript versions, Node and Bun; test the root without installing Zod.
- [ ] Run `bun run check`, inspect lint/build/test output, and remove any temporary development workflow before integration.
- [ ] Deliver verified commits with exact implemented scope and the next engine-bridge milestone documented.
