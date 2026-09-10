# Changelog

## Unreleased

### M1 — Typed contracts and Nest registration

- Add inert QueueService/JobDefinition contracts with schema-derived input, payload, result and failure types.
- Add Queue, Job, Retry and JobTimeout decorators with versioned identities, metadata inheritance and configuration validation.
- Resolve immutable module/queue/job/decorator policies, replacing retry policies as complete units.
- Add Standard Schema validation, explicit asynchronous codecs, JSON fidelity checks and round-trip validation.
- Add the optional better-nest-mq/zod subpath for Zod 4 nested codecs; keep the root Zod-independent.
- Add typed domain-failure exceptions and distinct validation, encoding, defect and declaration errors.
- Add MqModule.forFeature and an application-context-local registry over real Nest providers.
- Reject conflicting identities and scoped queue contracts before exposing any registry snapshot.
- Expand real Nest, schema, metadata and compile-time regressions; exercise packed consumers with and without Zod under TypeScript 6/7 and Node/Bun.

### M0 — Project bootstrap

- Initialize the better-nest-mq package with Bun 1.4.2, strict TypeScript and a TypeScript 6 compatibility check, tsdown and publint.
- Copy the original Oxlint/Oxfmt configurations and anti-slop plugin unchanged from the pinned better-effect revision.
- Add Nest 12 configurable modules, immutable shutdown configuration, package tests and CI.

No engine-backed publication, worker execution, database adapter, flow, schedule or transactional outbox is included yet. No npm release has been published.
