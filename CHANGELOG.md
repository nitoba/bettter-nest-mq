# Changelog

## Unreleased

### M2 — Private engine and PostgreSQL lifecycle

- Add one private engine runtime per configured Nest application context, named stores and protocol/capability checks.
- Add opaque inert connection descriptors and MqConnectionsService with safe snapshots and live probes.
- Preserve contract-only module usage without a runtime or implicit in-memory fallback.
- Add atomic startup, immediate admission closure, concurrent-close safety, failed-acquisition rollback and cleanup error aggregation.
- Add the optional PostgreSQL integration with borrowed/owned pools, schema validation and explicit deployment migrations.
- Handle owned pg idle-client errors without crashing the consuming process or logging raw client credentials.
- Preserve named-store persistence semantics: connection names are part of the durable storage address.
- Add real PostgreSQL persistence/rollback/ownership tests and public tarball consumers with deliberate connection termination under Node and Bun.
- Expand optional-dependency isolation and check all public declaration chunks for leaked engine imports.

### M1 — Typed contracts and Nest registration

- Add typed QueueService/JobDefinition, Queue/Job/Retry/JobTimeout, versioned identities and immutable policies.
- Add Standard Schema validation, explicit codecs, optional Zod and typed failure categories.
- Add real Nest feature registration, atomic registry discovery, aliases, isolation and standard module re-exports.

### M0 — Foundation

- Configure Bun, TypeScript 6 compatibility, exact upstream Oxlint/Oxfmt/plugin, tsdown, publint, Lefthook, tests and CI.

No public producer/worker execution, flow, schedule or transactional outbox facade is implemented yet. No npm release has been published.
