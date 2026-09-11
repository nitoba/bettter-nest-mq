# Changelog

## Unreleased

### M3 — Core producers and decorated workers

- Add typed enqueue/enqueueDecoded/enqueueMany, prepare, poll, awaitResult, execute, attempts, cancel, retry and promote through the real queue engine.
- Add Worker/Process/JobData/JobContext and local MqWorkersService status/awaitIdle, sharing the existing runtime with stores and Clock.
- Preserve Standard Schema and decoded codec boundaries for payloads, results and known failures.
- Execute persisted fixed/linear/exponential retry policies; keep unexpected defects non-retryable by default with explicit opt-in.
- Support handler/worker concurrency, execution timeout, cooperative active cancellation and graceful draining before store release.
- Resolve request/transient-scoped worker dependencies using a fresh Nest context per attempt.
- Preserve inherited processors and reject missing parameter metadata, duplicate processors, shared ambiguous job descriptors and unsupported HTTP enhancer metadata.
- Add producer-only deployment mode, explicit unbound/closed errors and identity-scoped job access.
- Verify caller wait timeouts/abort do not cancel durable jobs and prepare does not publish.
- Add real PostgreSQL packed end-to-end consumers for idempotency, producer restart, retries, decoded results, known failures and persisted outcomes under Node/Bun with TypeScript 6/7.

### M2 — Engine and PostgreSQL lifecycle

- Add one private runtime, named stores, protocol/capability validation and sanitized connection readiness.
- Preserve inert declarations, contract-only registration, owned/borrowed pools, startup rollback and memoized cleanup.
- Add PostgreSQL integration with explicit migration/validation and safe idle-client error handling.
- Verify persistence and connection-name namespace compatibility against a real server.

### M1 — Typed contracts and Nest registration

- Add QueueService/JobDefinition, Queue/Job/Retry/JobTimeout, stable versions and immutable policy resolution.
- Add Standard Schema, explicit codecs, optional Zod and distinct failure categories.
- Add real Nest feature registration, atomic discovery, aliases, application isolation and module re-exports.

### M0 — Foundation

- Configure Bun, TypeScript 6 compatibility, unchanged upstream Oxlint/Oxfmt/plugin, tsdown, publint, Lefthook and CI.

Still pending: flows, schedules, transactional outbox, additional drivers, distributed-control APIs, named custom retry providers and MQ enhancer/event integration. Version remains 0.0.0; no npm release has been published.
