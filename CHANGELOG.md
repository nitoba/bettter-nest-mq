# Changelog

## Unreleased

### Retry providers and MQ enhancers

- Add named/versioned RetryPolicy providers through static Nest DI and the native synchronous typed-failure retry hook.
- Persist reference metadata, not callbacks; fence mismatched versions before user code and retain producer-only publication across jobs, schedules, flows and outbox.
- Add UseMqGuards/Pipes/Interceptors/Filters with a facade-owned MQ execution context, deterministic class/method order and shared attempt-local DI.
- Revalidate decoded pipe outputs and preserve native result/failure validation, cancellation, lease ownership and attempt budgets.
- Prevent repeated/escaped interceptor continuations and drain already-admitted downstream work before settlement/shutdown.
- Add source/type regressions and installed PostgreSQL consumers for restarted retry execution, schedules/outbox references and custom flow children.
- Explicitly reject incompatible flow child backoff overrides while preserving inherited policies and compatible attempt-budget changes; upstream issue better-effect#389 tracks the native representation mismatch.
- No dependency version, schema migration, required consumer peer or npm publication changes.

### Durable flows

- Add typed `flowJob`/`flowChildren` references and `@Flow`/`@FanOut`/`@Collect` Nest phases with normal DI and attempt-local context.
- Add bounded `FlowResultsReader` pages/all collection with decoded completed, typed-failed and cancelled outcomes.
- Add opt-in PostgreSQL flow resources that reuse the existing runtime, pool, namespace and private JSON parser boundary.
- Preserve the native v1/v2 inspection boundary while supporting suspended-parent administration/recovery through a validated v2 projection.
- Qualify fresh-lease Collect execution, stable child identities, process-death recovery, scalar/null/Date codecs, nested flows, fail-fast and cascade cancellation.
- Pin the released internal flow engine pair `better-effect-mq@0.1.3` and `better-effect-mq-postgres@0.1.4`; consumers still declare no engine packages.

### Persistent schedules

- Add repeatable Schedule decorators, cron/interval/timezone policies and schema/codec-validated static payloads.
- Add MqSchedulesService with typed upsert, contract-scoped get/list/pause/resume/remove, reconcile reports and local scheduler status/sweep.
- Reuse upstream fenced occurrences and PostgreSQL schedule storage in the existing runtime/pool/parser boundary.
- Separate scheduler, worker and outbox roles; preserve operator pauses, unchanged revisions, cursors and omitted definitions.
- Validate declarations before resource writes and reject unsupported dispatch-key/per-key destinations, invalid options and unsafe timer delays explicitly.
- Add independent-process installed PostgreSQL tests for JSON/null/date values, occurrence deduplication, timezone, misfire/overlap and post-scheduler worker execution.
- Add startup-drift/rollback and in-flight tick shutdown regressions. No dependency version, migration envelope or required consumer peer changes.

### Native PostgreSQL transactional outbox

- Add opt-in outbox stores sharing existing native pools and the private application runtime.
- Add MqOutboxService diagnostics and a typed postgresOutbox transaction client with query/append, multi-record and generated-ID workflows.
- Commit domain SQL and outbox rows atomically, poison commit after caught operation failures, drain admitted work and reject escaped transaction handles.
- Preserve native custom parsers while using the existing corrected JSON view for adapter SQL.
- Add managed publisher roles, stable request IDs, independent publication retries and full destination/dispatch-key duplicate checks.
- Qualify uncommitted invisibility, rollback, scalar/null payloads, competing publishers and replay after enqueue-before-ack using installed Node/Bun PostgreSQL consumers.
- Separate public transaction types from engine/resource implementations; internal dependencies and tooling remain unchanged.
- Validate unsupported identical cross-connection handler identities before worker resource acquisition.

### PostgreSQL JSON fidelity — issue #5

- Preserve scalar string payloads/results without reinterpreting JSON-looking text as another type.
- Distinguish valid JSON null results from missing SQL NULL, including persisted attempt results.
- Add private per-query JSON/JSONB parser normalization without mutating native/global parsers, stored JSON, pool ownership or consumer dependencies.
- Preserve native client disposal on query failure and handle active connection errors without an unhandled process error.
- Qualify 18 JSON values across ordinary/controlled queues, owned/borrowed/custom-parser pools, batches, preparation, retries, typed failures and application restarts through installed tarballs.
- Retain direct native transaction/rollback/notification/disconnect regressions in read-only PostgreSQL CI. No migration or automatic replay/data repair is performed.

### M3.1a — Distributed controls

- Add QueueControls declarations for global concurrency, per-dispatch-key concurrency and fixed-window admission rates.
- Add typed dispatchKey callbacks on decoded payloads, conflict/reserved-key checks and required-key validation before publication, preparation or batch writes.
- Add default read-only policy validation and explicit coordinated reconciliation with group checks, unchanged revisions and omission safety.
- Add MqQueueControlsService with facade-only snapshots and deployment reports.
- Route claims, settlement, release, cancellation and stalled recovery through existing controlled-store operations without changing raw persistence tokens or opening another pool/runtime.
- Add real PostgreSQL package qualification with independent Node worker processes, audited claims, fixed-window identities and cancellation permit reuse; parent consumers compile with TS6/7 and run Node/Bun.
- Preserve optional dependencies, the 20 upstream tooling files and read-only retained CI.

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

Still pending: external ORM transaction bridges, additional drivers and durable event integration. Version remains 0.0.0; no npm release has been published.

### PR #4 completion and packaging correction

- Correct the controlled heartbeat/settlement race with bounded refresh of explicitly rejected stale-clock mutations, without replaying handlers or granting expired/replaced leases.
- Cover real PostgreSQL clock races, duplicate acknowledgments, cancellation, retry delays and bounded retry safety.
- Move the PostgreSQL/outbox adapters to normal internal dependencies alongside the engine; consumers no longer install any better-effect package manually.
- Test actual tarballs whose application manifest has no internal engine/adapter dependencies.
- Retain issue #5 as a separate scalar-payload qualification item and keep the package unreleased.
