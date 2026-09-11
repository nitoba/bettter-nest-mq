# Implementation roadmap

## Current delivery

M0 foundation, M1 contracts, M2 engine/PostgreSQL lifecycle and **M3 core producer/worker execution** are implemented. Jobs now run through the public Nest API and the existing durable supervisor. This is not yet full feature parity with better-effect-mq and is not published to npm.

README.md and docs/contracts.md, docs/connections.md and docs/execution.md describe actual supported behavior. Remaining milestones are not exported as simulated methods.

## M0 — Foundation — implemented

Bun lockfile and scripts; exact upstream Oxlint/Oxfmt/plugin with hash checks; TypeScript 6 floor and independent compatibility check; strict compilation; Nest dynamic modules; ESM/declarations; publint; genuine packed-consumer tests; read-only CI.

## M1 — Contracts — implemented

Typed inert QueueService/JobDefinition properties; Queue/Job/Retry/JobTimeout metadata; stable identities and versions; immutable policy precedence; Standard Schema input/output/failure inference; explicit inverse codecs and optional Zod integration. Real Nest registration validates complete snapshots and supports ordinary module re-exports. Contracts stay singleton/static and independent of worker providers.

## M2 — Engine and connection lifecycle — implemented

One private runtime per configured application context; named stores; protocol/capability checks; safe readiness/probes; failed-acquisition rollback; cooperative ordered cleanup. Contract-only contexts allocate no runtime. PostgreSQL borrows or owns pools explicitly, validates existing schemas and offers deliberate deployment migrations. Tests verify persistence, namespace identity, ownership, idle-client failures and package subpath isolation.

## M3 — Core producers and workers — implemented

Public enqueue/enqueueDecoded/enqueueMany, prepare, poll, awaitResult, execute, attempts, cancel, retry and promote now use actual engine operations. Schemas encode and revalidate payloads/results/failures, preserve decoded codecs, reject unbound clients and scope lookups/mutations to a job contract. Wait timeout/abort does not cancel the job. Preparation does not publish or automatically join a transaction.

Worker/Process/JobData/JobContext map Nest class providers onto the upstream supervisor in the same runtime. Implemented behavior includes local worker/handler concurrency, known failures versus defects, fixed/linear/exponential retries, execution timeout, cooperative active cancellation, attempt-local scoped dependencies, inherited processors, producer-only mode and shutdown draining. Unexpected defects do not retry by default; true is explicit opt-in.

Real PostgreSQL packed consumers verify producer exit, consumer startup in a separate application context, idempotency, retries, typed failures, decoded results and persistence after both contexts close. Unit/integration tests also cover lifecycle, cancellation, concurrency, identity guards and invalid registration.

## M3.1 — Execution extensions — next

Add storage-backed global/per-key concurrency and rate-limit declarations with actual distributed tests across multiple application contexts/replicas. Resolve named/versioned custom retry providers through DI without serializing executable functions. Define a genuine MQ execution-context pipeline before supporting Nest guards/pipes/interceptors/filters; current HTTP metadata is rejected rather than ignored. Add durable-event wakeups for result waits and a precise cursor/checkpoint contract before claiming subscription guarantees.

Expand supervisor/adapter crash, lease-loss, activation-failure and non-cooperative shutdown scenarios at the facade boundary. Preserve inherited metadata and request-scoped dependency isolation. These extensions may proceed in parallel where their contracts do not overlap.

## M4 — Additional adapters and resource integrations

PostgreSQL JobStore execution is available. Add MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers with optional drivers and their true topology/transaction guarantees. Reuse upstream conformance suites and real-server failure tests. Extend each connection bundle with flow/event/schedule/outbox stores as those features are implemented, sharing native resources instead of opening redundant pools.

## M5 — Durable flows and schedules

Implement Flow/FanOut/Collect bindings using persisted parent/children coordination, stable manifests, bounded child counts/depth, paginated collection and defined failure/cancellation policies. A waiting parent must not occupy a worker slot. No Promise.all substitute for durable state.

Implement persistent cron/interval schedules, time zones, misfire/overlap policies, administration and rolling-deployment-safe reconciliation. Test competing schedulers and restart windows. Dynamic work belongs in a coordinator job, not a serialized function or timer per replica.

## M6 — Transactional outbox

Build explicit transaction/append/publish APIs around the already implemented prepared request. The outbox write and domain write must share the same real transaction resource. Add adapter-specific contexts and independently tested ORM bridges. Test rollback, publish-after-commit, replay after enqueue-before-ack, conflicting IDs, independent publication/execution retries and borrowed resource ownership.

The upstream outbox package currently exists as a PostgreSQL adapter peer. That dependency alone is not the Nest transactional outbox facade. Keep application outbox separate from internal flow coordination.

## M7 — Operations and release hardening

Extend local workers and connection probes into job/flow/schedule/outbox administration, durable event cursors, observability and operational examples. Any management HTTP endpoints require opt-in authentication/authorization. Validate every optional entry point through installed tarballs, run distributed crash/restart/shutdown tests and define version compatibility before production release. No exactly-once external-side-effect promise or cross-database atomicity claim.

## Dependencies

```text
M0 → M1 → M2 → M3 core
                  ├──→ M3.1 execution extensions
                  ├──→ M4 additional adapters/resource bundles
                  ├──→ M5 flows/schedules (required stores first)
                  └──→ M6 outbox (transaction bridges first)
        M3.1 + M4 + M5 + M6 → M7 full parity/release hardening
```

Adapter wrappers can proceed independently after agreeing on resource/capability boundaries. Flow and schedule work can proceed in parallel once their required stores exist. Do not distribute overlapping lease/settlement/outbox protocol changes without shared contracts and failure tests.
