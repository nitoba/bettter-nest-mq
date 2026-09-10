# Approved architecture

## Status boundary

This document records the target architecture approved before implementation. The bootstrap implements module configuration and the development/package infrastructure only. Except for MqModule and MqConfiguration, the API names below are design targets and are not currently exported.

## Composition

A single-package Nest 12 ESM library exposes Modules, Services, decorators, schemas and Promise-returning methods. The engine remains better-effect-mq; its Effect/Result/Layer/Runtime types are private. Database wrappers will use separate optional entry points, not eagerly imported root dependencies.

The Nest container owns business services. One private engine host per application context owns the internal runtime and named stores. Borrowed pools are never closed by the library; owned pools are closed exactly once. Resource acquisition, start, drain, abort and release are separate lifecycle stages.

## Contracts and execution

Queue Services declare inert, versioned jobs through typed properties. @Queue selects identity and connection; @Job identifies the persisted contract; retry and timeout policies belong to the shared producer contract. A typed this.job helper preserves schema inference without relying on decorator reflection to infer TypeScript types.

Worker Services implement methods with @Process references to declared queue properties. @Worker controls local concurrency, leases and heartbeats. Queue-level distributed controls remain storage-backed and are checked against adapter capabilities. Producer-only applications can import contracts without importing processors or starting consumers.

Discovery operates over registered Nest providers. Before admission starts, reject duplicate identities, missing contracts, conflicting handler/flow registrations, invalid settings and unsupported adapter capabilities. A future execution pipeline must explicitly support applicable Nest enhancers; directly invoking a method is not equivalent to the Nest HTTP pipeline. Request-scoped providers require a job-attempt context, not a fabricated HTTP request.

## Validation and failure contracts

Standard Schema is the public validation boundary; Zod 4 is a supported implementation, not a mandatory dependency of the entire facade. Validate publication/preparation, persisted input, handler output and persisted results/failures. HTTP validation does not replace queue validation.

Schema input, decoded value and persisted JSON are distinct. Values such as Date need an explicit bidirectional codec/encoder. A one-way transform cannot be assumed to encode persisted data or safely run twice.

Known domain failures have validated serializable content. Unexpected defects, validation failures, timeout, cancellation and lease loss remain distinct. A Promise does not acquire checked exceptions by adding a failure schema. Retry policy references are stable/versioned identifiers, never serialized functions or provider instances.

## Durable behavior

Publishing, bulk enqueue, queries, awaiting results, cancellation, retries, promotion and attempts will delegate to the engine. execute means publish-and-wait, not call a local handler. A wait timeout does not implicitly cancel a durable job.

Flows retain the engine's persisted parent/children fan-out and collection model. Fan-out builds an inert child plan; parents waiting for children do not hold a live worker slot. Stable child keys, manifest recovery, paginated collection, continue/fail policies and cross-connection capability checks are required. This is not an arbitrary replay engine or automatic saga compensation.

Schedules persist cron/interval definitions, time zones, misfire and overlap policies. Startup reconciliation must be rolling-deployment-safe. Do not implement them as one in-memory timer per replica. Dynamic work is a coordinator job, not a serialized function.

Application outbox writes share the actual business transaction. Preparation, transaction append, publication after commit and settlement remain distinct. Publication retries and job execution retries are independent. Stable IDs detect conflicting duplicate requests. Republish windows remain at-least-once. SQL, MongoDB and Redis capabilities are not interchangeable; ORM bridges must prove transaction-resource identity.

The internal flow coordination outbox is separate from the business application's transactional outbox.

## Operations and non-goals

Administration, local worker events, durable events, metrics and health probes have separate contracts. Local callbacks are not durable subscriptions. Event notifications wake waiters; persisted job state remains authoritative. Do not advertise consumer groups or durable checkpoints without implementing them.

No automatic administrative HTTP endpoints, automatic production migrations, exactly-once side-effect promises, forceful JavaScript interruption or cross-database distributed transactions. Cancellation is cooperative and lease fencing protects persisted settlement, not arbitrary external effects.
