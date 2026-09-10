# Approved architecture

## Implementation status

M0 foundations and M1 typed contracts/Nest registration are implemented. The current API includes MqModule, MqConfiguration, MqRegistry, QueueService/JobDefinition, Queue/Job/Retry/JobTimeout, schema helpers and the optional Zod codec subpath. See docs/contracts.md for operational details of those APIs. The engine host, database adapters, producers, workers, flows, schedules and outbox described below remain planned.

## Composition

A Nest 12 ESM library exposes Modules, Services, decorators, schemas and Promise-returning methods. The planned engine is better-effect-mq; Effect/Result/Layer/Runtime types remain private. Database integrations use optional entry points rather than eager imports from the root.

Nest owns business services. One private engine host per application context will own its runtime and named stores. Borrowed pools must never be closed by the library; owned pools must close exactly once. Acquisition, startup, drain, cancellation and release are distinct lifecycle stages. Failed acquisition needs local rollback because Nest can rethrow failed initialization from close() before running destruction hooks.

## Contracts and execution

Queue Services declare inert, versioned jobs through typed properties. Queue selects identity and connection; Job identifies the durable contract; retry and timeout policies belong to the shared producer definition. A typed this.job helper preserves schema inference without pretending decorator reflection can infer TypeScript generics.

M1 discovery operates over registered singleton Nest providers and commits an immutable snapshot only after complete validation. It rejects duplicate identities, missing decorators and scoped contract dependency trees. Connection names currently remain metadata; capability/resource checks belong to the engine bridge. Provider aliases are deduplicated by actual instance identity.

Worker Services will implement Process methods. Worker settings control local concurrency, leases and heartbeats; distributed queue controls remain storage-backed. Producer-only applications must not need processor providers. The execution pipeline must explicitly support applicable Nest enhancers: directly calling a method is not equivalent to an HTTP pipeline. Request-scoped handler dependencies need an attempt-specific context, not a fabricated HTTP request.

## Validation and failures

Standard Schema is the validation contract; Zod is optional and never required by the root entry point. Validate publication/preparation, persisted input, handler output and persisted results/failures. HTTP validation is not a replacement for queue validation.

Schema input, decoded value and wire JSON are distinct. Non-JSON values and non-idempotent transformations require an explicit inverse. M1 checks JSON fidelity and codec round trips, supports asynchronous validators/encoders and rejects lossy encoding. Validators are expected to be deterministic and side-effect-free.

Known domain failures carry typed serializable content validated through the job's failure contract. Unexpected defects, schema failures, timeout, cancellation and lease loss remain distinct. Promise-returning methods do not have checked exceptions. Custom retry declarations identify a versioned policy; they do not serialize executable functions or provider instances.

## Durable behavior to preserve

Publishing, bulk enqueue, querying, waiting, cancellation, promotion, retries and attempt history will delegate to the existing engine. execute means enqueue-and-wait, not an in-process handler call. A client's waiting timeout does not imply cancellation of its durable job.

Flows retain the persisted parent/children fan-out and collection model. Fan-out constructs an inert child plan; parents waiting for children must not hold a live worker slot. Preserve stable child keys, manifest recovery, bounded/paginated collection, failure policies and cross-connection capability checks. This is not arbitrary function replay or automatic saga compensation.

Schedules persist cron/interval definitions, time zones, misfire and overlap decisions. Reconciliation must be safe during rolling deployments. Dynamic work belongs in a coordinator job, not a serialized function or in-memory per-replica timer.

Application outbox writes share the actual business transaction. Preparation, transactional append, post-commit publication and settlement stay separate. Publication retries and job retries are independent. Stable IDs detect conflicting duplicates; republish windows remain at-least-once. SQL, MongoDB and Redis transactional semantics are not interchangeable, and each ORM bridge must prove transaction-resource identity.

The internal flow coordination outbox is separate from the application's transactional outbox.

## Operations and non-goals

Administration, local worker callbacks, durable events, metrics and health probes are separate contracts. Local callbacks are not durable subscriptions. Events wake waiters; persisted state is authoritative. Do not advertise consumer groups/checkpoints without implementing their persistence.

No automatic administrative HTTP endpoints, production migrations, forceful JavaScript interruption, cross-database distributed transactions or exactly-once external side effects. Cancellation is cooperative. Lease fencing protects settlement, not arbitrary external effects already executed.
