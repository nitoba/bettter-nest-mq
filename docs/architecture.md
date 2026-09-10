# Approved architecture

## Implementation status

M0 foundations, M1 typed contracts/Nest registration and M2 engine lifecycle are implemented. The public surface includes modules, contracts/decorators, schema helpers, the registry, opaque connections and connection readiness. Optional subpaths provide Zod codecs and PostgreSQL configuration/migration helpers.

The private host now owns a real runtime and named JobStores. Public producer/worker APIs, executable job-policy compilation, other adapter wrappers, flows, schedules, transactional outbox and administration remain planned. See docs/contracts.md and docs/connections.md for implemented behavior; do not treat the targets below as available APIs.

## Composition and ownership

The public API is Nest 12 ESM: Modules, Services, decorators and Promises. The core engine remains better-effect-mq; its Effect/Result/Layer/Runtime types are private. Native database types are isolated behind optional integration subpaths. The root works without Zod, pg or the PostgreSQL adapter installed.

Nest owns business services. Each configured application context owns one private runtime covering its named stores. Contract-only applications allocate none. Descriptor definitions are inert; they never hold a shared runtime or pool acquired on behalf of another context. Owned pools are created during startup; borrowed pools are never closed by the facade.

Startup validates queue references, resolves named tokens, checks protocol/capabilities and probes real stores before publishing connection readiness. The host explicitly invokes idempotent contract registration rather than relying on concurrent provider-hook ordering. Acquisition failure rolls back locally: Nest can rethrow failed initialization from close() before running destruction hooks.

Shutdown closes admission, drains/cooperatively aborts runtime operations, releases scoped adapter resources, then closes facade-owned resources. Cleanup is memoized, continues through individual failures, and aggregates errors. It does not forcibly interrupt arbitrary JavaScript or guarantee cancellation of every non-cooperative database query.

## Contracts and validation

Queue Services declare inert, versioned jobs through typed properties. Queue selects connection/queue identity; Job supplies the durable job name/version. Shared producer declarations own retry/timeout policies. The typed this.job helper preserves schema inference without pretending decorators can infer TypeScript generics.

Discovery uses actual registered Nest singleton providers, validates complete snapshots and rejects duplicate identities, missing metadata, accessors and scoped dependency trees. Aliases of the same provider instance are deduplicated. Each concrete QueueService declares its own queue identity. Feature module re-exports use the public MqModule class without root-only providers leaking into feature imports.

Standard Schema defines validation. Zod is optional. Input, decoded value and persisted JSON remain separate types; non-JSON values/non-idempotent transformations need explicit inverse encoding. JSON fidelity and codec round trips are checked. Validators and encoders must be deterministic and side-effect-free. HTTP validation never substitutes for queue boundary validation.

Known domain failures have typed content validated by the job contract before persistence. Schema issues, encoding errors, throwing vendors and invalid declarations have distinct facade errors. Promise methods do not acquire checked exceptions. Custom retry declarations identify named/versioned policies rather than serializing executable functions.

## Persistence compatibility

PostgreSQL delegates to the upstream adapter/migrator, not copied queue SQL. Startup validates the existing schema and never applies migrations automatically. Deployment helpers are explicit. pg idle-client failures are handled for owned pools without logging raw client/credential objects; borrowed-pool error handling belongs to its owner.

The connection name is part of durable identity. The facade's stable `nestjs/<name>` token is hashed into the upstream PostgreSQL namespace. Changing a connection name does not reopen the same stored jobs, even with the same URL/schema/configured namespace. Keep this token scheme stable and require an explicit migration design before changing it.

The configuration boundary checks exact duplicate descriptors/pool-or-string identities; it does not discover arbitrary physical-database aliases. Capabilities are validated using the real upstream protocol. Advertised readiness capabilities do not imply every corresponding public facade operation has already been implemented.

## Producer and worker targets

Job compilation, enqueue/bulk, query/wait, prepare, retry/cancel/promote and attempts will use the current lifecycle-managed stores. execute means publish-and-wait, not calling a local handler. A wait timeout does not imply cancelling the durable job. Named connections must match across producer and worker deployments.

Worker Services will implement decorated Process methods with local concurrency, heartbeat, leases and stalled recovery delegated to the engine. Global/per-key concurrency and rate limits must remain storage-backed. The execution context explicitly supports appropriate Nest enhancers; invoking a method directly is not equivalent to an HTTP pipeline. Attempt-scoped handler dependencies use a job context, not a fabricated HTTP request. Producer-only modules do not import workers or start consumers.

## Durable feature targets

Flows retain the engine's persisted parent/child fan-out and collection model: inert child plans, stable keys/manifests, bounded/paginated results and defined failure policies. Waiting parents do not occupy a worker slot. This is not arbitrary function replay or automatic saga compensation.

Schedules persist cron/interval, timezone, misfire and overlap decisions. Reconciliation must be safe during rolling deployments and competing replicas. Dynamic work belongs in a coordinator job, not a serialized function or per-replica timer.

Application outbox records share the actual domain transaction. Preparation, append, post-commit publication and settlement remain separate, with independent publication/job retries. Stable IDs detect conflicting duplicates. SQL, MongoDB and Redis transaction guarantees are not interchangeable; each ORM bridge must prove resource identity. The internal flow coordination outbox is distinct from the application's transactional outbox. The currently installed upstream outbox peer does not implement these Nest APIs.

## Operations and non-goals

Administration, local callbacks, durable events, metrics and health probes have distinct contracts. Local callbacks are not durable subscriptions. Persisted job state remains authoritative even when events wake consumers. Do not advertise consumer groups/checkpoints without implementing their persistence.

No automatic HTTP management endpoints, production migrations, process signal handlers, cross-database atomicity or exactly-once external-side-effect promise. Cancellation is cooperative; lease fencing protects persisted settlement, not effects already executed in external systems.
