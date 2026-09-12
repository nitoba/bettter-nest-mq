# Named connections and the private engine lifecycle

## Current scope

The private host now serves both M2 connection management and M3 public producers/workers. One better-effect runtime per configured Nest application owns named JobStores, Clock, lazy Worker supervisors and explicitly enabled outbox stores/publisher. Public APIs expose Nest Services and Promises rather than engine handles. PostgreSQL is the first production adapter; the root remains independent of pg and Zod.

Connection factories return opaque inert descriptors. They never open a pool when a module is merely declared or imported. Each application acquires its own resources from those descriptors; no acquired runtime is shared globally. See execution.md for job/worker operations using these connections.

## Startup modes

Omitting connections retains contract-only registration: state is disabled, no runtime/store exists, and producers/probes reject explicitly. This is not a memory fallback. An empty explicit connection map supports no registered queues and acquires no stores.

With a map such as `{ primary: postgres(...) }`, bootstrap validates contracts, worker bindings and queue connection references before acquiring stores. It resolves each named token, checks the upstream protocol and requested capabilities, and probes the actual store. Only a complete successful pass publishes connection readiness. Producers are then bound and lazy Workers activated in the same runtime. Failure detaches bindings and rolls back acquired resources.

Use one forRoot registration per application context. Feature imports register/re-export queue contracts without duplicating the root engine. An application with `execution: { workers: false }` can publish without starting workers. Importing only Queue Services also starts no consumers.

Exact repeated descriptors or the same pool/connection-string boundary plus schema/namespace under different names are rejected as ambiguous configuration. This is a conservative configuration identity check, not a universal detector of URLs or DNS names pointing at one physical server.

## PostgreSQL configuration

Import postgres from better-nest-mq/postgres. Consumers select pg and its TypeScript types; better-effect-mq-postgres and better-effect-mq-outbox are automatically installed internal dependencies of this library, not peers the application must manage. The native transactional-outbox facade is enabled explicitly with outbox:true on a connection; see outbox.md for managed transactions and independent publisher roles. See dependencies.md for the complete installation boundary.

```ts
const owned = postgres({
  connectionString: databaseUrl,
  schema: 'mq',
  namespace: 'billing',
  max: 10,
  connectionTimeoutMs: 10_000,
  idleTimeoutMs: 10_000,
  validateSchema: true,
  requireCapabilities: ['globalConcurrency', 'rateLimiting']
})

const borrowed = postgres({ pool, schema: 'mq', namespace: 'billing' })
```

Owned pools are created at startup. max must be at least two because the adapter reserves a listener and needs query capacity; the default is ten. Connection timeout is positive, idle timeout may be zero. Advanced native pg options are available through a caller-created borrowed Pool.

Borrowed pools are not frozen, closed or given pool-level error listeners by the facade. Their owner handles idle-client errors and closes the pool only after MQ resources finish. Shared pools require enough capacity for listener reservations and application traffic; the upstream adapter validates its requirements.

Owned pools install a safe Nest logger handler for idle-client errors. pg removes failed idle clients and replaces them on demand. The handler prevents unhandled pool errors from crashing the process and avoids logging raw client objects that can expose credentials. This is not automatic transaction retry or proof of transparent recovery from every outage. Failed operations still report errors.

## Durable addresses

The facade uses the stable named-store token `nestjs/<connection-name>`. The upstream PostgreSQL adapter hashes that service tag into its effective namespace. The durable location therefore depends on database, schema, configured namespace and connection name.

A producer and worker using the same name/schema/namespace see the same jobs, even after their application contexts are recreated. Changing alpha to primary opens another named store even with the same database URL. Do not casually refactor the token prefix or rename connections; migration of these addresses is not currently implemented. Class/property names are not job identity components.

Tests cover persistence with identical names and isolation after a rename. Full packed execution tests also close the producer, start a worker in another context, then reopen a reader and verify the completed results and attempt ledger.

## Explicit migrations

migratePostgres({ pool, schema }) delegates to the upstream migrator and returns a frozen report containing schema, version and applied versions. Re-running on an up-to-date layout applies nothing. validatePostgres({ pool, schema }) verifies the existing layout without modifying it. Both borrow their pool and leave closure to the caller.

Module startup defaults to validation and never executes migrations automatically. Missing/incompatible layouts fail readiness and trigger rollback. validateSchema:false skips the migrator's layout check only; the real store probe must still succeed and creates no tables. Use deployment privileges for migration and appropriately restricted runtime credentials; the library does not grant permissions or install migration endpoints.

## Readiness

MqConnectionsService exposes state, connections() and probe(name). The state is a lifecycle value: idle, disabled, starting, ready, stopping, closed or failed. Connections returns immutable successful-startup snapshots, empty before readiness and after failure/shutdown. Probes perform live store access.

Snapshots contain connection name, adapter/version, protocol/layout version, declared capabilities and ownership; no credentials, pool or runtime references. State ready is not continuous connectivity monitoring. Probes currently use the store counts query, whose cost depends on job volume; avoid high-frequency polling. No health HTTP endpoint or timer is installed.

Unsupported required capabilities fail before readiness. An advertised capability does not imply its public facade operation is already available: QueueControls now exposes global/per-key concurrency and rate-limit declarations through the controlled-store protocol; see controls.md for qualification and deployment rules. MqWorkersService independently exposes local worker status and awaitIdle; local idle is not a statement that every queued/delayed job has completed.

Failures use MqConnectionException with connection/phase/cause or MqEngineStateException for invalid lifecycle operations. Public job methods add their own operation errors. Causes are trusted diagnostics and may contain infrastructure details; do not serialize them directly to untrusted HTTP clients. Multiple cleanup failures are aggregated instead of silently hiding the startup cause.

## Shutdown and rollback

The host detaches clients and stops admission before cleanup. Admitted operations follow the configured grace/cooperative-abort policy. The upstream Worker layers quiesce and drain attempts before scoped stores are released, and facade-owned pools close afterward. Concurrent/repeated close uses one cleanup promise, avoiding duplicate resource release. Cleanup continues through failing resources and reports the collected errors.

If close races startup, readiness must not be published after the stop request; late acquisition is released. Acquisition/activation failures perform their own rollback, because Nest close may rethrow a failed initialization before invoking destruction hooks. No automatic process-level signal handling is added; the application controls normal Nest shutdown hooks.

Cancellation is cooperative. Runtime abort is not forced JavaScript interruption, guaranteed pg query cancellation or rollback of external effects. The grace period is not a hard deadline for arbitrary non-cooperative code. Active job cancellation uses a persisted request and fenced supervisor settlement, as described in execution.md.

## Tests and remaining integration

Tests use explicit internal upstream memory fixtures and a real PostgreSQL service, never a production fallback. They cover acquisition, capability failures, independent contexts, close races, cleanup errors, ownership, explicit migrations, durable identity and packed consumers. Public tarball tests exercise Node/Bun with TypeScript 6/7, including intentional idle-client termination and end-to-end worker execution.

Native PostgreSQL outbox resources now share their existing pool and runtime. Other storage wrappers, flow/schedule/event bundles and true ORM transaction bridges remain planned. New resource bundles should share native connections and preserve these ownership/lifecycle guarantees rather than opening redundant pools.

## JSON fidelity and native parser isolation

Issue #5 is corrected by a private, non-owning adapter pool/client view. JSON and JSONB fields reach the pinned adapter as encoded text, so its decoder parses exactly once and preserves strings, JSON null and other valid JSON values. Native application queries, custom parsers, global pg configuration and pool ownership remain unchanged. The format on disk is unchanged and needs no migration.

The real database/package matrix covers 18 JSON values in ordinary and controlled queues, including single/decoded/batch publication, preparation, results, typed failures, retries and post-restart reads. Native driver regressions also check transaction identity, rollback, LISTEN notifications, SQL query failures and active connection termination. Correctly stored data remains readable; previously misinterpreted business results are not automatically repaired or replayed. See [PostgreSQL JSON fidelity](postgres-json.md) for the pinned compatibility boundary and operational precautions.

## Persistent schedule integration

Persistent Schedule declarations and MqSchedulesService are now implemented; see schedules.md for the supported API and exact recurrence semantics. Schedule stores opt in with postgres({ schedules: true }), sharing the existing pool, private JSON view and stable namespace. Definition deployment/validation and scheduler execution are independent from workers and the outbox publisher.

This does not add flows or external ORM transactions. The pinned schedule protocol cannot carry dispatch keys, so keyed/per-key-limited schedules reject explicitly. Normal startup preserves operator pauses and validates deployed definitions; explicit reconciliation is a coordinated administrative operation, not a cross-store transaction.

## Event-assisted wait integration

Event-assisted awaitResult/execute is implemented; see event-waits.md for the API and exact scope. Configure postgres({ events: true }) on the waiting application and select strategy: 'events' with pollFallbackMs. Polling remains the default. The raw event token shares the job namespace; only an in-runtime operation alias is added. No second pool/runtime, automatic migrations or required-writer activation is introduced.

Runtime reader errors/lost hints use the existing bounded fallback; missing explicit reader configuration still fails. Timeout and abort do not cancel durable work. Keep tests for shutdown, parser fidelity and installed consumers. Resumable subscriptions, checkpoint APIs and retention administration remain pending; this is not a promise of zero polling.
