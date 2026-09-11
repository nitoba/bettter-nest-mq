# Named connections and the private engine lifecycle

## Current scope

The private host now serves both M2 connection management and M3 public producers/workers. One better-effect runtime per configured Nest application owns named JobStores, Clock and lazy Worker supervisors. Public APIs expose Nest Services and Promises rather than engine handles. PostgreSQL is the first production adapter; the root remains independent of pg and Zod.

Connection factories return opaque inert descriptors. They never open a pool when a module is merely declared or imported. Each application acquires its own resources from those descriptors; no acquired runtime is shared globally. See execution.md for job/worker operations using these connections.

## Startup modes

Omitting connections retains contract-only registration: state is disabled, no runtime/store exists, and producers/probes reject explicitly. This is not a memory fallback. An empty explicit connection map supports no registered queues and acquires no stores.

With a map such as `{ primary: postgres(...) }`, bootstrap validates contracts, worker bindings and queue connection references before acquiring stores. It resolves each named token, checks the upstream protocol and requested capabilities, and probes the actual store. Only a complete successful pass publishes connection readiness. Producers are then bound and lazy Workers activated in the same runtime. Failure detaches bindings and rolls back acquired resources.

Use one forRoot registration per application context. Feature imports register/re-export queue contracts without duplicating the root engine. An application with `execution: { workers: false }` can publish without starting workers. Importing only Queue Services also starts no consumers.

Exact repeated descriptors or the same pool/connection-string boundary plus schema/namespace under different names are rejected as ambiguous configuration. This is a conservative configuration identity check, not a universal detector of URLs or DNS names pointing at one physical server.

## PostgreSQL configuration

Import postgres from better-nest-mq/postgres. This subpath needs pg, its TypeScript types when compiling, better-effect-mq-postgres@0.1.3 and the adapter's better-effect-mq-outbox@0.1.3 peer. The peer does not mean a Nest transactional outbox facade is implemented.

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

Unsupported required capabilities fail before readiness. An advertised capability does not imply its public facade operation is already available: distributed-control APIs remain a planned extension. MqWorkersService independently exposes local worker status and awaitIdle; local idle is not a statement that every queued/delayed job has completed.

Failures use MqConnectionException with connection/phase/cause or MqEngineStateException for invalid lifecycle operations. Public job methods add their own operation errors. Causes are trusted diagnostics and may contain infrastructure details; do not serialize them directly to untrusted HTTP clients. Multiple cleanup failures are aggregated instead of silently hiding the startup cause.

## Shutdown and rollback

The host detaches clients and stops admission before cleanup. Admitted operations follow the configured grace/cooperative-abort policy. The upstream Worker layers quiesce and drain attempts before scoped stores are released, and facade-owned pools close afterward. Concurrent/repeated close uses one cleanup promise, avoiding duplicate resource release. Cleanup continues through failing resources and reports the collected errors.

If close races startup, readiness must not be published after the stop request; late acquisition is released. Acquisition/activation failures perform their own rollback, because Nest close may rethrow a failed initialization before invoking destruction hooks. No automatic process-level signal handling is added; the application controls normal Nest shutdown hooks.

Cancellation is cooperative. Runtime abort is not forced JavaScript interruption, guaranteed pg query cancellation or rollback of external effects. The grace period is not a hard deadline for arbitrary non-cooperative code. Active job cancellation uses a persisted request and fenced supervisor settlement, as described in execution.md.

## Tests and remaining integration

Tests use explicit internal upstream memory fixtures and a real PostgreSQL service, never a production fallback. They cover acquisition, capability failures, independent contexts, close races, cleanup errors, ownership, explicit migrations, durable identity and packed consumers. Public tarball tests exercise Node/Bun with TypeScript 6/7, including intentional idle-client termination and end-to-end worker execution.

Other storage wrappers, flow/schedule/event/outbox resource bundles and true ORM transaction bridges remain planned. New resource bundles should share native connections and preserve these ownership/lifecycle guarantees rather than opening redundant pools.
