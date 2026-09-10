# Named connections and the private engine lifecycle

## Scope

M2 connects Nest to the actual better-effect Runtime and JobStore protocol. It adds named connection acquisition, readiness and cleanup, with PostgreSQL as the first optional production adapter. Public producer/worker methods are deliberately not implemented yet. The storage persistence tests call the real engine internally; this is not an advertised enqueue API.

A connection descriptor is inert and opaque. It contains only public adapter/ownership metadata; the internal factory is not exported. A registry of inert descriptors does not share runtimes or resources between application contexts. Each configured application owns one runtime for all of its named stores, not a runtime per connection, job or request.

## Registration modes

`MqModule.forRoot({})` retains contract-only registration. No runtime exists, connection state is `disabled`, and probes reject explicitly. This is useful for metadata/type tests and does not provide an implicit memory queue.

With `connections: { primary: postgres(...) }`, startup first validates registered queues and connection names, then acquires resources, resolves named store tokens, checks the upstream protocol and requested capabilities, and probes each actual store. Readiness appears only after the complete pass succeeds. An empty explicit map supports no queues and opens no resources.

Missing references fail before resource acquisition. Reusing the same descriptor, or exactly the same pool/connection-string boundary plus schema and namespace under another name, is rejected as ambiguous configuration. This is a conservative configuration-identity check, not a universal detector of URL/DNS aliases pointing at the same physical database.

Use one `forRoot` registration per application context. Feature modules still register/re-export contracts through `MqModule.forFeature` and `exports: [MqModule]`; they do not duplicate the engine host.

## PostgreSQL configuration

Import `postgres` from `better-nest-mq/postgres`. The root entry point does not import pg or the PostgreSQL adapter. Consumers of this subpath install `pg`, `@types/pg` when compiling TypeScript, `better-effect-mq-postgres@0.1.3` and its `better-effect-mq-outbox@0.1.3` peer. The latter is an upstream package dependency, not a claim that Nest outbox is implemented.

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

An owned connection uses a PostgreSQL URL and creates a new pool only during startup. `max` must be at least two because the adapter reserves a listener connection and needs query capacity. The default is ten. Connection timeout must be positive; idle timeout may be zero. Advanced native pg configuration can be supplied through a caller-created borrowed Pool.

Borrowed pools retain the caller's ownership. The facade neither freezes the pool nor installs pool-level error listeners on it. Its owner must handle pg idle-client errors and arrange shutdown after all MQ users have released their adapter resources. Shared pools need enough capacity for all listeners and application queries; the upstream adapter also validates its reservation requirements.

Owned pools install a safe Nest logger handler for idle-client errors. pg removes those clients and creates replacements on demand. This prevents an unhandled pool `error` event from crashing the Node process; it is not a retry policy for failed business transactions or proof of transparent recovery from every database outage. Failed queries still reject their operation. Raw pg error/client objects are not logged by the handler because they can contain credentials and backend session secrets.

## Durable identity and namespaces

Connection names are not freely renameable DI aliases. The facade uses the stable named-store token `nestjs/<connection-name>`, and the upstream PostgreSQL adapter hashes its service tag into the storage namespace. Therefore the durable location depends on the database, schema, configured namespace and connection name.

A subsequent application using the same name, schema and namespace reopens existing jobs. Changing `alpha` to `primary` intentionally opens another named store even when the URL and configured namespace are unchanged. The `nestjs/` token prefix is part of this persistence compatibility boundary and must not be casually refactored. Class/property names remain outside persisted job identity.

The real PostgreSQL tests cover both preservation under identical names and isolation after renaming. Migration of connection identities is not implemented; keep names stable across deployments and between future producer/worker processes.

## Migration deployment

`migratePostgres({ pool, schema })` is an explicit deployment helper calling the upstream migrator. It returns a frozen `{ schema, version, applied }` report. A second invocation on an up-to-date schema has an empty `applied` list. `validatePostgres({ pool, schema })` returns `{ schema, version }` without applying changes. Both borrow the supplied pool and leave its closure to the caller.

Normal module startup defaults to `validateSchema: true` and never invokes migration execution. A missing/incompatible schema fails readiness and rolls back acquired resources. Setting validation false only skips the migrator's layout check; the real store probe still has to succeed and does not create tables.

Run migrations with appropriate deployment privileges and use restricted application credentials afterward. This library does not grant database permissions or expose migration HTTP routes.

## Readiness and failures

`MqConnectionsService` exposes:

| Member          | Meaning                                                                               |
| --------------- | ------------------------------------------------------------------------------------- |
| `state`         | Lifecycle state: idle, disabled, starting, ready, stopping, closed or failed          |
| `connections()` | Frozen successful-startup snapshot, empty before readiness and after shutdown/failure |
| `probe(name)`   | Promise performing a live store query and returning its safe descriptor               |

Descriptors include adapter/version, protocol/layout version, declared capabilities and ownership. Credentials, pools, storage tokens and runtime handles never appear. `state: ready` is not continual health monitoring; use a probe when checking current connectivity. Probes currently use the store's counts query, so their cost depends on the store and job population; avoid aggressive high-frequency polling.

Connection configuration/acquisition/protocol/probe/migration failures use `MqConnectionException`, carrying a named connection, phase and original cause. Invalid lifecycle operations use `MqEngineStateException`. Error causes are for trusted diagnostics and can contain driver details; do not serialize them directly to untrusted HTTP clients. Multiple cleanup failures are reported through AggregateError rather than replacing the primary startup failure silently.

Capability checks use the upstream protocol descriptor. Requiring a capability that the adapter does not declare fails startup. Advertising a capability in readiness does not mean this milestone exposes its corresponding queue control API.

## Shutdown and rollback

The host stops admission before releasing resources. Admitted runtime operations follow the configured grace period and cooperative-abort policy. Adapter scoped finalizers run before facade-owned pools close, and cleanup continues through all owned handles even when one fails. Concurrent/repeated close operations use the same cleanup promise and do not release resources twice.

If close races startup, readiness is never published after the stop request; an acquisition that finishes later is cleaned up. If startup fails halfway through several stores, its own failure path performs rollback. This matters because Nest can rethrow a failed initialization promise from `close()` before destruction hooks run.

Cancellation remains cooperative. A runtime abort does not forcibly interrupt JavaScript, kill every pg query or undo an external effect. Do not interpret the grace period as a hard wall-clock guarantee for an arbitrary non-cooperative driver operation. No process-level signal handlers are installed; the consuming Nest application controls signal handling and calls its normal shutdown hooks.

## Verification and next boundary

Tests use real upstream memory stores only as explicit internal fixtures, plus a real PostgreSQL server. They verify atomic startup, scope/protocol errors, independent contexts, concurrent close, cleanup failures, borrowed/owned pools, no implicit migrations, durable persistence and connection renaming. External tarball tests additionally verify optional subpaths and deliberate idle-client termination under both Node and Bun.

The next milestone binds typed JobDefinitions to these resources and implements real producer and Worker/Process APIs. Contract compilation, durable retry execution, flows, schedules and transactional outbox remain separate follow-on work, not simulated methods on this host.
