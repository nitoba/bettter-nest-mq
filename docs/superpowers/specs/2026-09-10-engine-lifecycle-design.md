# M2 — Private engine host and named connections

The user approved the facade architecture, requested the prior PR be merged, and authorized continuing. PR #1 was already merged at 3b654ae; this implementation starts from that revision without rewriting it.

## Deliverable

Connect the Nest module to one private better-effect Runtime per application context, with named JobStore tokens, real adapter acquisition, protocol/capability validation, readiness, lifecycle fencing and deterministic cleanup. Include an optional PostgreSQL integration as the first real storage slice. Keep producer/worker APIs out of this milestone rather than exporting simulated operations.

Connections are declared as inert opaque adapter descriptors. Nest configuration copies the name map without freezing caller-owned pools. The PostgreSQL factory accepts either a borrowed pg Pool or an owned connection configuration; it validates arguments without opening connections. The root entry point must not require pg or the PostgreSQL adapter. Drivers are loaded only when their explicit integration is used.

## Startup and failure handling

Contract-only forRoot registrations continue to work without connections. Once connections are configured, every registered queue must refer to one, and duplicate physical storage boundaries must not silently split one durable queue across independent configuration identities. Validate names, contracts and required capabilities before marking readiness. Create the runtime once, warm all named stores, and keep it private. A failed acquisition or readiness check releases every acquired resource, preserving the primary failure and cleanup errors. Do not depend on Nest close hooks after a failed bootstrap.

The registry must be initialized explicitly/idempotently by the host so startup never depends on concurrent provider-hook ordering. Failed engine startup clears its own state and does not publish partial connection readiness.

## Ownership and migrations

The existing PostgreSQL JobStore layer owns its adapter resources. When an existing pool is supplied, cleanup must not call pool.end. When connection settings are supplied, the facade owns the created pool and closes it exactly once after the runtime releases adapter resources. Resolve all store tokens inside the same runtime; never create a runtime per connection or request.

Validate the shipped PostgreSQL schema by default. Do not apply migrations during module startup. Offer an explicit PostgreSQL migration/validation helper for deploy scripts, translating failures into the facade's errors. Do not claim general ORM transaction bridges, flows or outbox execution in this milestone.

## Public surface and observability

MqConnectionsService exposes immutable snapshots, state and explicit readiness probes through Promises. Snapshots contain names, adapter/version/protocol capabilities and ownership, never credentials, raw pools, Layers, Results or runtime handles. The engine host and low-level store access stay internal for the next producer/worker milestone.

Connection failures retain an Error cause without copying connection strings into messages. Unavailable/closed engine operations fail explicitly, not via an implicit memory fallback. Shutdown closes admission first, drains/aborts runtime executions using the configured policy and releases owned resources. Concurrent/repeated starts and closes are idempotent; a disposed host cannot be reopened accidentally.

## Verification

Observe a failing public API regression first. Add pure lifecycle tests using the real in-memory engine only as an explicit test adapter, not production fallback. Add real PostgreSQL integration tests for schema validation, borrowed/owned pools, failure rollback, named-store isolation and durable persistence across application contexts. Exercise the packed root without optional database/Zod dependencies and the PostgreSQL subpath separately. Retain TypeScript 6/7, Node 22/24, Bun, the exact tooling hashes and strict lint configuration. No npm release or repository setting changes.
