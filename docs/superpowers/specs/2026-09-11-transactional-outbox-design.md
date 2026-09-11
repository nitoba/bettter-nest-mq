# PostgreSQL transactional outbox — approved roadmap continuation

The user authorized continuing implementation after main 55a22f064a9f9591ec08b3d21d72ba52de6464ae. The previously approved architecture requires explicit transactional outbox, internal engine dependencies and one application runtime. This is a focused first PostgreSQL outbox slice, not ORM integration, flows or scheduling.

## Public surface

`postgres({ ..., outbox: true })` opts a connection into durable outbox storage using its existing native pool. `MqOutboxService` is an injectable root Service exposing get/list/counts and publisher status with facade-owned Promise types. `postgresOutbox(service, 'primary')` from the optional PostgreSQL subpath creates a typed connection-specific client without opening resources. Its transaction callback receives query and append operations, not a pool or a releasable client. It supports transaction(entry, callback), multiple predeclared entries, and transaction(callback) with explicit appends for generated business IDs.

An entry contains a stable outbox id, a PreparedJob and an independent publication attempt budget. Prepared data is revalidated against a registered destination contract. A missing explicit job id receives a deterministic id derived from source, outbox id and target. Replays reuse one prepared request; recomputing time-sensitive prepared data can intentionally conflict. Duplicate detection must include destination and dispatch key even if the upstream digest omits them. A duplicate outbox append is not deduplication of the business callback itself.

## Atomicity and ownership

The domain queries and adapter append use the same actual PostgreSQL transaction client. Domain queries preserve the native parser behavior; the outbox SQL uses the already tested private JSON parser view. No global parser or pool is modified. Successful callback plus all tracked operations is followed by commit; callback failure, caught query/append failure or connection loss prevents commit and triggers rollback. Late use of a completed transaction rejects. Operations already started are drained before releasing the client. A failed/uncertain commit is never automatically rerun: external business callbacks may not be idempotent.

The first slice only manages its own transaction boundary. It does not pretend that an arbitrary caller PoolClient/EntityManager is verified to belong to this resource, and does not yet export ORM appendIn bridges. User SQL must not manually issue transaction-control commands. Borrowed pools are never closed; outbox stores release before owned pools. Failed initialization rolls back acquisitions locally.

## Publisher

Reuse the upstream OutboxPublisher service layer within the existing runtime. It reads only committed rows, uses stable target connection routes, lease/heartbeat recovery, independent publication retries and idempotent enqueue. Enable it only for opted-in outbox resources; `execution.outboxPublisher: false` supports domain/API-only processes. Worker enablement is independent. Startup checks source protocol/storage before activating publisher, and routes use the existing job operation-store view. Shutdown quiesces publishing and releases claimed work without draining the entire backlog.

A crash after enqueue but before acknowledgement remains at-least-once. Test replay by enqueuing the accepted prepared request, claiming without acknowledgement, expiring/recovering the lease, and then starting a publisher. Verify one job ID and no lost work. No exactly-once external-effect or cross-database transaction claim.

## Verification

Start with the observed failing public-export regression. Add portable option/record tests and real Nest/no-source behavior tests. The real PostgreSQL installed-package fixture covers rollback, uncommitted invisibility, commit then publication, queries plus multiple appends, late-handle rejection, caught-failure poisoning, duplicate/conflicting id/target/key, codecs/scalar JSON, native custom parsers, independent publisher and worker processes/contexts, replay after enqueue-before-ack and borrowed/owned shutdown. Keep all existing JSON, distributed controls and package-isolation regressions.

Bun 1.4.2, TypeScript >=6 with both existing compiler checks, exact upstream tooling and normal internal dependencies remain unchanged. No npm publication, automatic schema migration or production deployment. Complete code and tests must pass the retained read-only CI before integration.
