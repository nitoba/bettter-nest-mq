# Contributing

Use Bun 1.4.2 and a supported Node version:

```sh
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

The package now implements typed contracts, private engine/PostgreSQL lifecycle and core public producers/workers. Read docs/execution.md and the roadmap before adding APIs. Distributed QueueControls are implemented and documented in docs/controls.md. Future flow, schedule and outbox APIs must be real integrations, never simulated methods. Keep normal policy validation read-only and qualify shared limits with actual independent PostgreSQL worker processes.

Retain the exact upstream Oxlint/Oxfmt/plugin baseline and generated Bun lockfile. Source and packed declarations must work with TypeScript 6 and the primary TypeScript 7 compiler. Keep strictness and Nest decorator metadata; do not disable a rule to accommodate a fixture or broad internal type.

Tests use actual Nest contexts and the upstream queue supervisor. Add failing regressions before fixes. Preserve coverage for input/decoded codecs, typed errors versus defects, retry delays, caller wait cancellation, idempotency, inherited processors, request-scoped dependencies, local concurrency and graceful shutdown.

Packed-consumer tests install an actual tarball outside the repository, first without optional Zod/pg/adapter peers, then with optional subpaths. They compile with both TypeScript versions and execute under Node and Bun. Build before individually invoking publint/test:package; the full check builds automatically.

For PostgreSQL changes, use a dedicated database:

```sh
MQ_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/test_db' bun run test:postgres
bun run build
MQ_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/test_db' bun run test:package
```

Tests create/drop random schemas and deliberately disconnect tagged clients. Never point them at production. The package execution fixture verifies producer-only shutdown, worker execution in a new application context, retry history and durable result reads afterward. CI supplies PostgreSQL 16 and runs the same scenarios.

Preserve borrowed/owned resource boundaries, stable connection identities, explicit migrations and one runtime shared by stores and workers. Application schemas and Services use the Nest facade; engine types must not escape in declaration chunks. Keep cancellation cooperative and settlement fenced by the owning lease.

No automatic npm publishing is configured. Releases require explicit authorization and successful complete verification.
