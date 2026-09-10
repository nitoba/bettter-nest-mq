# Repository instructions

## Scope

Read README.md, docs/contracts.md, docs/connections.md, docs/architecture.md and docs/roadmap.md before changing public APIs. M0/M1 foundations and M2 private engine/PostgreSQL lifecycle are implemented. Public producer/worker, flow, schedule and outbox APIs are not implemented. Never export fake enqueue, no-op workers or implicit memory fallbacks.

## Toolchain

- Use Bun 1.4.2 for installation/scripts/tests and commit its generated lockfile. CI uses frozen installs.
- TypeScript >=6.0.0 is the public floor; check the TS6 alias and the primary compiler.
- Keep the exact vendored type-aware Oxlint/Oxfmt/plugin, tsdown, publint and Lefthook. Do not introduce ESLint/Prettier or weaken rules to pass.
- Preserve strictness, exactOptionalPropertyTypes, noUncheckedIndexedAccess and Nest legacy decorators/metadata.

## Architecture

- Public APIs use Nest DI, facade-owned types and Promises. No Effect/Result/Layer/Runtime imports in public declarations, including shared declaration chunks.
- One private runtime per configured application context; none for contract-only registrations. No runtime per request/job/provider or global mutable engine registry.
- Connection descriptors/contracts are inert. Never acquire pools in decorators or constructors.
- Keep optional Zod/pg/adapter imports out of the root. Test actual packed consumption with optional peers absent, then optional subpaths present.
- Preserve Standard Schema input/output inference and explicit inverse codecs; never guess how to reverse a transform.
- Queue declarations remain singleton/static; attempt-scoped dependencies belong to the worker bridge.
- Preserve the stable named-store token scheme. PostgreSQL hashes the token into its namespace: renaming connections or changing `nestjs/` changes the durable address.
- Startup validates schema and never applies migrations. Migration helpers are explicit deployment operations.
- Roll back partial acquisition inside the failure path; failed Nest bootstrap can prevent destruction hooks.
- Adapter resources close before owned pools. Borrowed pools remain caller-owned. Owned pg idle-client errors must not crash consumers or log raw credentials/client objects.
- Lifecycle state is not continuous health monitoring. Probes use live store access; do not fake readiness.
- Outbox requires the real domain transaction. The upstream outbox dependency alone does not provide the Nest facade.
- Preserve at-least-once delivery, fencing, cooperative cancellation and storage-backed distributed guarantees.

## Verification

Run `bun run check` for every delivery and `bun run test:postgres` with MQ_TEST_DATABASE_URL against a dedicated database for connection changes. CI creates an isolated PostgreSQL service; tests create/drop random schemas and terminate only tagged test clients. Do not point failure tests at production.

Add failing regressions before behavior fixes. Use actual Nest contexts and real upstream stores; module mocks do not establish transaction/lease correctness. Packed consumers compile with TS6/7 and run under Node/Bun, including actual PostgreSQL startup and idle-client recovery in the database job.

Never infer persistence from a successful method return alone: verify durable records after recreating the application context using the same connection identity.

## Delivery

Use conventional commits. No npm publication, releases or repository setting changes without explicit authorization. Retained CI is read-only; remove temporary branch-specific generation workflows before integration. Document implemented and planned boundaries accurately.
