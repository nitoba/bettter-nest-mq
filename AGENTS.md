# Repository instructions

## Scope and status

Read docs/controls.md, README.md and docs/contracts.md, docs/connections.md, docs/execution.md, docs/architecture.md and docs/roadmap.md before changing APIs. M0/M1 foundations, M2 private engine/PostgreSQL lifecycle and M3 core producer/worker execution plus M3.1a distributed controls are implemented. Flows, schedules, transactional outbox, other driver wrappers and execution extensions remain planned. Never simulate planned APIs or silently fall back to memory.

## Toolchain

- Use Bun 1.4.2 for installation/scripts/tests and commit its generated lockfile. CI uses frozen installs.
- TypeScript >=6.0.0 is the public floor; check TS6 and the primary compiler.
- Preserve exact upstream Oxlint/Oxfmt/custom plugin hashes, type-aware lint, strict compiler options, tsdown, publint and Lefthook. Do not add ESLint/Prettier or disable rules to pass a change.
- Keep legacy Nest decorators, emitted metadata, exactOptionalPropertyTypes and noUncheckedIndexedAccess.

## Architecture

- Public API: Nest DI, Promises and facade-owned types. No Effect/Result/Layer/Runtime imports in any public declaration chunk.
- One private runtime per configured application, sharing Clock/stores/lazy Workers. No runtime per request/job/provider and no global runtime singleton. Contract-only apps allocate none.
- Descriptors/decorators are inert. Bind producers only after store readiness and before worker activation; detach on shutdown/failure. An unbound or closed descriptor rejects explicitly.
- Delegate leases, heartbeats, claims, retries and settlement to the existing engine. Do not implement another polling/supervisor protocol.
- Active cancel must request cancellation through the store and let the owner settle with its lease. Never steal leases or mark an active job terminal directly.
- Caller wait timeout/abort is not job cancellation. execute is enqueue-and-wait, not a local method call.
- Preserve input/decoded/JSON distinctions and explicit inverse codecs. Validate results and typed failures before persistence. Do not guess transform inverses.
- Unexpected exceptions remain defects; retryDefects defaults to false at this facade, even if upstream defaults differ. Typed failures require the declared failure schema and retryability predicate.
- Queue contracts remain singleton/static. Worker class providers may use request/transient dependencies via a fresh ContextId per attempt; never fabricate an HTTP Request.
- Process arguments require JobData/JobContext. Preserve base processors when extending classes and require annotations on overridden implementations. Reject unsupported HTTP enhancer metadata instead of silently bypassing it.
- Local concurrency is not distributed concurrency. QueueControls uses the upstream controlled-store claim/settlement/release/recovery protocol; never substitute process-local limits or plain claims for an enabled policy.
- Policy validation is read-only by default. Reconcile requires one coordinated deployment authority; it never disables omitted queues, silently overrides a foreign group or claims atomic cross-store rollback. Validate all adapter capabilities before policy writes and apply policy before workers start.
- Keep the raw nestjs/<name> persistence token stable; the operation-view token introduces no new database namespace or pool. Preserve typed decoded dispatch keys, reject conflicting overrides and require keys for per-key declarations at all publication boundaries.
- Custom retry providers and event-based waits remain pending. Test distributed guarantees with independent processes against PostgreSQL, not only the explicitly test-only shared memory reference fixture.
- Keep Zod/pg and adapter runtime imports out of the root. better-effect, better-result, better-effect-mq and the internal PostgreSQL/outbox adapters are normal dependencies, never consumer peer obligations. Packed consumers must not explicitly declare those internal packages. Verify root usage without pg/Zod, then their selected integration subpaths.
- Preserve stable named-store tokens: `nestjs/<name>` participates in PostgreSQL's durable namespace. A connection rename is not a harmless refactor.
- Startup validates schema, never applies migrations. Borrowed resources remain caller-owned. Roll back acquisition/activation locally after failed bootstrap; Nest close may not reach destruction hooks.
- Drain/quiesce workers before stores, then close owned pools. Owned pg error handlers must not log raw clients/credentials.
- Prepared requests are not transactions. Outbox requires the real domain transaction and at-least-once/idempotency semantics.

## Verification

Run `bun run check` before delivery. It includes TypeScript 6/7, actual Nest/engine tests, formatting/lint, build, declarations, publint and tarball consumers. Add failing regressions before behavior fixes and keep public type-negative tests.

Run PostgreSQL tests with MQ_TEST_DATABASE_URL against a dedicated database: `bun run test:postgres`, then build/test:package. The CI PostgreSQL job exercises actual packed producers/workers under Node/Bun, producer exit/restart, retries, codec results, ownership and client disconnections. Tests create/drop random schemas and terminate tagged clients; never target production.

Do not infer durability from a returned ID alone: verify the record after recreating application contexts with identical connection identity. Do not replace real DB/lease failure tests with module mocks. Preserve existing regression coverage for scope, inheritance, idempotency, active cancellation and shutdown draining.

## Delivery

Use conventional commits. No npm publication, release or repository setting change without explicit authorization. Retained CI is read-only; remove temporary branch-only formatter/generation workflows before merging. Report actual verification results and remaining scope without implying full engine feature parity.
