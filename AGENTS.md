# Repository instructions

## Scope and status

Read README.md, docs/dependencies.md, docs/postgres-json.md, docs/controls.md, docs/contracts.md, docs/connections.md, docs/execution.md, docs/outbox.md, docs/architecture.md and docs/roadmap.md before changing APIs. Foundations, typed producers/workers, PostgreSQL lifecycle/JSON fidelity, distributed controls and native PostgreSQL transactional outbox are implemented. Flows, schedules, ORM transaction bridges, other drivers and further execution integration remain planned. Never simulate missing APIs or silently fall back to memory.

## Toolchain

- Use Bun 1.4.2 for installation/scripts/tests and commit its generated lockfile. CI uses frozen installs.
- TypeScript >=6.0.0 is the public floor; check TS6 and the primary compiler.
- Preserve the exact 20 upstream Oxlint/Oxfmt/plugin hashes, type-aware lint, strictness, tsdown, publint and Lefthook. Do not disable rules or introduce ESLint/Prettier to make a change pass.
- Keep legacy Nest decorators, emitted metadata, exactOptionalPropertyTypes and noUncheckedIndexedAccess.

## Application boundary

- Public APIs use Nest DI, Promises and facade-owned types. No engine imports in public declaration chunks. Type-only public transaction interfaces must remain separate from native resource implementations.
- better-effect, better-result, better-effect-mq and its PostgreSQL/outbox adapters are normal internal dependencies, never consumer peers. Installed test consumers must not declare these packages themselves. Native pg and chosen Zod remain optional integrations; root imports cannot require them.
- One private runtime per configured application, sharing Clock, stores, workers and opt-in outbox publisher. No per-request/job/worker runtime or global acquired-resource singleton. Contract-only applications acquire none.
- Descriptors are inert. Validate contracts and capabilities before activating workers, bind clients only after storage readiness, and detach them on failure/shutdown.
- Keep the raw `nestjs/<name>` persistence token stable. A named connection is part of PostgreSQL storage identity; the operation-store view must not introduce another pool or namespace.

## Execution and controls

- Delegate claims, leases, heartbeat, retry and settlement to the existing engine. Active cancel requests cancellation without stealing ownership. Lease fencing cannot undo external effects.
- Caller wait timeout/abort is not job cancellation; execute publishes and waits rather than directly invoking a handler.
- Preserve input/decoded/JSON boundaries and explicit inverse codecs. Validate results and typed failures before persistence. Defects do not retry unless explicitly enabled.
- Queue contracts are singleton/static. Worker dependencies may be attempt-scoped with fresh Nest contexts, never fabricated HTTP requests. Require JobData/JobContext annotations, preserve inherited processors and reject unsupported HTTP enhancer metadata.
- The pinned supervisor cannot host duplicate queue/name/version identities across different connections inside one Worker. Reject that before acquisition; use separate Worker Services without changing persisted identities or secretly splitting concurrency limits.
- Distributed controls use upstream controlled operations, not local semaphores or fallback plain claims. Policy validation is read-only by default; one coordinated writer reconciles and never disables omissions or claims cross-store atomicity.
- Derive dispatch keys from decoded payloads, reject conflicting overrides and require keys for per-key declarations on every publication/preparation path. Qualify distributed guarantees using independent PostgreSQL worker processes.
- Custom retry providers, MQ enhancer execution and durable-event waits remain pending.

## PostgreSQL and outbox

- Preserve the private adapter-query JSON parser boundary and SQL NULL distinction. Never mutate global/native parsers or hide bugs by changing payload envelopes. Run the existing scalar/null, transaction, listener and disconnection regressions.
- Startup validates schema without applying migrations. Borrowed pools remain caller-owned; owned pools close after scoped resources. Do not log raw client objects or credentials.
- Outbox domain SQL and append must use the same actual transaction client. Business queries keep native parser semantics while adapter SQL uses its private view. Do not substitute a second client, pool or domain write outside this boundary.
- The callback handle exposes only query/append and closes when the callback returns. Track/drain already-started operations before release; a caught query/append failure must still prevent commit.
- Never automatically retry the business callback, especially after uncertain commit. SQL is trusted application code and must not issue manual transaction control through the managed handle. Arbitrary external PoolClient/ORM bridges are not implemented.
- Prepared requests are revalidated against registered destination contracts. Duplicate validation includes full request, destination, dispatch key and publication budget. Duplicate append does not deduplicate the domain callback. Keep publication retry budget independent of job handler attempts.
- Outbox publisher and Worker roles are independent. Publish only committed rows and preserve stable IDs/routes across replay. Delivery remains at-least-once; publish accepted does not mean executed. Shutdown does not drain the entire durable backlog.
- Failed acquisition/activation must roll back locally: Nest close may rethrow bootstrap failure before destruction hooks. Drain admitted transactions and quiesce publisher/workers before releasing their stores and owned pools.

## Verification and delivery

Run `bun run check` before delivery: TS6/7, real Nest/engine tests, formatting, lint, ESM/declarations, publint and actual installed tarballs. Add observed failing regressions before behavior fixes. Keep type-negative tests and public dependency isolation.

Use MQ_TEST_DATABASE_URL only with a dedicated database. PostgreSQL tests create/drop random schemas and terminate tagged clients. Run test:postgres and the JSON boundary scripts, then build/test:package with that environment. Retained CI also runs the transactional outbox fixture under Node and Bun with both TypeScript versions: rollback/invisibility, native parsers, duplicate routing, replay, scalar results, caught failures and shutdown/disconnection safety. Verify persistence after recreated contexts, not merely a returned ID.

Use conventional commits. No npm publication, release, production deployment or repository settings changes without explicit authorization. Retained CI is read-only; remove temporary branch-only generation workflows before merging. Confirm the exact final commit passes and report remaining boundaries accurately.
