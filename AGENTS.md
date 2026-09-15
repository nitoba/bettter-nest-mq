# Repository instructions

## Scope and status

Read docs/sqlite.md, docs/kysely-outbox.md, docs/event-waits.md, docs/execution-extensions.md, docs/flows.md, docs/schedules.md, README.md, docs/dependencies.md, docs/postgres-json.md, docs/controls.md, docs/contracts.md, docs/connections.md, docs/execution.md, docs/outbox.md, docs/architecture.md and docs/roadmap.md before changing APIs. Foundations, typed producers/workers, PostgreSQL lifecycle/JSON fidelity, distributed controls, native PostgreSQL transactional outbox, persistent schedules, durable PostgreSQL flows, named retry providers, explicit MQ enhancers, event-assisted waits and managed Kysely outbox queries are implemented. External ORM transaction enrollment, other drivers and further execution integration remain planned. Never simulate missing APIs or silently fall back to memory.

## Toolchain

- Use Bun 1.4.2 for installation/scripts/tests and commit its generated lockfile. CI uses frozen installs.
- TypeScript >=6.0.0 is the public floor; check TS6 and the primary compiler.
- Preserve the exact 20 upstream Oxlint/Oxfmt/plugin hashes, type-aware lint, strictness, tsdown, publint and Lefthook. Do not disable rules or introduce ESLint/Prettier to make a change pass.
- Keep legacy Nest decorators, emitted metadata, exactOptionalPropertyTypes and noUncheckedIndexedAccess.

## Application boundary

- Public APIs use Nest DI, Promises and facade-owned types. No engine imports in public declaration chunks. Type-only public transaction interfaces must remain separate from native resource implementations.
- better-effect, better-result, better-effect-mq and its PostgreSQL/outbox adapters are normal internal dependencies, never consumer peers. Installed test consumers must not declare these packages themselves. Native pg, chosen Zod and Kysely remain optional integrations; root imports cannot require them.
- One private runtime per configured application, sharing Clock, job/flow/schedule/outbox stores, workers and opt-in publishers. No per-request/job/worker runtime or global acquired-resource singleton. Contract-only applications acquire none.
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
- RetryPolicy providers are synchronous/static and named/versioned. Keep typed failure validation, retryable predicates and native attempt budgets; do not serialize callbacks or invoke async policy providers. Producer-only contexts do not require implementations.
- Reserve `__better_nest_mq_retry` for internal references across enqueue/batch/prepare/schedules/flows/outbox. Reject caller overrides and check the persisted reference before business code or enhancers. Incompatible deployments must retain old versioned contracts/providers.
- MQ enhancers are opt-in registered classes, not HTTP metadata. Share one ContextId with the worker per attempt/phase; preserve stage order and codec validation. Continuations are single-use, closed on return, and must drain admitted downstream work before settlement or resource release. Filters cannot bypass cancellation or reference/lease checks.
- Event-assisted waits are implemented; resumable subscriptions remain pending. Process-local middleware is not a durable event log.

## PostgreSQL and outbox

- Preserve the private adapter-query JSON parser boundary and SQL NULL distinction. Never mutate global/native parsers or hide bugs by changing payload envelopes. Run the existing scalar/null, transaction, listener and disconnection regressions.
- Startup validates schema without applying migrations. Borrowed pools remain caller-owned; owned pools close after scoped resources. Do not log raw client objects or credentials.
- Outbox domain SQL and append must use the same actual transaction client. Business queries keep native parser semantics while adapter SQL uses its private view. Do not substitute a second client, pool or domain write outside this boundary.
- The callback handle exposes only query/append and closes when the callback returns. Track/drain already-started operations before release; a caught query/append failure must still prevent commit.
- Never automatically retry the business callback, especially after uncertain commit. SQL is trusted application code and must not issue manual transaction control through the managed handle. The managed Kysely bridge uses this boundary; arbitrary external PoolClient/ORM enrollment is not implemented.
- Prepared requests are revalidated against registered destination contracts. Duplicate validation includes full request, destination, dispatch key and publication budget. Duplicate append does not deduplicate the domain callback. Keep publication retry budget independent of job handler attempts.
- Outbox publisher and Worker roles are independent. Publish only committed rows and preserve stable IDs/routes across replay. Delivery remains at-least-once; publish accepted does not mean executed. Shutdown does not drain the entire durable backlog.
- Failed acquisition/activation must roll back locally: Nest close may rethrow bootstrap failure before destruction hooks. Drain admitted transactions and quiesce publisher/workers before releasing their stores and owned pools.

## Kysely transaction boundary

- Keep Kysely optional behind better-nest-mq/kysely. Root/PostgreSQL imports and declarations must work without Kysely installed.
- The scoped driver uses the existing native outbox client. It must never open another pool, control a separate transaction, retry the business callback or destroy the borrowed resource.
- Guard escaped and derived builders and drain operations actually admitted to the driver. Caught driver/append failures poison commit. Compilation/plugins outside driver admission are not implicitly tracked.
- Nested transactions, streaming and driver destruction reject rather than pretending to work. Initialize the public connection wrapper without SQL so pre-query destruction also reaches the guard.
- Test actual Kysely and installed PostgreSQL consumers: compare physical backend PID and transaction ID, verify invisibility/rollback, native parsers, duplicates and publication after producer shutdown. Do not substitute module mocks or relax tooling.

## Durable flow rules

- Keep FanOut and Collect as separate durable phases. PostgreSQL handoff relinquishes the original parent lease; Collect must execute only after a fresh native claim.
- Suspended-parent inspection belongs to FlowStore/v2. Do not widen the frozen v1 JobStore state union or coerce `waiting-children` into an ordinary job state.
- Preserve stable child keys, bounded manifests/readers, explicit continue/fail policies and the same schema/codec boundaries used by normal jobs. JSON null must remain distinct from SQL NULL.
- Waiting parents must not hold ordinary worker execution capacity. Keep child-report delivery and bounded recovery as independent paths; recovery must not starve later parents or child keys.
- Parent controls and per-key child dispatch combinations that cannot preserve flow ownership/routing must fail explicitly. Do not fabricate handlers, local flow engines or serialized executable functions.
- Qualify changes through real PostgreSQL and installed-package process-death tests, including two replacement workers, typed failures, empty/nested flows, fail-fast and cascade cancellation.

## Verification and delivery

Run `bun run check` before delivery: TS6/7, real Nest/engine tests, formatting, lint, ESM/declarations, publint and actual installed tarballs. Add observed failing regressions before behavior fixes. Keep type-negative tests and public dependency isolation.

Use MQ_TEST_DATABASE_URL only with a dedicated database. PostgreSQL tests create/drop random schemas and terminate tagged clients. Run test:postgres and the JSON boundary scripts, then build/test:package with that environment. Retained CI also runs the transactional outbox fixture under Node and Bun with both TypeScript versions: rollback/invisibility, native parsers, duplicate routing, replay, scalar results, caught failures and shutdown/disconnection safety. Verify persistence after recreated contexts, not merely a returned ID.

Use conventional commits. No npm publication, release, production deployment or repository settings changes without explicit authorization. Retained CI is read-only; remove temporary branch-only generation workflows before merging. Confirm the exact final commit passes and report remaining boundaries accurately.

## Persistent scheduling rules

Schedule decorators, schedule administration and PostgreSQL JobScheduler integration are implemented. Preserve the raw connection namespace, same-pool JSON parser view and single runtime. Validate definitions and static payloads before consumers start; normal replicas are read-only and deployment reconciliation must preserve pauses, cursors and omissions.

Never replace atomic upstream tick/occurrence fencing with local timers. Keep scheduler/worker/outbox roles independent and drain admitted ticks before resources close. The pinned protocol cannot persist dispatch keys: reject derived-key/per-key destinations instead of silently losing keys. Catch-up is capped at 256 per tick; skip drops every observed due slot with no lateness tolerance. Do not advertise different semantics.

Qualify schedules through installed tarballs with independent PostgreSQL scheduler processes under both consumer compilers/runtimes. Keep tests for scalar/null/date payloads, drift, paused definitions, invalid preflight, unsafe timer configuration and shutdown. Do not rerun one-shot source patch scripts from formatter workflows; remove temporary development helpers before integration.

## Event-assisted wait integration

Event-assisted awaitResult/execute is implemented; see docs/event-waits.md and docs/sqlite.md for the API and exact scope. Configure postgres({ events: true }) or sqlite({ events: true }) on the waiting application and select strategy: 'events' with pollFallbackMs. Polling remains the default. The raw event token shares the job namespace; only an in-runtime operation alias is added. No second native connection/runtime, automatic migrations or required-writer activation is introduced.

Runtime reader errors/lost hints use the existing bounded fallback; missing explicit reader configuration still fails. Timeout and abort do not cancel durable work. Keep tests for shutdown, parser fidelity and installed consumers. Resumable subscriptions, checkpoint APIs and retention administration remain pending; this is not a promise of zero polling.

## SQLite JobStore and event-reader boundary

Native SQLite JobStore integration is implemented through separate sqlite/node and sqlite/bun entry points; read docs/sqlite.md (sqlite.md within docs). The raw named token is passed to the upstream layerFor factory. Never replace it with an unscoped make/file wrapper that loses namespace or store disposal. Native databases are acquired lazily and owned handles close only after the existing runtime releases stores. Borrowed handles keep ownership and default pragmas. Schema migration is explicit.

The released SQLite adapter remains an internal dependency. Tests must execute actual files and native host drivers, not memory queue substitutes. Keep Node/root declaration compilation independent from Bun typings and verify installed consumers under both compilers. SQLite flow/schedule/outbox resources remain pending despite extension tables in its complete migration set. Do not advertise multi-host distribution or asynchronous driver execution.

SQLite events:true enables only a native scoped reader on the same acquired database and raw namespace. It does not enable required writer activation, mutate retention, prune history or create a subscriber checkpoint. Native writer bookkeeping can be initialized lazily by upstream job writes; reader-only startup must leave the catalog and activation/history unchanged. Keep SELECT/probe failure, caller isolation, borrowed pragma/ownership, shutdown and installed cross-runtime SIGKILL/recovery regressions.

SQLite schedule integration is gated by the reproduced scalar JSON corruption tracked in better-effect#390. A corrected upstream release must be qualified before enabling the facade. Do not monkey-patch installed dependencies, change payload envelopes or accept unsafe schedules merely because their tables exist. Ordinary crash recovery does not establish every distributed-control or flow failure guarantee.
