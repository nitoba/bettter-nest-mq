import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, changes) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of changes) {
    assert.equal(text.split(before).length - 1, 1, `Expected one anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('tests/package/consumer/kysely-outbox.ts', [
  ['  let open = () => {', '  let open: () => void = () => {']
])
patch('README.md', [
  ['explicit MQ enhancers and event-assisted result waits are implemented.', 'explicit MQ enhancers, event-assisted result waits and managed Kysely outbox queries are implemented.'],
  ['ORM transaction bridges and additional integrations remain on the roadmap.', 'External ORM transaction enrollment and additional integrations remain on the roadmap.'],
  ['See [event-assisted result waits]', 'See [Kysely outbox transactions](docs/kysely-outbox.md), [event-assisted result waits]'],
  ['## Event-assisted result waits', `## Kysely outbox transactions

The optional \`better-nest-mq/kysely\` entry point provides \`kyselyOutbox<Database>(outboxes, 'primary').transaction(async ({ db, append }) => ...)\`. Typed Kysely business queries and the outbox append use the same existing native PostgreSQL transaction. There is no extra pool/runtime and no separate commit for the job record.

Use the scoped query builder in repositories that participate in the transaction. Database/append failures prevent commit even when caught, admitted SQL drains before release, and escaped builders cannot query after the callback. Transaction ownership, nested transactions and streaming are not exposed as supported operations. This is not enrollment of an arbitrary pre-existing ORM transaction. Kysely is optional and required only by users choosing this integration; internal engine dependencies remain automatic. See [docs/kysely-outbox.md](docs/kysely-outbox.md).

## Event-assisted result waits`]
])
patch('docs/roadmap.md', [
  ['explicit MQ enhancers and event-assisted result waits** are implemented.', 'explicit MQ enhancers, event-assisted result waits and managed Kysely outbox queries** are implemented.'],
  ['docs/execution-extensions.md and docs/event-waits.md.', 'docs/execution-extensions.md, docs/event-waits.md and docs/kysely-outbox.md.'],
  [`## M6b — Outbox bridges and administration — pending

Add separately verified TypeORM/Prisma/Kysely transaction bridges, other adapters, failed-record reset/retry and richer recovery administration. Preserve real transaction identity, uncertain-commit semantics and borrowed ownership. Application outbox remains distinct from flow coordination.`,
   `## M6b — Managed Kysely outbox transactions — implemented

The optional Kysely integration builds typed business queries on the native outbox's already-acquired PostgreSQL transaction. The public factory supports dynamic appends and predeclared single/batch entries. It preserves native parsers, rollback after caught driver/append failures, callback closure, admitted-query draining and borrowed resource ownership. Kysely never opens a second pool or controls a separate transaction. Installed PostgreSQL tests compare actual backend PIDs/transaction IDs and verify invisibility, rollback and later publication. See kysely-outbox.md.

## M6c — External ORM enrollment and administration — pending

Enrollment of caller-owned Kysely transactions, TypeORM/Prisma bridges, other adapters, failed-record reset/retry and richer recovery administration remain separate work. Preserve verified transaction identity, uncertain-commit semantics and borrowed ownership. The managed Kysely callback does not prove ownership of an arbitrary external transaction. Application outbox remains distinct from flow coordination.`],
  ['M6a native outbox implemented; M6b ORM bridges pending', 'M6a native outbox + M6b managed Kysely implemented; external ORM enrollment pending']
])
patch('docs/dependencies.md', [
  ['ORM transaction bridges remain separate work.', 'Managed Kysely callbacks are available through the optional Kysely subpath; external ORM transaction enrollment remains separate work.'],
  ['Optional integrations require only the selected schema library or native driver:', 'Optional integrations require only the selected schema library, query builder or native driver:'],
  ['## Verification and version responsibility', `## Optional Kysely integration

Import \`kyselyOutbox\` from \`better-nest-mq/kysely\` when choosing typed Kysely queries in a managed PostgreSQL outbox callback. The application supplies Kysely >=0.29.5 <0.30.0 and its PostgreSQL driver; Kysely 0.29.5 is pinned for development/qualification. It is an optional peer, not a normal dependency required by root-only or PostgreSQL-only users. The engine's internal dependency policy is unchanged.

Actual tarball consumers remove Kysely along with pg and Zod before compiling/running root-only usage, then install selected integrations for their dedicated scenarios. See kysely-outbox.md for the managed transaction boundary and limitations.

## Verification and version responsibility`]
])
patch('docs/outbox.md', [
  ['This first slice supports library-managed PostgreSQL transactions and get/list/count diagnostics. It is not an ORM transaction bridge, a cross-database transaction, a workflow engine or an exactly-once external-effects guarantee.', 'The native API supports library-managed PostgreSQL transactions and get/list/count diagnostics. Typed Kysely queries on this same managed transaction are also available through the optional Kysely integration (see kysely-outbox.md). Neither API accepts arbitrary external ORM transactions or promises cross-database atomicity or exactly-once external effects.'],
  ['This slice intentionally does not accept an arbitrary caller `PoolClient`, TypeORM EntityManager, Prisma transaction or Kysely executor. Those bridges need separate resource-identity and rollback tests.', 'The native API intentionally does not accept an arbitrary caller `PoolClient`, TypeORM EntityManager, Prisma transaction or external Kysely executor. The optional managed Kysely callback reuses this API internally; enrollment of external transactions still needs separate resource-identity and rollback tests.'],
  ['The native PostgreSQL transaction boundary is the first outbox implementation. ORM transaction bridges, additional drivers, richer administrative recovery controls, flows and persistent schedules remain separate work.', 'The native PostgreSQL transaction boundary and its managed Kysely query bridge are implemented. External ORM enrollment, additional drivers and richer administrative recovery controls remain separate work. Flows and persistent schedules are also available, as documented in their own guides.'],
  ['This does not add flows or external ORM transactions.', 'This schedule integration is independent of durable flows and does not enroll external ORM transactions.']
])
patch('AGENTS.md', [
  ['Read docs/execution-extensions.md,', 'Read docs/kysely-outbox.md, docs/execution-extensions.md,'],
  ['ORM transaction bridges, other drivers and further execution integration remain planned.', 'Managed Kysely outbox queries are implemented. External ORM transaction enrollment, other drivers and further execution integration remain planned.'],
  ['- Durable-event waits remain pending; process-local middleware is not a durable event log.', '- Event-assisted waits are implemented; resumable subscriptions remain pending. Process-local middleware is not a durable event log.'],
  ['Arbitrary external PoolClient/ORM bridges are not implemented.', 'The optional managed Kysely bridge uses this boundary; arbitrary external PoolClient/ORM enrollment is not implemented.'],
  ['## Durable flow rules', `## Kysely transaction boundary

- Keep Kysely optional behind better-nest-mq/kysely; root/PostgreSQL imports and declarations must work without Kysely installed.
- The scoped driver uses the existing native outbox client. It must never open another pool, issue independent transaction control, retry the business callback or destroy the borrowed native resource.
- Guard escaped/derived builders and drain only operations actually admitted to the driver. Caught driver/append failures poison commit; compilation/plugins outside driver admission are not implicitly tracked.
- Nested transaction/streaming/destruction operations reject rather than pretending to work. Initialize the public connection wrapper without SQL so pre-query destroy also reaches the guard.
- Test with actual Kysely and installed PostgreSQL consumers: compare backend PID and transaction ID, verify invisibility/rollback, custom parsers, duplicate append and post-producer publication. No module mocks or weakened tooling.

## Durable flow rules`]
])
patch('CHANGELOG.md', [
  ['## Unreleased', `## Unreleased

### Managed Kysely outbox transactions

- Add optional better-nest-mq/kysely with typed transaction-scoped queries and dynamic or predeclared outbox entries on the same native PostgreSQL client.
- Reuse native commit/rollback, parsing, source ownership and publication semantics without a second pool/runtime or callback replay.
- Guard escaped builders, reject nested transaction/stream/destruction operations, drain admitted SQL and preserve failure poisoning after caught errors.
- Qualify actual Kysely compilation and installed PostgreSQL consumers, including physical backend/transaction identity, pre-commit invisibility and rollback.
- Add Kysely 0.29.5 as a development dependency and optional application-facing peer; keep internal engine packages and required root dependencies unchanged.
`]
])
for (const [path, text] of staged) writeFileSync(path, text)
