import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

function replace(path, before, after) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(after)) return
  assert.equal(text.split(before).length - 1, 1, `Expected one documentation anchor in ${path}: ${before}`)
  writeFileSync(path, text.replace(before, after))
}

replace('README.md', 'Status: M3 core execution and M3.1a distributed controls are implemented.', 'Status: Core execution, distributed controls and the native PostgreSQL transactional outbox are implemented.')
replace('README.md', 'Flows, schedules, transactional outbox and additional integrations remain on the roadmap.', 'Flows, schedules, ORM transaction bridges and additional integrations remain on the roadmap.')
replace('README.md', 'See [PostgreSQL JSON fidelity]', 'See [transactional outbox](docs/outbox.md), [PostgreSQL JSON fidelity]')
replace('README.md', 'and the internal outbox dependency does not enable the pending Nest transactional-outbox API.', 'and outbox storage is enabled explicitly with postgres({ outbox: true }), not merely by installing its internal dependency.')
replace('README.md', 'flows, schedules and transactional outbox are still pending.', 'flows, schedules and ORM outbox bridges are still pending. Native PostgreSQL outbox transactions and a managed publisher are available.')
replace('README.md', '## PostgreSQL JSON correction', `## Transactional outbox

Enable \`outbox: true\` on a PostgreSQL connection, inject \`MqOutboxService\`, and obtain a typed client with \`postgresOutbox(service, 'primary')\` from better-nest-mq/postgres. Domain SQL and outbox appends share the same real transaction client:

\`\`\`ts
const prepared = await reports.summarize.prepare({ values: [10, 20, 30] })
await postgresOutbox(outboxes, 'primary').transaction(
  { id: operationId, job: prepared },
  async (tx) => {
    await tx.query('INSERT INTO report_requests (id) VALUES ($1)', [operationId])
  }
)
\`\`\`

The report_requests table and operationId belong to the application. The publisher sees committed rows only; callback or append failures roll back both writes. Multiple records and dynamic \`tx.append\` calls are supported. The transaction handle exposes only query/append and closes with its callback. Native application parsers remain unchanged.

Publisher and worker roles are independent: a domain-only process sets \`execution: { workers: false, outboxPublisher: false }\`, while a publisher process enables outboxPublisher for the same durable source configuration. Publication retries are separate from job attempts, with stable request identities and at-least-once recovery. No second runtime/pool or additional consumer-installed engine dependency is required. See [docs/outbox.md](docs/outbox.md) for the complete Service example, explicit migration prerequisite, duplicate semantics and transaction restrictions.

## PostgreSQL JSON correction`)

replace('docs/dependencies.md', 'The outbox package is currently an internal dependency required by the upstream PostgreSQL integration. It does not mean the Nest transactional-outbox API has already been implemented. Current features and remaining work are documented in roadmap.md.', 'The native PostgreSQL outbox now reuses this internal package through MqOutboxService and the postgresOutbox factory. Consumers still install no additional better-effect packages. Outbox resources are explicitly enabled on their connection; ORM transaction bridges remain separate work. See outbox.md and roadmap.md.')
replace('docs/dependencies.md', 'even during PostgreSQL worker and distributed-control tests.', 'even during PostgreSQL worker, distributed-control and transactional-outbox tests.')
replace('docs/architecture.md', 'flows, schedules and transactional outbox are still planned.', 'flows, schedules and ORM transaction bridges are still planned. Native PostgreSQL outbox transactions and a managed publisher are implemented.')
replace('docs/architecture.md', 'Read contracts.md, connections.md and execution.md', 'Read outbox.md, contracts.md, connections.md and execution.md')
replace('docs/architecture.md', 'The current internal outbox dependency does not implement a Nest outbox facade.', 'The native PostgreSQL facade now implements managed transactions using one actual PoolClient, separate native/adapter JSON views, tracked-operation draining and poison-on-failure. Its opt-in source stores and upstream publisher share the existing runtime and pool. Publisher enablement is independent of worker enablement. Duplicate validation includes destination and dispatch key; repeated callbacks remain an application idempotency concern. No arbitrary external transaction/ORM bridge or automatic callback replay is promised.')
replace('docs/architecture.md', 'Factory/value workers are not currently supported.', 'Factory/value workers are not currently supported. The pinned supervisor also requires queue/name/version uniqueness within one Worker even across different connections; use separate Worker Services for those identities. This limitation is validated before acquisition rather than leaking an engine startup failure.')
replace('docs/architecture.md', 'further distributed/crash/flow/outbox coverage', 'further distributed/crash/flow/ORM transaction coverage')
replace('docs/roadmap.md', 'and **M3.1a distributed queue controls** are implemented.', ', **M3.1a distributed queue controls** and the **M6a native PostgreSQL outbox** are implemented.')
replace('docs/roadmap.md', 'and docs/controls.md.', ', docs/controls.md and docs/outbox.md.')
replace('docs/roadmap.md', 'this correction does not implement the pending flow/schedule/outbox features below.', 'the subsequent native outbox integration reuses this corrected JSON boundary; flows and schedules remain pending.')
replace('docs/roadmap.md', 'the presence of the internal upstream outbox dependency does not provide a Nest outbox facade.', 'native PostgreSQL outbox resources now share the same connection pool; corresponding resources for other adapters remain pending.')
replace('docs/roadmap.md', '## M6 — Transactional outbox — pending\n\nBuild explicit transaction/append/publish APIs around prepared requests. Domain writes and outbox appends must share the actual transaction resource. Implement/test adapter-specific contexts and ORM bridges separately. Preserve rollback, publication only after commit, replay after enqueue-before-ack, conflicting IDs, independent publication/job retries and ownership. Application outbox stays separate from flow coordination.', `## M6a — Native PostgreSQL transactional outbox — implemented

Opt-in outbox source stores share existing PostgreSQL pools and the application runtime. MqOutboxService exposes safe read models; postgresOutbox provides typed managed transactions with domain query/append on one actual client. Callbacks can append several prepared jobs, including data derived from generated domain IDs. Already-started work is drained, caught failures prevent commit, and completed handles reject further use. Native parser behavior is preserved.

The existing outbox publisher forwards committed rows using stable routes and independent publication retries. Worker and publisher enablement are separate. Full duplicate checks include destination and dispatch key, and abandoned enqueue-before-ack records are replayed idempotently. Callback replay, arbitrary external transaction handles, exactly-once effects and cross-database atomicity are not promised. See outbox.md.

## M6b — Outbox bridges and administration — pending

Add separately verified TypeORM/Prisma/Kysely or other native transaction bridges, additional adapter support, failed-record retry/reset APIs and richer operational recovery controls. Preserve real transaction identity, borrowed ownership and uncertain-commit semantics. Application outbox remains distinct from internal flow coordination.`)
replace('docs/roadmap.md', 'M6 outbox after transaction bridges exist', 'M6a native outbox implemented; M6b ORM bridges remain')

replace('CHANGELOG.md', '## Unreleased', `## Unreleased

### Native PostgreSQL transactional outbox

- Add opt-in outbox stores sharing existing native pools and the private application runtime.
- Add MqOutboxService diagnostics and a typed postgresOutbox transaction client with query/append, multi-record and generated-ID workflows.
- Commit domain SQL and outbox rows atomically, poison commit after caught operation failures, drain admitted work and reject escaped transaction handles.
- Preserve native custom parsers while using the existing corrected JSON view for adapter SQL.
- Add managed publisher roles, stable request IDs, independent publication retries and full destination/dispatch-key duplicate checks.
- Qualify uncommitted invisibility, rollback, scalar/null payloads, competing publishers and replay after enqueue-before-ack using installed Node/Bun PostgreSQL consumers.
- Separate public transaction types from engine/resource implementations; internal dependencies and tooling remain unchanged.
- Validate unsupported identical cross-connection handler identities before worker resource acquisition.
`)
