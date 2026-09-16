import { sqlite as nodeSqlite } from '../../src/integrations/sqlite-node.ts'
import { sqlite as bunSqlite } from '../../src/integrations/sqlite-bun.ts'

nodeSqlite({ path: './jobs.db', events: true })
bunSqlite({ path: './jobs.db', events: false, pollIntervalMs: 100 })
// @ts-expect-error Event activation is an explicit boolean, not a retention configuration.
nodeSqlite({ path: './jobs.db', events: { retention: { count: 10 } } })
// @ts-expect-error SQLite flows still require a separately qualified integration.
bunSqlite({ path: './jobs.db', flows: true })
// @ts-expect-error Event reader options cannot expose an engine token.
nodeSqlite({ path: './jobs.db', eventStore: 'primary' })

nodeSqlite({ path: './jobs.db', schedules: true, events: true })
bunSqlite({ path: './jobs.db', schedules: false })
// @ts-expect-error Schedule configuration is a boolean opt-in, not an engine token.
nodeSqlite({ path: './jobs.db', schedules: {} })
