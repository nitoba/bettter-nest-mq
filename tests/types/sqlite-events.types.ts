import { sqlite as nodeSqlite } from '../../src/integrations/sqlite-node.ts'
import { sqlite as bunSqlite } from '../../src/integrations/sqlite-bun.ts'

nodeSqlite({ path: './jobs.db', events: true })
bunSqlite({ path: './jobs.db', events: false, pollIntervalMs: 100 })
// @ts-expect-error Event activation is an explicit boolean, not a retention configuration.
nodeSqlite({ path: './jobs.db', events: { retention: { count: 10 } } })
// @ts-expect-error SQLite schedules remain unqualified against the pinned released adapter.
bunSqlite({ path: './jobs.db', schedules: true })
// @ts-expect-error Event reader options cannot expose an engine token.
nodeSqlite({ path: './jobs.db', eventStore: 'primary' })
