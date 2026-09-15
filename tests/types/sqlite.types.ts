import type { DatabaseSync } from 'node:sqlite'
import type { Database } from 'bun:sqlite'
import { sqlite as nodeSqlite } from '../../src/integrations/sqlite-node.ts'
import { sqlite as bunSqlite } from '../../src/integrations/sqlite-bun.ts'

export function nativeTypes(node: DatabaseSync, bun: Database): void {
  nodeSqlite({ database: node })
  bunSqlite({ database: bun })
  nodeSqlite({ path: './jobs.db' })
  // @ts-expect-error A database path and borrowed handle are mutually exclusive.
  nodeSqlite({ path: './jobs.db', database: node })
  // @ts-expect-error An explicit source is mandatory; there is no memory fallback.
  bunSqlite({})
  // @ts-expect-error Unimplemented resource bundles must not be advertised as enabled.
  nodeSqlite({ path: './jobs.db', flows: true })
}
