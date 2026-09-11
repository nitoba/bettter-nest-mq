import { expect, test } from 'bun:test'
import { Client, Pool } from 'pg'
import {
  adapterJsonTypes,
  flowJsonTypes,
  postgresJsonPool,
  postgresFlowJsonPool
} from '../../src/integrations/postgres-json-pool.ts'
import { postgresFlowNamespace } from '../../src/integrations/postgres-flow-resource.ts'

test('flow JSON views decode exactly once without changing ordinary or native parsers', () => {
  const client = new Client({ types: { getTypeParser: () => (text: string) => `native:${text}` } })
  for (const text of [
    '"true"',
    '"123"',
    '"{\\"a\\":1}"',
    'null',
    'false',
    '12',
    '[1,2]',
    '{"a":1}'
  ]) {
    expect(flowJsonTypes(client).getTypeParser(3802)(text)).toEqual(JSON.parse(text))
    expect(adapterJsonTypes(client).getTypeParser(3802)(text)).toBe(text)
    expect(client.getTypeParser(3802)(text)).toBe(`native:${text}`)
  }
  expect(flowJsonTypes(client).getTypeParser(23)('1')).toBe('native:1')
})
test('separate parser views remain inert and stable over one caller-owned pool', async () => {
  const pool = new Pool({ connectionString: 'postgresql://localhost/unused' })
  expect(postgresFlowJsonPool(pool)).toBe(postgresFlowJsonPool(pool))
  expect(postgresFlowJsonPool(pool)).not.toBe(postgresJsonPool(pool))
  expect(pool.totalCount).toBe(0)
  expect(postgresFlowNamespace('first', 'app')).not.toBe(postgresFlowNamespace('second', 'app'))
  await pool.end()
})
