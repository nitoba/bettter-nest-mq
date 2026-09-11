import { expect, test } from 'bun:test'
import { Client, Pool, types } from 'pg'
import { adapterJsonTypes, postgresJsonPool } from '../../src/integrations/postgres-json-pool.ts'

const jsonTexts = ['"retry me"', '""', '"null"', '"true"', '"123"', '"{\\"nested\\":1}"', 'null', 'false', '0', '[]', '{"value":null}']

test.each(jsonTexts)('MQ JSON and JSONB parsers preserve exactly one encoded layer: %p', (text) => {
  const client = new Client()
  const parsers = adapterJsonTypes(client)
  expect(parsers.getTypeParser(114, 'text')(text)).toBe(text)
  expect(parsers.getTypeParser(3802)(text)).toBe(text)
  expect(client.getTypeParser(3802)(text)).toEqual(JSON.parse(text))
})

test('native and global JSON parsers are not replaced when constructing an adapter view', () => {
  const client = new Client()
  const original = client.getTypeParser(3802)
  const global = types.getTypeParser(3802)
  const parsers = adapterJsonTypes(client)
  expect(parsers.getTypeParser(3802)('null')).toBe('null')
  expect(client.getTypeParser(3802)).toBe(original)
  expect(types.getTypeParser(3802)).toBe(global)
  expect(original('null')).toBeNull()
})

test('non-JSON parsing uses the native client configuration, including per-client overrides', () => {
  const client = new Client()
  client.setTypeParser(23, (text) => Number(text) + 100)
  const parsers = adapterJsonTypes(client)
  expect(parsers.getTypeParser(23, 'text')('12')).toBe(112)
  expect(parsers.getTypeParser(25, 'text')('plain text')).toBe('plain text')
  expect(parsers.getTypeParser(23, 'binary')).toBe(client.getTypeParser(23, 'binary'))
})

test('application-specific JSON parsers remain active outside adapter queries', () => {
  const client = new Client()
  client.setTypeParser(3802, (text) => ({ applicationValue: JSON.parse(text) }))
  expect(adapterJsonTypes(client).getTypeParser(3802)('"text"')).toBe('"text"')
  expect(client.getTypeParser(3802)('"text"')).toEqual({ applicationValue: 'text' })
})

test('views are inert, non-owning, and stable per pool for shared listener reservations', async () => {
  const pool = new Pool({ connectionString: 'postgresql://unused:unused@localhost:1/unused', max: 2 })
  const connect = pool.connect
  const query = pool.query
  const end = pool.end
  try {
    const first = postgresJsonPool(pool)
    const second = postgresJsonPool(pool)
    expect(first).toBe(second)
    expect(first.options).toBe(pool.options)
    expect('end' in first).toBe(false)
    expect(pool.totalCount).toBe(0)
    expect(pool.connect).toBe(connect)
    expect(pool.query).toBe(query)
    expect(pool.end).toBe(end)
    expect(Object.isFrozen(pool)).toBe(false)
  } finally { await pool.end() }
})
