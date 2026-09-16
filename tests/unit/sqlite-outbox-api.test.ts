import { expect, test } from 'bun:test'
import { MqConnectionException } from '../../src/index.ts'
import { sqlite, sqliteOutbox } from '../../src/integrations/sqlite-bun.ts'

test('SQLite outbox is an explicit inert connection capability', () => {
  expect(sqlite({ path: './missing/jobs.db', outbox: true }).ownership).toBe('owned')
  expect(sqlite({ path: './missing/jobs.db', outbox: false }).ownership).toBe('owned')
  expect(sqliteOutbox).toBeInstanceOf(Function)
})

for (const outbox of [null, 1, 'true', {}]) {
  test(`SQLite outbox rejects ${JSON.stringify(outbox)}`, () => {
    expect(() => sqlite(JSON.parse(JSON.stringify({ path: './jobs.db', outbox })))).toThrow(
      MqConnectionException
    )
  })
}

test('SQLite outbox configuration accessors are rejected without execution', () => {
  let reads = 0
  const options = {
    path: './jobs.db',
    get outbox() {
      reads += 1
      return true
    }
  }
  expect(() => sqlite(options)).toThrow(MqConnectionException)
  expect(reads).toBe(0)
})
