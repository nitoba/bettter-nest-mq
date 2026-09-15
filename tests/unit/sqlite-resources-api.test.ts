import { expect, test } from 'bun:test'
import { MqConnectionException } from '../../src/index.ts'
import { sqlite } from '../../src/integrations/sqlite-bun.ts'

for (const events of [true, false]) {
  test(`SQLite events=${events} is an explicit inert resource option`, () => {
    const connection = sqlite({ path: './jobs.db', events })
    expect(connection.adapter).toBe('sqlite')
    expect(connection.ownership).toBe('owned')
  })
}

for (const events of [null, 1, 'true', {}, { retention: { count: 10 } }]) {
  test(`invalid SQLite event configuration rejects: ${JSON.stringify(events)}`, () => {
    const options = JSON.parse(JSON.stringify({ path: './jobs.db', events }))
    expect(() => sqlite(options)).toThrow(MqConnectionException)
  })
}

test('unqualified SQLite flows remain rejected', () => {
  const options = JSON.parse('{"path":"./jobs.db","flows":true}')
  expect(() => sqlite(options)).toThrow(MqConnectionException)
})

test('SQLite event configuration accessors are never invoked', () => {
  let reads = 0
  const options = {
    path: './jobs.db',
    get events() {
      reads += 1
      return true
    }
  }
  expect(() => sqlite(options)).toThrow(MqConnectionException)
  expect(reads).toBe(0)
})
