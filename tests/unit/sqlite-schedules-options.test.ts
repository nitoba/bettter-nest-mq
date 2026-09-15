import { expect, test } from 'bun:test'
import { MqConnectionException } from '../../src/index.ts'
import { sqlite } from '../../src/integrations/sqlite-bun.ts'

for (const schedules of [true, false]) {
  test(`SQLite schedule option ${schedules} is inert`, () => {
    expect(
      sqlite(JSON.parse(JSON.stringify({ path: './missing-directory/jobs.db', schedules })))
        .ownership
    ).toBe('owned')
  })
}
for (const schedules of [null, 1, 'true', {}]) {
  test(`SQLite schedules reject ${JSON.stringify(schedules)}`, () => {
    expect(() => sqlite(JSON.parse(JSON.stringify({ path: './jobs.db', schedules })))).toThrow(
      MqConnectionException
    )
  })
}
test('SQLite schedule options never evaluate accessors', () => {
  let reads = 0
  const options = {
    path: './jobs.db',
    get schedules() {
      reads += 1
      return true
    }
  }
  expect(() => sqlite(options)).toThrow(MqConnectionException)
  expect(reads).toBe(0)
})
