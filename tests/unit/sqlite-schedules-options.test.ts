import { expect, test } from 'bun:test'
import { MqConnectionException } from '../../src/index.ts'
import { sqliteSchedulesQualified } from '../../src/integrations/sqlite-adapter-version.ts'
import { sqlite } from '../../src/integrations/sqlite-bun.ts'

for (const version of ['0.1.0', '0.1.1', '0.1.2', '0.1.3-beta.1', '0.2.0', 'invalid']) {
  test(`SQLite schedules reject unqualified adapter version ${version}`, () => {
    expect(sqliteSchedulesQualified(version)).toBe(false)
  })
}
for (const version of ['0.1.3', '0.1.4', '0.1.30']) {
  test(`SQLite schedules accept qualified adapter version ${version}`, () => {
    expect(sqliteSchedulesQualified(version)).toBe(true)
  })
}

test('SQLite schedule activation fails closed or stays inert according to the installed adapter', () => {
  const operation = () =>
    sqlite(JSON.parse('{"path":"./missing-directory/jobs.db","schedules":true}'))
  if (sqliteSchedulesQualified()) {
    expect(operation().ownership).toBe('owned')
  } else {
    expect(operation).toThrow(MqConnectionException)
    try {
      operation()
    } catch (cause) {
      expect(cause).toBeInstanceOf(MqConnectionException)
      if (cause instanceof MqConnectionException)
        expect(String(cause.cause)).toContain('SQLite schedules require better-effect-mq-sqlite')
    }
  }
})

test('SQLite schedules=false remains an inert descriptor on every adapter patch', () => {
  expect(sqlite({ path: './missing-directory/jobs.db', schedules: false }).ownership).toBe('owned')
})

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
