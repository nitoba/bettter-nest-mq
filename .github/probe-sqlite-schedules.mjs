import assert from 'node:assert/strict'
import { Database } from 'bun:sqlite'
import { SqliteMigrator, SqliteJobScheduleStore } from 'better-effect-mq-sqlite'
import { Result } from 'better-result'

const database = new Database(':memory:')
try {
  SqliteMigrator.migrate({ database })
  console.log('SQLite native version', database.prepare('SELECT sqlite_version() AS version').get())
  const store = SqliteJobScheduleStore.make({ database })
  const now = Date.now()
  const record = {
    key: 'string-json', group: 'probe', queue: 'probe',
    job: { queue: 'probe', name: 'echo', version: 1 },
    everyMs: 60000, timeZone: 'UTC', payload: 'null', metadata: {},
    priority: 0, attemptsMax: 1, misfire: { strategy: 'run-once' },
    overlap: 'allow', paused: false, revision: 0,
    nextRunAtMs: now + 60000, createdAtMs: now, updatedAtMs: now
  }
  const inserted = await store.upsertSchedule(record)
  if (Result.isError(inserted)) throw inserted.error
  const selected = await store.getSchedule({ group: 'probe', key: record.key })
  if (Result.isError(selected)) throw selected.error
  const before = database.prepare('SELECT payload FROM better_effect_mq_schedules').get()
  const paused = await store.pauseSchedule({ group: 'probe', key: record.key })
  if (Result.isError(paused)) throw paused.error
  const after = database.prepare('SELECT payload FROM better_effect_mq_schedules').get()
  console.log('RELEASED SCHEDULE JSON', JSON.stringify({ input: record.payload, inserted: inserted.value.record.payload, selected: selected.value.payload, before, after }))
  assert.equal(inserted.value.record.payload, record.payload)
  assert.equal(selected.value.payload, record.payload)
  assert.deepEqual(after, before)
} finally {
  database.close(true)
}
