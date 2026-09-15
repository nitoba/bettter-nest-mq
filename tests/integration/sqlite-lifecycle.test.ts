import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import assert from 'node:assert/strict'
import { Inject, Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { Job, JobData, MqConnectionException, MqModule, Process, Queue, QueueService, Worker } from '../../src/index.ts'
import { sqlite, migrateSqlite } from '../../src/integrations/sqlite-bun.ts'
import { sqliteConnection, type SqliteHost } from '../../src/integrations/sqlite-connection.ts'
import { z } from 'zod'

@Queue({ name: 'drain', connection: 'primary' })
class DrainQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}
@Injectable()
class Gate {
  readonly entered = Promise.withResolvers<void>()
  readonly release = Promise.withResolvers<void>()
}
@Worker({ name: 'drain-worker', pollIntervalMs: 5 })
class DrainWorker {
  constructor(@Inject(Gate) private readonly gate: Gate) {}
  @Process(DrainQueue, 'task')
  async process(@JobData() value: string) {
    this.gate.entered.resolve()
    await this.gate.release.promise
    return value
  }
}

test('owned SQLite handles remain open until the admitted handler is drained', async () => {
  let closed = 0
  const snapshots: unknown[] = []
  const host: SqliteHost<Database> = {
    async open() {
      const database = new Database(':memory:')
      await migrateSqlite({ database })
      return database
    },
    close(database) {
      snapshots.push(...database.prepare('SELECT state,result FROM better_effect_mq_jobs').all())
      closed += 1
      database.close(true)
    }
  }
  const app = await Test.createTestingModule({ imports: [
    MqModule.forRoot({ connections: { primary: sqliteConnection({ path: './unused-lifecycle.db' }, host) }, shutdown: { gracePeriodMs: 2000 } }),
    MqModule.forFeature([DrainQueue])
  ], providers: [Gate, DrainWorker] }).compile()
  const gate = app.get(Gate)
  try {
    await app.init()
    await app.get(DrainQueue).task.enqueue('completed before close')
    await gate.entered.promise
    const stopping = app.close()
    expect(closed).toBe(0)
    gate.release.resolve()
    await stopping
    expect(closed).toBe(1)
    expect(snapshots).toEqual([{ state: 'completed', result: '"completed before close"' }])
  } finally { gate.release.resolve(); await app.close() }
})

test('duplicate lexical file paths are rejected before resource acquisition', async () => {
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ connections: {
    one: sqlite({ path: './duplicate.db', namespace: 'same' }),
    two: sqlite({ path: './nested/../duplicate.db', namespace: 'same' })
  } })] }).compile()
  await assert.rejects(app.init(), MqConnectionException)
  await assert.rejects(app.close())
})

test('borrowed pragma mutation requires explicit configuration and capability failures do not close its database', async () => {
  const database = new Database(':memory:')
  try {
    await migrateSqlite({ database })
    expect(() => sqlite({ database, busyTimeoutMs: 30 })).toThrow(MqConnectionException)
    const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ connections: {
      primary: sqlite({ database, configurePragmas: true, busyTimeoutMs: 31, requireCapabilities: ['durableChangeFeed'] })
    } })] }).compile()
    await assert.rejects(app.init(), MqConnectionException)
    await assert.rejects(app.close())
    expect(database.prepare('PRAGMA foreign_keys').get()).toEqual({ foreign_keys: 1 })
    expect(database.prepare('PRAGMA busy_timeout').get()).toEqual({ timeout: 31 })
    expect(database.prepare('SELECT 1 AS ok').get()).toEqual({ ok: 1 })
  } finally { database.close() }
})
