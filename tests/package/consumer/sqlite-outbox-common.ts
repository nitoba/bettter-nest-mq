import 'reflect-metadata'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { setTimeout as sleep } from 'node:timers/promises'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqModule,
  MqOutboxService,
  Process,
  Queue,
  QueueService,
  Worker,
  type MqConnection
} from 'better-nest-mq'
import type { SqliteOutboxClient } from 'better-nest-mq/sqlite/node'

interface NativeDatabase {
  exec(sql: string): void
  prepare(sql: string): {
    all(...parameters: readonly (string | number)[]): readonly Record<string, unknown>[]
    get(...parameters: readonly (string | number)[]): Record<string, unknown> | undefined | null
  }
  close(): void
}

interface Api {
  sqlite(options: {
    path: string
    namespace: string
    outbox?: boolean
    pollIntervalMs?: number
  }): MqConnection
  migrateSqlite(options: { path: string }): Promise<unknown>
  sqliteOutbox(service: MqOutboxService, source?: string): SqliteOutboxClient
}

@Injectable()
@Queue({ name: 'sqlite-outbox-process', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.string(), result: z.string() })
}

@Injectable()
@Worker({ name: 'sqlite-outbox-worker', concurrency: 3, pollIntervalMs: 10 })
class Processor {
  @Process(Jobs, 'echo')
  echo(@JobData() value: string) {
    return value
  }
}

const jobIds = ['outbox-job-one', 'outbox-job-two', 'outbox-job-dynamic'] as const

async function until(check: () => Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 15_000
  while (!(await check())) {
    assert.ok(Date.now() < deadline, `Timed out: ${label}`)
    await sleep(10)
  }
}

async function application(api: Api, path: string, role: 'producer' | 'publisher' | 'worker' | 'reader') {
  const usesOutbox = role !== 'worker'
  @Module({
    imports: [
      MqModule.forRoot({
        connections: {
          primary: api.sqlite({
            path,
            namespace: 'sqlite-outbox',
            outbox: usesOutbox,
            pollIntervalMs: 10
          })
        },
        execution: {
          workers: role === 'worker',
          outboxPublisher: role === 'publisher'
        },
        outbox: {
          concurrency: 2,
          pollIntervalMs: 10,
          leaseDurationMs: 500,
          heartbeatIntervalMs: 50,
          retryBaseDelayMs: 10,
          retryMaxDelayMs: 50
        }
      }),
      MqModule.forFeature([Jobs])
    ],
    providers: [Processor]
  })
  class Application {}
  return NestFactory.createApplicationContext(Application, { logger: false, abortOnError: false })
}

function child(runtime: 'node' | 'bun', script: string, mode: string, path: string) {
  const process = spawn(
    runtime,
    [...(runtime === 'node' ? ['--experimental-sqlite'] : []), script, mode, path],
    { stdio: ['ignore', 'ignore', 'pipe'] }
  )
  let errors = ''
  process.stderr.on('data', (chunk: Buffer) => {
    errors = `${errors}${chunk.toString()}`.slice(-10_000)
  })
  const watchdog = setTimeout(() => process.kill('SIGKILL'), 20_000)
  const exited = new Promise<number | null>((resolve) => {
    process.once('error', (cause) => {
      errors += String(cause)
      clearTimeout(watchdog)
      resolve(null)
    })
    process.once('exit', (code) => {
      clearTimeout(watchdog)
      resolve(code)
    })
  })
  return {
    async done() {
      assert.equal(await exited, 0, errors)
    },
    async dispose() {
      if (process.exitCode === null && process.signalCode === null) process.kill('SIGKILL')
      await exited
    }
  }
}

export async function verifySqliteOutbox(
  api: Api,
  runtime: 'node' | 'bun',
  open: (path: string) => NativeDatabase
): Promise<void> {
  const mode = process.argv[2]
  const path = process.argv[3]
  if (mode === 'outbox-publisher') {
    assert.ok(path)
    const app = await application(api, path, 'publisher')
    try {
      const service = app.get(MqOutboxService)
      await until(async () => (await service.counts('primary')).published === 3, 'publisher completion')
    } finally {
      await app.close()
    }
    return
  }
  if (mode === 'outbox-worker') {
    assert.ok(path)
    const app = await application(api, path, 'worker')
    try {
      const jobs = app.get(Jobs).echo
      await Promise.all(
        jobIds.map((id) => jobs.awaitResult(id, { timeoutMs: 10_000, pollIntervalMs: 10 }))
      )
    } finally {
      await app.close()
    }
    return
  }

  const entry = process.argv[1]
  assert.ok(entry)
  const target = mode === 'cross' ? (runtime === 'node' ? 'bun' : 'node') : runtime
  const script = join(dirname(entry), `sqlite-${target}.js`)
  const directory = await mkdtemp(join(tmpdir(), 'mq-sqlite-outbox-'))
  const databasePath = join(directory, 'jobs.db')
  try {
    await api.migrateSqlite({ path: databasePath })
    const setup = open(databasePath)
    try {
      setup.exec(
        "CREATE TABLE business(id TEXT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO business VALUES ('seed','existing')"
      )
    } finally {
      setup.close()
    }

    const producer = await application(api, databasePath, 'producer')
    try {
      const jobs = producer.get(Jobs).echo
      const outboxes = producer.get(MqOutboxService)
      const transaction = api.sqliteOutbox(outboxes, 'primary')
      const rollback = await jobs.prepare('rollback', { jobId: 'rollback-job' })
      await assert.rejects(
        transaction.transaction({ id: 'rollback-record', job: rollback }, (tx) => {
          tx.run("INSERT INTO business VALUES ('temporary','no')")
          try {
            tx.run("INSERT INTO business VALUES ('seed','duplicate')")
          } catch {
            // The managed transaction must retain the native failure.
          }
          return 'ignored'
        })
      )
      const first = await jobs.prepare('first', { jobId: jobIds[0] })
      const second = await jobs.prepare('second', { jobId: jobIds[1] })
      const dynamic = await jobs.prepare('dynamic', { jobId: jobIds[2] })
      const returned = await transaction.transaction(
        [
          { id: 'record-one', job: first },
          { id: 'record-two', job: second }
        ],
        async (tx) => {
          tx.run("INSERT INTO business VALUES ('committed','yes')")
          const row = tx.get<{ value: string }>('SELECT value FROM business WHERE id=?', [
            'committed'
          ])
          assert.deepEqual(row, { value: 'yes' })
          void tx.append({ id: 'record-dynamic', job: dynamic })
          return 7
        }
      )
      assert.equal(returned, 7)
      assert.deepEqual(await outboxes.counts('primary'), {
        pending: 3,
        active: 0,
        published: 0,
        failed: 0,
        total: 3
      })
      for (const id of jobIds) assert.equal(await jobs.poll(id), undefined)
    } finally {
      await producer.close()
    }

    const committed = open(databasePath)
    try {
      assert.deepEqual(committed.prepare("SELECT value FROM business WHERE id='committed'").get(), {
        value: 'yes'
      })
      assert.equal(committed.prepare("SELECT id FROM business WHERE id='temporary'").get(), undefined)
      assert.deepEqual(
        committed.prepare('SELECT state FROM better_effect_mq_outbox ORDER BY id').all(),
        [{ state: 'pending' }, { state: 'pending' }, { state: 'pending' }]
      )
      assert.equal(committed.prepare('SELECT count(*) AS total FROM better_effect_mq_jobs').get()?.total, 0)
    } finally {
      committed.close()
    }

    const publisher = child(target, script, 'outbox-publisher', databasePath)
    try {
      await publisher.done()
    } finally {
      await publisher.dispose()
    }
    const published = open(databasePath)
    try {
      assert.deepEqual(
        published.prepare('SELECT state FROM better_effect_mq_outbox ORDER BY id').all(),
        [{ state: 'published' }, { state: 'published' }, { state: 'published' }]
      )
      assert.equal(published.prepare('SELECT count(*) AS total FROM better_effect_mq_jobs').get()?.total, 3)
    } finally {
      published.close()
    }

    const worker = child(target, script, 'outbox-worker', databasePath)
    try {
      await worker.done()
    } finally {
      await worker.dispose()
    }
    const reader = await application(api, databasePath, 'reader')
    try {
      const jobs = reader.get(Jobs).echo
      assert.deepEqual(
        await Promise.all(jobIds.map((id) => jobs.awaitResult(id, { timeoutMs: 2000, pollIntervalMs: 10 }))),
        ['first', 'second', 'dynamic']
      )
      assert.deepEqual(await reader.get(MqOutboxService).counts('primary'), {
        pending: 0,
        active: 0,
        published: 3,
        failed: 0,
        total: 3
      })
    } finally {
      await reader.close()
    }
    console.log(
      `PASS ${runtime}->${target} SQLite outbox: atomic domain commit, rollback poisoning, independent publisher and worker processes`
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}
