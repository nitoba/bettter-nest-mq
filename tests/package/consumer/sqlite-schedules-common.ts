import 'reflect-metadata'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { tmpdir } from 'node:os'
import { createInterface } from 'node:readline'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqModule,
  MqSchedulesService,
  Process,
  Queue,
  QueueService,
  Schedule,
  Worker,
  type MqConnection,
  type PayloadOf
} from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

interface NativeDatabase {
  prepare(sql: string): {
    all(...parameters: (string | number)[]): unknown[]
    run(...parameters: (string | number)[]): void
  }
  close(): void
}
interface Api {
  sqlite(options: {
    path: string
    namespace: string
    schedules: boolean
    events: boolean
  }): MqConnection
  migrateSqlite(options: { path: string }): Promise<{
    readonly component: string
    readonly version: number
    readonly applied: readonly number[]
  }>
}
const values = [
  'null',
  'true',
  '123',
  '{}',
  '[]',
  '"quoted"',
  '',
  'ação',
  null,
  false,
  7,
  ['null'],
  { value: [null, 'true'] }
]
const timestamp = '2026-09-15T12:00:00.000Z'
const date = zodCodec(
  z.codec(z.iso.datetime(), z.date(), {
    decode: (value) => new Date(value),
    encode: (value) => value.toISOString()
  })
)
@Queue({ name: 'sqlite-scheduled', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  @Schedule({ key: 'declared', everyMs: 60_000, payload: 'null' })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
  @Job({ name: 'date', version: 1 })
  readonly date = this.job({ payload: date, result: date })
}
@Worker({ name: 'sqlite-scheduled', concurrency: 4, pollIntervalMs: 5 })
class Processor {
  @Process(Jobs, 'echo')
  echo(@JobData() value: PayloadOf<Jobs['echo']>) {
    return value
  }
  @Process(Jobs, 'date')
  date(@JobData() value: Date) {
    assert.ok(value instanceof Date)
    return value
  }
}
async function application(
  api: Api,
  path: string,
  role: 'deploy' | 'scheduler' | 'worker' | 'reader'
) {
  @Module({
    imports: [
      MqModule.forRoot({
        connections: {
          primary: api.sqlite({ path, namespace: 'schedules', schedules: true, events: true })
        },
        execution: { workers: role === 'worker', scheduler: role === 'scheduler' },
        schedules: {
          mode: role === 'deploy' ? 'reconcile' : 'validate',
          sweepIntervalMs: 60_000,
          batchSize: 32
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
    {
      stdio: ['pipe', 'pipe', 'pipe']
    }
  )
  let errors = ''
  process.stderr.on('data', (chunk: Buffer) => {
    errors = (errors + chunk.toString()).slice(-8000)
  })
  const messages: string[] = []
  const listeners: Array<{ name: string; resolve(): void; reject(cause: Error): void }> = []
  let ended = false
  const waitFor = (name: string) =>
    new Promise<void>((resolve, reject) => {
      const index = messages.indexOf(name)
      if (index !== -1) {
        messages.splice(index, 1)
        resolve()
        return
      }
      if (ended) {
        reject(new Error(`Child already exited: ${errors}`))
        return
      }
      listeners.push({ name, resolve, reject })
    })
  createInterface({ input: process.stdout }).on('line', (line) => {
    const index = listeners.findIndex((listener) => listener.name === line)
    if (index !== -1) listeners.splice(index, 1)[0]?.resolve()
    else messages.push(line)
  })
  process.stdin.on('error', (cause: Error) => {
    errors = (errors + String(cause)).slice(-8000)
  })
  const watchdog = setTimeout(() => process.kill('SIGKILL'), 30_000)
  const exited = new Promise<number | null>((resolve) => {
    process.once('error', (cause) => {
      errors += String(cause)
      ended = true
      clearTimeout(watchdog)
      for (const listener of listeners.splice(0)) listener.reject(cause)
      resolve(null)
    })
    process.once('exit', (code) => {
      ended = true
      clearTimeout(watchdog)
      for (const listener of listeners.splice(0))
        listener.reject(new Error(`Child exit ${code}: ${errors}`))
      resolve(code)
    })
  })
  return {
    ready: () => waitFor('READY'),
    async sweep() {
      const done = waitFor('DONE')
      process.stdin.write('sweep\n')
      await done
    },
    async stop() {
      if (!ended) process.stdin.end('stop\n')
      assert.equal(await exited, 0, errors)
    },
    async dispose() {
      if (!ended) process.kill('SIGKILL')
      await exited
    }
  }
}

export async function verifySqliteSchedules(
  api: Api,
  runtime: 'node' | 'bun',
  open: (path: string) => NativeDatabase
) {
  const mode = process.argv[2]
  if (mode === 'schedules-child' || mode === 'schedules-worker') {
    const path = process.argv[3]
    assert.ok(path)
    const app = await application(api, path, mode === 'schedules-child' ? 'scheduler' : 'worker')
    try {
      const schedules = app.get(MqSchedulesService)
      process.stdout.write('READY\n')
      if (mode === 'schedules-child') {
        for await (const line of createInterface({ input: process.stdin })) {
          if (line === 'stop') break
          assert.equal(line, 'sweep')
          await schedules.sweep()
          process.stdout.write('DONE\n')
        }
      } else {
        const jobs = app.get(Jobs)
        for (const [index, value] of values.entries()) {
          const record = await schedules.get(Jobs, 'echo', `value-${index}`)
          assert.ok(record?.lastJobId)
          assert.deepEqual(
            await jobs.echo.awaitResult(record.lastJobId, { timeoutMs: 5000, pollIntervalMs: 5 }),
            value
          )
        }
        const record = await schedules.get(Jobs, 'date', 'date')
        assert.ok(record?.lastJobId)
        assert.deepEqual(
          await jobs.date.awaitResult(record.lastJobId, { timeoutMs: 5000, pollIntervalMs: 5 }),
          new Date(timestamp)
        )
      }
    } finally {
      await app.close()
    }
    return
  }
  const entry = process.argv[1]
  assert.ok(entry)
  const target =
    mode === 'cross' || mode === 'schedules-cross' ? (runtime === 'node' ? 'bun' : 'node') : runtime
  const script = join(dirname(entry), `sqlite-${target}.js`)
  const directory = await mkdtemp(join(tmpdir(), 'mq-sqlite-schedules-'))
  const path = join(directory, 'jobs.db')
  try {
    await api.migrateSqlite({ path })
    const deploy = await application(api, path, 'deploy')
    let pausedRevision = 0
    try {
      const schedules = deploy.get(MqSchedulesService)
      await schedules.pause(Jobs, 'echo', 'declared')
      pausedRevision = (await schedules.get(Jobs, 'echo', 'declared'))!.revision
      for (const [index, payload] of values.entries()) {
        const key = `value-${index}`
        await schedules.upsert(Jobs, 'echo', { key, everyMs: 60_000, payload })
        await schedules.pause(Jobs, 'echo', key)
        await schedules.resume(Jobs, 'echo', key)
        assert.deepEqual((await schedules.get(Jobs, 'echo', key))?.payload, payload)
      }
      await schedules.upsert(Jobs, 'date', { key: 'date', everyMs: 60_000, payload: timestamp })
      await schedules.reconcile()
      assert.equal((await schedules.get(Jobs, 'echo', 'declared'))?.revision, pausedRevision)
    } finally {
      await deploy.close()
    }
    const first = child(target, script, 'schedules-child', path)
    const second = child(target, script, 'schedules-child', path)
    try {
      await Promise.all([first.ready(), second.ready()])
      const database = open(path)
      const due = Date.now() - 100
      try {
        database
          .prepare(
            "UPDATE better_effect_mq_schedules SET next_run_at_ms=? WHERE schedule_key<>'declared'"
          )
          .run(due)
      } finally {
        database.close()
      }
      await Promise.all([first.sweep(), second.sweep()])
      await Promise.all([first.sweep(), second.sweep()])
      const inspect = open(path)
      try {
        const rows = z
          .array(z.object({ id: z.string(), payload: z.string(), state: z.literal('waiting') }))
          .parse(
            inspect
              .prepare('SELECT id, payload, state FROM better_effect_mq_jobs ORDER BY id')
              .all()
          )
        assert.equal(rows.length, values.length + 1)
        assert.equal(new Set(rows.map((row) => row.id)).size, rows.length)
        for (const [index, value] of values.entries()) {
          const row = rows.find((entry) => entry.id === `sched/value-${index}/${due}`)
          assert.ok(row)
          assert.equal(row.payload, JSON.stringify(value))
        }
        const stored = z
          .array(z.object({ schedule_key: z.string(), payload: z.string(), namespace: z.string() }))
          .parse(
            inspect
              .prepare('SELECT schedule_key, payload, namespace FROM better_effect_mq_schedules')
              .all()
          )
        for (const [index, value] of values.entries())
          assert.equal(
            stored.find((row) => row.schedule_key === `value-${index}`)?.payload,
            JSON.stringify(value)
          )
        assert.deepEqual(
          inspect.prepare('SELECT DISTINCT namespace FROM better_effect_mq_jobs').all(),
          inspect.prepare('SELECT DISTINCT namespace FROM better_effect_mq_schedules').all()
        )
      } finally {
        inspect.close()
      }
      await Promise.all([first.stop(), second.stop()])
    } finally {
      await Promise.all([first.dispose(), second.dispose()])
    }
    // Only after both schedulers and the producer close does a new process execute the jobs.
    const worker = child(target, script, 'schedules-worker', path)
    try {
      await worker.ready()
      await worker.stop()
    } finally {
      await worker.dispose()
    }
    const reader = await application(api, path, 'reader')
    try {
      const schedules = reader.get(MqSchedulesService)
      const jobs = reader.get(Jobs)
      const paused = await schedules.get(Jobs, 'echo', 'declared')
      assert.equal(paused?.paused, true)
      assert.equal(paused.revision, pausedRevision)
      for (const [index, value] of values.entries()) {
        const record = await schedules.get(Jobs, 'echo', `value-${index}`)
        assert.ok(record?.lastJobId)
        assert.deepEqual(record.payload, value)
        assert.deepEqual(
          await jobs.echo.awaitResult(record.lastJobId, { strategy: 'events', timeoutMs: 1000 }),
          value
        )
      }
    } finally {
      await reader.close()
    }
    console.log(
      `PASS ${runtime}->${target} SQLite schedules: two independent schedulers, one occurrence, JSON/Date fidelity and results after process restart`
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}
