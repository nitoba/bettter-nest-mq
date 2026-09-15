import 'reflect-metadata'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { z } from 'zod'
import {
  Job,
  JobContext,
  JobData,
  JobCancelledException,
  JobFailureException,
  JobWaitTimeoutException,
  MqConnectionsService,
  MqModule,
  Process,
  Queue,
  QueueService,
  Retry,
  Worker,
  type MqConnection,
  type JobExecutionContext
} from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

interface SqliteApi {
  sqlite(options: { path: string; namespace?: string; pollIntervalMs?: number }): MqConnection
  migrateSqlite(options: { path: string }): Promise<{ version: number; applied: readonly number[] }>
  validateSqlite(options: { path: string }): Promise<void>
}
const values = [
  '123',
  'true',
  'null',
  '{"value":1}',
  '',
  'ação',
  0,
  7,
  false,
  null,
  ['array'],
  { value: 'object' }
]
const timestamp = zodCodec(
  z.codec(z.iso.datetime(), z.date(), {
    decode: (value) => new Date(value),
    encode: (date) => date.toISOString()
  })
)
@Queue({ name: 'packed-sqlite', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
  @Job({ name: 'retry', version: 1 })
  @Retry({ attempts: 2, backoff: { type: 'fixed', delayMs: 10 } })
  readonly retry = this.job({
    payload: z.string(),
    result: z.string(),
    failure: z.object({ code: z.literal('temporary') }),
    retryable: () => true
  })
  @Job({ name: 'date', version: 1 })
  readonly date = this.job({ payload: timestamp, result: timestamp })
}
@Worker({ name: 'sqlite-consumer', concurrency: 2, pollIntervalMs: 5 })
class Processor {
  @Process(Jobs, 'echo')
  echo(@JobData() value: z.output<ReturnType<typeof z.json>>) {
    return value
  }
  @Process(Jobs, 'retry')
  retry(@JobData() value: string, @JobContext() context: JobExecutionContext) {
    if (context.attempt === 1) throw new JobFailureException({ code: 'temporary' })
    return value
  }
  @Process(Jobs, 'date')
  date(@JobData() value: Date) {
    assert.ok(value instanceof Date)
    return value
  }
}
async function app(api: SqliteApi, path: string, workers: boolean, namespace = 'native-hosts') {
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: api.sqlite({ path, namespace, pollIntervalMs: 5 }) },
        execution: { workers }
      }),
      MqModule.forFeature([Jobs])
    ],
    providers: [Processor]
  })
  class Application {}
  return NestFactory.createApplicationContext(Application, { logger: false, abortOnError: false })
}
const wait = { timeoutMs: 5_000, pollIntervalMs: 5 }

async function consume(api: SqliteApi, path: string): Promise<void> {
  const worker = await app(api, path, true)
  try {
    const jobs = worker.get(Jobs)
    assert.deepEqual(
      await Promise.all(values.map((_, index) => jobs.echo.awaitResult(`json-${index}`, wait))),
      values
    )
    assert.equal(await jobs.retry.awaitResult('retry', wait), 'after retry')
    assert.deepEqual(
      (await jobs.retry.attempts('retry')).map((attempt) => attempt.outcome),
      ['retried', 'completed']
    )
    assert.deepEqual(
      await jobs.date.awaitResult('date', wait),
      new Date('2026-09-15T12:00:00.000Z')
    )
  } finally {
    await worker.close()
  }
}
export async function verifySqlite(api: SqliteApi, runtime: 'node' | 'bun'): Promise<void> {
  const externalPath = process.argv[3]
  if (process.argv[2] === 'consume') {
    assert.ok(externalPath)
    await consume(api, externalPath)
    return
  }
  const directory = await mkdtemp(join(tmpdir(), `mq-packed-${runtime}-`))
  const path = join(directory, 'jobs.db')
  try {
    const migration = await api.migrateSqlite({ path })
    assert.ok(migration.applied.length > 0)
    assert.deepEqual((await api.migrateSqlite({ path })).applied, [])
    await api.validateSqlite({ path })
    const producer = await app(api, path, false)
    try {
      const jobs = producer.get(Jobs)
      assert.equal(producer.get(MqConnectionsService).connections()[0]?.ownership, 'owned')
      await jobs.echo.enqueueMany(
        values.map((payload, index) => ({ payload, options: { jobId: `json-${index}` } }))
      )
      await jobs.retry.enqueue('after retry', { jobId: 'retry' })
      await jobs.date.enqueueDecoded(new Date('2026-09-15T12:00:00.000Z'), { jobId: 'date' })
      assert.equal(
        await jobs.echo.enqueue('deduplicated', { idempotencyKey: 'stable' }),
        await jobs.echo.enqueue('deduplicated', { idempotencyKey: 'stable' })
      )
      await jobs.echo.prepare('prepared', { jobId: 'never-published' })
      assert.equal(await jobs.echo.poll('never-published'), undefined)
      const cancelled = await jobs.echo.enqueue('cancelled')
      await jobs.echo.cancel(cancelled)
      await assert.rejects(jobs.echo.awaitResult(cancelled, wait), JobCancelledException)
      const delayed = await jobs.echo.enqueue('promoted', { delayMs: 60_000, jobId: 'delayed' })
      assert.equal((await jobs.echo.poll(delayed))?.state, 'delayed')
      await jobs.echo.promote(delayed)
      await assert.rejects(
        jobs.echo.awaitResult('json-0', { timeoutMs: 10 }),
        JobWaitTimeoutException
      )
      assert.equal((await jobs.echo.poll('json-0'))?.state, 'waiting')
    } finally {
      await producer.close()
    }
    // A genuinely new process must read the producer's file and process its jobs.
    const script = process.argv[1]
    assert.ok(script)
    const target = process.argv[2] === 'cross' ? (runtime === 'node' ? 'bun' : 'node') : runtime
    const targetScript = target === runtime ? script : join(dirname(script), `sqlite-${target}.js`)
    const args =
      target === 'node'
        ? ['--experimental-sqlite', targetScript, 'consume', path]
        : [targetScript, 'consume', path]
    const child = spawn(target, args, { stdio: 'inherit' })
    const timeout = setTimeout(() => child.kill('SIGKILL'), 15_000)
    try {
      await new Promise<void>((resolve, reject) => {
        child.once('error', reject)
        child.once('exit', (code, signal) =>
          code === 0 ? resolve() : reject(new Error(`SQLite consumer exited ${code}/${signal}`))
        )
      })
    } finally {
      clearTimeout(timeout)
    }
    const reader = await app(api, path, false)
    try {
      const jobs = reader.get(Jobs)
      for (const [index, value] of values.entries())
        assert.deepEqual((await jobs.echo.poll(`json-${index}`))?.result, value)
      assert.equal(await jobs.echo.awaitResult('delayed', wait), 'promoted')
      assert.equal((await jobs.echo.attempts('json-0'))[0]?.outcome, 'completed')
    } finally {
      await reader.close()
    }
    const isolated = await app(api, path, false, 'another-namespace')
    try {
      assert.equal(await isolated.get(Jobs).echo.poll('json-0'), undefined)
    } finally {
      await isolated.close()
    }
    console.log(
      `PASS ${runtime}->${target} SQLite: native file persistence, independent worker process, JSON/null/Date, retries, cancellation, promotion and namespace isolation`
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}
