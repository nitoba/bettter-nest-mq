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
  JobWaitAbortedException,
  JobWaitTimeoutException,
  MqJobException,
  MqModule,
  Process,
  Queue,
  QueueService,
  Retry,
  Worker,
  type JobExecutionContext,
  type MqConnection
} from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

interface SqliteEventApi {
  sqlite(options: {
    path: string
    namespace: string
    events: boolean
    pollIntervalMs: number
  }): MqConnection
  migrateSqlite(options: { path: string }): Promise<{ version: number }>
}
const values = ['null', '123', 'true', '{"value":1}', '', 'ação', null, false, 0, [], { a: 'null' }]
const date = zodCodec(
  z.codec(z.iso.datetime(), z.date(), {
    decode: (value) => new Date(value),
    encode: (value) => value.toISOString()
  })
)
@Queue({ name: 'sqlite-event-process', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
  @Job({ name: 'date', version: 1 })
  readonly date = this.job({ payload: date, result: date })
  @Job({ name: 'retry', version: 1 })
  @Retry({ attempts: 2, backoff: { type: 'fixed', delayMs: 10 } })
  readonly retry = this.job({
    payload: z.string(),
    result: z.string(),
    failure: z.object({ code: z.literal('temporary') }),
    retryable: () => true
  })
  @Job({ name: 'fail', version: 1 })
  readonly fail = this.job({
    payload: z.string(),
    result: z.string(),
    failure: z.object({ code: z.literal('terminal') })
  })
  @Job({ name: 'recover', version: 1 })
  @Retry({ attempts: 3 })
  readonly recover = this.job({ payload: z.string(), result: z.string() })
}
@Worker({
  name: 'sqlite-event-worker',
  concurrency: 2,
  pollIntervalMs: 10,
  leaseDurationMs: 1000,
  heartbeatIntervalMs: 200,
  stalledIntervalMs: 100,
  maxStalledCount: 2
})
class Processor {
  @Process(Jobs, 'echo')
  echo(@JobData() value: z.output<ReturnType<typeof z.json>>) {
    return value
  }
  @Process(Jobs, 'date')
  date(@JobData() value: Date) {
    assert.ok(value instanceof Date)
    return value
  }
  @Process(Jobs, 'retry')
  retry(@JobData() value: string, @JobContext() context: JobExecutionContext) {
    if (context.attempt === 1) throw new JobFailureException({ code: 'temporary' })
    return value
  }
  @Process(Jobs, 'fail')
  fail(@JobData() _value: string): string {
    throw new JobFailureException({ code: 'terminal' })
  }
  @Process(Jobs, 'recover')
  async recover(@JobData() value: string, @JobContext() context: JobExecutionContext) {
    if (process.argv[2] === 'events-crash') {
      process.stdout.write('SQLITE_EVENT_ACTIVE\n')
      await new Promise<void>((resolve) => {
        if (context.signal.aborted) resolve()
        else context.signal.addEventListener('abort', () => resolve(), { once: true })
      })
      context.signal.throwIfAborted()
    }
    return value
  }
}
async function application(
  api: SqliteEventApi,
  path: string,
  namespace: string,
  events: boolean,
  workers: boolean
) {
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: api.sqlite({ path, namespace, events, pollIntervalMs: 10 }) },
        execution: { workers }
      }),
      MqModule.forFeature([Jobs])
    ],
    providers: [Processor]
  })
  class Application {}
  return NestFactory.createApplicationContext(Application, { logger: false, abortOnError: false })
}
const wait = { strategy: 'events', pollFallbackMs: 30_000, timeoutMs: 15_000 } as const
const polling = { pollIntervalMs: 10, timeoutMs: 15_000 }
const timestamp = '2026-09-15T12:00:00.000Z'

async function verifyResults(jobs: Jobs, events: boolean): Promise<void> {
  const options = events ? wait : polling
  await Promise.all([
    ...values.map(async (value, index) => {
      assert.deepEqual(await jobs.echo.awaitResult(`value-${index}`, options), value)
    }),
    jobs.date.awaitResult('date', options).then((value) => {
      assert.deepEqual(value, new Date(timestamp))
    }),
    jobs.retry.awaitResult('retry', options).then((value) => {
      assert.equal(value, 'after retry')
    }),
    assert.rejects(jobs.fail.awaitResult('failure', options), (cause) => {
      assert.ok(cause instanceof JobFailureException)
      assert.deepEqual(cause.failure, { code: 'terminal' })
      return true
    }),
    assert.rejects(jobs.echo.awaitResult('cancelled', options), JobCancelledException)
  ])
  assert.deepEqual(
    (await jobs.retry.attempts('retry')).map((attempt) => attempt.outcome),
    ['retried', 'completed']
  )
}
function launch(
  runtime: 'node' | 'bun',
  script: string,
  mode: 'events-consume' | 'events-crash',
  path: string,
  namespace: string
) {
  const args = [script, mode, path, namespace]
  if (runtime === 'node') args.unshift('--experimental-sqlite')
  const child = spawn(runtime, args, { stdio: ['ignore', 'pipe', 'inherit'] })
  const active = new Promise<void>((resolve) => {
    let output = ''
    child.stdout.on('data', (chunk: Buffer) => {
      output += chunk.toString()
      if (output.includes('SQLITE_EVENT_ACTIVE\n')) resolve()
    })
  })
  const done = new Promise<{
    code: number | null
    signal: string | null
    error?: Error
  }>((resolve) => {
    const watchdog = setTimeout(() => child.kill('SIGKILL'), 20_000)
    child.once('error', (error) => {
      clearTimeout(watchdog)
      resolve({ code: null, signal: null, error })
    })
    child.once('exit', (code, signal) => {
      clearTimeout(watchdog)
      resolve({ code, signal })
    })
  })
  return { child, active, done }
}
async function stop(worker: ReturnType<typeof launch>): Promise<void> {
  if (worker.child.exitCode === null && worker.child.signalCode === null) {
    worker.child.kill('SIGKILL')
  }
  await worker.done
}

export async function verifySqliteEvents(api: SqliteEventApi, runtime: 'node' | 'bun') {
  const mode = process.argv[2]
  if (mode === 'events-consume' || mode === 'events-crash') {
    const path = process.argv[3]
    const namespace = process.argv[4]
    assert.ok(path && namespace)
    const app = await application(api, path, namespace, false, true)
    try {
      const jobs = app.get(Jobs)
      if (namespace === 'crash') {
        assert.equal(await jobs.recover.awaitResult('recover', polling), 'survived')
      } else {
        await verifyResults(jobs, false)
      }
    } finally {
      await app.close()
    }
    return
  }
  const entry = process.argv[1]
  assert.ok(entry)
  const target = mode === 'cross' ? (runtime === 'node' ? 'bun' : 'node') : runtime
  const script = join(dirname(entry), `sqlite-${target}.js`)
  const directory = await mkdtemp(join(tmpdir(), 'mq-sqlite-events-'))
  const path = join(directory, 'jobs.db')
  try {
    await api.migrateSqlite({ path })
    const producer = await application(api, path, 'events', false, false)
    try {
      const jobs = producer.get(Jobs)
      for (const [index, payload] of values.entries()) {
        await jobs.echo.enqueue(payload, { jobId: `value-${index}` })
      }
      await jobs.date.enqueue(timestamp, { jobId: 'date' })
      await jobs.retry.enqueue('after retry', { jobId: 'retry' })
      await jobs.fail.enqueue('terminal', { jobId: 'failure' })
      await jobs.echo.enqueue('cancel me', { jobId: 'cancelled' })
      await jobs.echo.cancel('cancelled')
      await assert.rejects(jobs.echo.awaitResult('value-0', wait), MqJobException)
    } finally {
      await producer.close()
    }
    const reader = await application(api, path, 'events', true, false)
    const results = verifyResults(reader.get(Jobs), true)
    const observed = Promise.allSettled([results])
    const worker = launch(target, script, 'events-consume', path, 'events')
    try {
      await results
      assert.equal((await worker.done).code, 0)
      const jobs = reader.get(Jobs)
      const delayed = await jobs.echo.enqueue('not cancelled', { delayMs: 60_000 })
      await assert.rejects(
        jobs.echo.awaitResult(delayed, { ...wait, timeoutMs: 20 }),
        JobWaitTimeoutException
      )
      const controller = new AbortController()
      const pending = jobs.echo.awaitResult(delayed, { ...wait, signal: controller.signal })
      const aborted = assert.rejects(pending, JobWaitAbortedException)
      controller.abort()
      await aborted
      assert.equal((await jobs.echo.poll(delayed))?.state, 'delayed')
    } finally {
      await stop(worker)
      await reader.close()
      await observed
    }
    const reopened = await application(api, path, 'events', true, false)
    try {
      await verifyResults(reopened.get(Jobs), true)
    } finally {
      await reopened.close()
    }
    const isolated = await application(api, path, 'isolated', true, false)
    try {
      assert.equal(await isolated.get(Jobs).echo.poll('value-0'), undefined)
    } finally {
      await isolated.close()
    }
    const recovery = await application(api, path, 'crash', true, false)
    try {
      const jobs = recovery.get(Jobs)
      await jobs.recover.enqueue('survived', { jobId: 'recover' })
      const crashed = launch(target, script, 'events-crash', path, 'crash')
      try {
        await Promise.race([
          crashed.active,
          crashed.done.then((exit) => {
            throw new Error(`Worker exited before its active claim: ${JSON.stringify(exit)}`)
          })
        ])
        assert.equal((await jobs.recover.poll('recover'))?.state, 'active')
        crashed.child.kill('SIGKILL')
        assert.equal((await crashed.done).signal, 'SIGKILL')
      } finally {
        await stop(crashed)
      }
      const replacement = launch(target, script, 'events-consume', path, 'crash')
      try {
        assert.equal(await jobs.recover.awaitResult('recover', wait), 'survived')
        assert.equal((await replacement.done).code, 0)
        assert.deepEqual(
          (await jobs.recover.attempts('recover')).map((attempt) => attempt.outcome),
          ['stalled', 'completed']
        )
      } finally {
        await stop(replacement)
      }
    } finally {
      await recovery.close()
    }
    console.log(
      `PASS ${runtime}->${target} SQLite events: separate readers/workers, JSON/Date, retries/failures, restart and SIGKILL recovery before polling fallback`
    )
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
}
