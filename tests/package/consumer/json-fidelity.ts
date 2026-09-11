import { poolMethodDescriptors } from './pool-methods.js'
import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Client, Pool, types } from 'pg'
import { z } from 'zod'
import {
  Job,
  JobContext,
  JobData,
  JobFailureException,
  MqJobException,
  MqModule,
  Process,
  Queue,
  QueueControls,
  QueueService,
  Retry,
  Worker,
  type JobExecutionContext,
  type MqConnection
} from 'better-nest-mq'
import { migratePostgres, postgres } from 'better-nest-mq/postgres'

type JsonValue = z.input<ReturnType<typeof z.json>>
interface JsonCase {
  readonly value: JsonValue
  readonly kind: string
}
const cases: readonly JsonCase[] = [
  { value: 'retry me', kind: 'string' },
  { value: '', kind: 'string' },
  { value: 'null', kind: 'string' },
  { value: 'true', kind: 'string' },
  { value: '123', kind: 'string' },
  { value: '"quoted"', kind: 'string' },
  { value: '{"nested":1}', kind: 'string' },
  { value: '[1,2]', kind: 'string' },
  { value: 'ação 🌱\n"quoted"\\', kind: 'string' },
  { value: 123, kind: 'number' },
  { value: -4.5, kind: 'number' },
  { value: 0, kind: 'number' },
  { value: true, kind: 'boolean' },
  { value: false, kind: 'boolean' },
  { value: null, kind: 'null' },
  { value: [], kind: 'array' },
  { value: [1, 'x', null], kind: 'array' },
  { value: { nested: ['null', null, false, 0], text: 'retry me' }, kind: 'object' }
]

class JsonJobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
  @Job({ name: 'reject', version: 1 })
  readonly reject = this.job({ payload: z.json(), result: z.json(), failure: z.json() })
  @Job({ name: 'retry', version: 1 })
  @Retry({ attempts: 2, backoff: { type: 'fixed', delayMs: 5 } })
  readonly retrying = this.job({
    payload: z.json(),
    result: z.json(),
    failure: z.json(),
    retryable: () => true
  })
}
@Queue({ name: 'json-ordinary', connection: 'primary' })
class OrdinaryJson extends JsonJobs {}
@Queue({ name: 'json-controlled', connection: 'primary' })
@QueueControls({ globalConcurrency: 4 })
class ControlledJson extends JsonJobs {}
const queues = [OrdinaryJson, ControlledJson]

function retryValue(value: JsonValue, context: JobExecutionContext): JsonValue {
  if (context.attempt === 1) throw new JobFailureException(value)
  return value
}

@Worker({ name: 'json-worker', concurrency: 8, pollIntervalMs: 5 })
class JsonWorker {
  @Process(OrdinaryJson, 'echo')
  echo(@JobData() value: JsonValue) {
    return value
  }
  @Process(ControlledJson, 'echo')
  controlledEcho(@JobData() value: JsonValue) {
    return value
  }
  @Process(OrdinaryJson, 'reject')
  reject(@JobData() value: JsonValue): never {
    throw new JobFailureException(value)
  }
  @Process(ControlledJson, 'reject')
  controlledReject(@JobData() value: JsonValue): never {
    throw new JobFailureException(value)
  }
  @Process(OrdinaryJson, 'retrying')
  retrying(@JobData() value: JsonValue, @JobContext() context: JobExecutionContext) {
    return retryValue(value, context)
  }
  @Process(ControlledJson, 'retrying')
  controlledRetry(@JobData() value: JsonValue, @JobContext() context: JobExecutionContext) {
    return retryValue(value, context)
  }
}

interface Published {
  readonly queue: typeof OrdinaryJson | typeof ControlledJson
  readonly successes: readonly { readonly id: string; readonly sample: JsonCase }[]
  readonly failures: readonly { readonly id: string; readonly sample: JsonCase }[]
  readonly retries: readonly { readonly id: string; readonly sample: JsonCase }[]
}
const wait = { timeoutMs: 10_000, pollIntervalMs: 10 }

async function assertStoredJson(
  admin: Pool,
  schema: string,
  id: string,
  sample: JsonCase
): Promise<void> {
  const result = await admin.query<{
    payload_kind: string
    result_kind: string
    payload_matches: boolean
    result_matches: boolean
    result_missing: boolean
  }>(
    `SELECT jsonb_typeof(payload) AS payload_kind,jsonb_typeof(result) AS result_kind,
        payload=$2::jsonb AS payload_matches,result=$2::jsonb AS result_matches,result IS NULL AS result_missing
      FROM "${schema}".better_effect_mq_jobs WHERE id=$1`,
    [id, JSON.stringify(sample.value)]
  )
  assert.deepEqual(result.rows, [
    {
      payload_kind: sample.kind,
      result_kind: sample.kind,
      payload_matches: true,
      result_matches: true,
      result_missing: false
    }
  ])
}

async function nativeParsing(pool: Pool, custom: boolean): Promise<void> {
  const result = await pool.query(
    'SELECT $1::jsonb AS value,$2::jsonb AS json_null,NULL::jsonb AS sql_null',
    [JSON.stringify('123'), 'null']
  )
  assert.deepEqual(result.rows, [
    {
      value: custom ? { applicationValue: '123' } : '123',
      json_null: custom ? { applicationValue: null } : null,
      sql_null: null
    }
  ])
}

async function runOwnership(
  connectionString: string,
  ownership: 'owned' | 'borrowed' | 'custom-parsers'
): Promise<void> {
  const schema = `mq_json_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString, max: 3 })
  const template = new Client()
  if (ownership === 'custom-parsers')
    template.setTypeParser(3802, (text) => ({ applicationValue: JSON.parse(text) }))
  const pool = new Pool({
    connectionString,
    max: 10,
    types: { getTypeParser: template.getTypeParser.bind(template) }
  })
  const original = {
    methods: poolMethodDescriptors(pool),
    json: types.getTypeParser(3802)
  }
  const connection: MqConnection =
    ownership === 'owned'
      ? postgres({ connectionString, schema, namespace: 'json-fidelity' })
      : postgres({ pool, schema, namespace: 'json-fidelity' })

  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: connection },
        execution: { workers: false },
        controls: { mode: 'reconcile' }
      }),
      MqModule.forFeature(queues)
    ]
  })
  class DeploymentModule {}
  @Module({
    imports: [
      MqModule.forRoot({ connections: { primary: connection }, execution: { workers: false } }),
      MqModule.forFeature(queues)
    ]
  })
  class ReaderModule {}
  @Module({
    imports: [
      MqModule.forRoot({ connections: { primary: connection } }),
      MqModule.forFeature(queues)
    ],
    providers: [JsonWorker]
  })
  class ConsumerModule {}

  const published: Published[] = []
  try {
    await migratePostgres({ pool: admin, schema })
    await nativeParsing(pool, ownership === 'custom-parsers')
    const producer = await NestFactory.createApplicationContext(DeploymentModule, {
      logger: false,
      abortOnError: false
    })
    try {
      for (const QueueType of queues) {
        const queue = producer.get(QueueType)
        const successes: { id: string; sample: JsonCase }[] = []
        const failures: { id: string; sample: JsonCase }[] = []
        const retries: { id: string; sample: JsonCase }[] = []
        for (const sample of cases) {
          const preparedId = randomUUID()
          const prepared = await queue.echo.prepare(sample.value, { jobId: preparedId })
          assert.deepEqual(prepared.request.payload, sample.value)
          assert.equal(await queue.echo.poll(preparedId), undefined)
          successes.push({ id: await queue.echo.enqueue(sample.value), sample })
          successes.push({ id: await queue.echo.enqueueDecoded(sample.value), sample })
          failures.push({ id: await queue.reject.enqueue(sample.value), sample })
          retries.push({ id: await queue.retrying.enqueue(sample.value), sample })
        }
        const batch = await queue.echo.enqueueMany(
          cases.map((sample) => ({ payload: sample.value }))
        )
        batch.forEach((id, index) => {
          const sample = cases[index]
          assert.ok(sample)
          successes.push({ id, sample })
        })
        for (const { id, sample } of successes) {
          const pending = await queue.echo.poll(id)
          assert.equal(pending?.state, 'waiting')
          assert.equal(pending.result, undefined)
          assert.deepEqual(pending.payload, sample.value)
        }
        published.push({ queue: QueueType, successes, failures, retries })
      }
      await nativeParsing(pool, ownership === 'custom-parsers')
    } finally {
      await producer.close()
    }

    const consumer = await NestFactory.createApplicationContext(ConsumerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      for (const entry of published) {
        const queue = consumer.get(entry.queue)
        for (const { id, sample } of entry.successes)
          assert.deepEqual(await queue.echo.awaitResult(id, wait), sample.value)
        for (const { id, sample } of entry.failures) {
          await assert.rejects(queue.reject.awaitResult(id, wait), (error) => {
            assert.ok(error instanceof JobFailureException)
            assert.deepEqual(error.failure, sample.value)
            return true
          })
        }
        for (const { id, sample } of entry.retries)
          assert.deepEqual(await queue.retrying.awaitResult(id, wait), sample.value)
      }
      await nativeParsing(pool, ownership === 'custom-parsers')
    } finally {
      await consumer.close()
    }

    const reader = await NestFactory.createApplicationContext(ReaderModule, {
      logger: false,
      abortOnError: false
    })
    try {
      for (const entry of published) {
        const queue = reader.get(entry.queue)
        for (const { id, sample } of entry.successes) {
          const job = await queue.echo.poll(id)
          assert.equal(job?.state, 'completed')
          assert.deepEqual(job.result, sample.value)
          assert.deepEqual((await queue.echo.attempts(id))[0]?.result, sample.value)
          await assertStoredJson(admin, schema, id, sample)
        }
        for (const { id, sample } of entry.failures) {
          const job = await queue.reject.poll(id)
          assert.equal(job?.failure?.kind, 'typed')
          assert.deepEqual(job.failure.data, sample.value)
          assert.deepEqual((await queue.reject.attempts(id))[0]?.failure?.data, sample.value)
        }
        for (const { id, sample } of entry.retries) {
          const attempts = await queue.retrying.attempts(id)
          assert.deepEqual(
            attempts.map((attempt) => attempt.outcome),
            ['retried', 'completed']
          )
          assert.deepEqual(attempts[0]?.failure?.data, sample.value)
          assert.deepEqual(attempts[1]?.result, sample.value)
        }
        // SQL NULL is still absence, not a successful JSON null. A corrupt completed row
        // must reject instead of manufacturing a valid null result.
        const nullSample = entry.successes.find((value) => value.sample.kind === 'null')
        assert.ok(nullSample)
        await admin.query(`UPDATE "${schema}".better_effect_mq_jobs SET result=NULL WHERE id=$1`, [
          nullSample.id
        ])
        await assert.rejects(queue.echo.awaitResult(nullSample.id, wait), MqJobException)
      }
    } finally {
      await reader.close()
    }
    await nativeParsing(pool, ownership === 'custom-parsers')
    assert.deepEqual(poolMethodDescriptors(pool), original.methods)
    assert.equal(types.getTypeParser(3802), original.json)
    console.log(
      `PASS packed JSON fidelity (${ownership}): 18 values, ordinary/controlled queues, single/decoded/batch, preparation, retries, typed failures, SQL null distinction and post-restart reads`
    )
  } finally {
    // The application drains its own pool before test-only destructive DDL.
    try {
      await pool.end()
    } finally {
      try {
        await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      } finally {
        await admin.end()
      }
    }
  }
}

const connectionString = process.env.MQ_TEST_DATABASE_URL
if (connectionString !== undefined) {
  for (const ownership of ['owned', 'borrowed', 'custom-parsers'] as const)
    await runOwnership(connectionString, ownership)
} else {
  console.log(
    'Packed JSON contracts compile without a database; live coverage runs in PostgreSQL CI'
  )
}
