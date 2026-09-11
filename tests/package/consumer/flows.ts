import 'reflect-metadata'
import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import { MqModule, MqFlowsService, MqFlowException, JobFailureException } from 'better-nest-mq'
import { postgres, migratePostgres } from 'better-nest-mq/postgres'
import {
  FlowQueue,
  FlowValue,
  FlowWorkers,
  FLOW_POOL,
  FLOW_SETTINGS,
  batch,
  item,
  fast,
  nested
} from './flow-contracts.js'

const readyMessage = z.object({ type: z.literal('ready'), pid: z.int() })
class FlowProcess {
  readonly child: ChildProcess
  readonly ready: Promise<number>
  readonly exited: Promise<number | null>
  private errors = ''
  constructor(connectionString: string, schema: string, hold: boolean) {
    this.child = spawn('node', [fileURLToPath(new URL('./flow-child.js', import.meta.url))], {
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      env: {
        ...process.env,
        MQ_TEST_DATABASE_URL: connectionString,
        MQ_FLOW_SCHEMA: schema,
        MQ_FLOW_HOLD: String(hold)
      }
    })
    this.child.stderr?.on('data', (chunk: Buffer) => {
      this.errors = `${this.errors}${chunk.toString()}`.slice(-10_000)
    })
    this.exited = new Promise((resolve) => this.child.once('exit', resolve))
    this.ready = new Promise((resolve, reject) => {
      this.child.once('error', reject)
      this.child.once('exit', (code) =>
        reject(new Error(`Flow process exited ${code}: ${this.errors}`))
      )
      this.child.on('message', (message) => {
        const parsed = readyMessage.safeParse(message)
        if (parsed.success) resolve(parsed.data.pid)
      })
    })
  }
  async start(): Promise<number> {
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 10_000)
    try {
      return await this.ready
    } finally {
      clearTimeout(timer)
    }
  }
  async kill(): Promise<void> {
    this.child.kill('SIGKILL')
    await this.exited
  }
  async close(): Promise<void> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return
    if (this.child.connected) this.child.send({ type: 'stop' })
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 8_000)
    try {
      assert.equal(await this.exited, 0, this.errors)
    } finally {
      clearTimeout(timer)
    }
  }
}
async function until(predicate: () => Promise<boolean>, description: string): Promise<void> {
  const end = Date.now() + 12_000
  while (!(await predicate())) {
    assert.ok(Date.now() < end, description)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}
async function verify(connectionString: string): Promise<void> {
  const schema = `mq_flow_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 8 })
  const processes: FlowProcess[] = []
  const connection = postgres({
    pool,
    schema,
    namespace: 'flows',
    flows: true,
    schedules: true,
    outbox: true
  })
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: connection },
        execution: { workers: false, scheduler: false, outboxPublisher: false }
      }),
      MqModule.forFeature([FlowQueue])
    ],
    providers: [
      { provide: FLOW_POOL, useValue: pool },
      { provide: FLOW_SETTINGS, useValue: { schema, hold: false } },
      ...FlowWorkers
    ]
  })
  class ProducerModule {}
  const wait = { timeoutMs: 15_000, pollIntervalMs: 10 }
  try {
    await migratePostgres({ pool, schema })
    await pool.query(
      `CREATE TABLE "${schema}".phase_audit(job_id text NOT NULL, phase text NOT NULL, delivery integer NOT NULL, pid integer NOT NULL)`
    )
    const producer = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      const queue = producer.get(FlowQueue)
      const service = producer.get(MqFlowsService)
      const held = new FlowProcess(connectionString, schema, true)
      processes.push(held)
      await held.start()
      const restartId = await queue.batch.enqueue({ values: ['persisted', 123, null] })
      await until(
        async () => (await service.get(batch, restartId))?.counts.pending === 3,
        'Fan-out must persist before the crash'
      )
      const before = await pool.query(
        `SELECT child_key,child_job_id FROM "${schema}".better_effect_mq_flow_children WHERE flow_id=$1 ORDER BY child_key`,
        [restartId]
      )
      assert.equal(before.rows.length, 3)
      await held.kill()
      const left = new FlowProcess(connectionString, schema, false)
      const right = new FlowProcess(connectionString, schema, false)
      processes.push(left, right)
      assert.equal(new Set(await Promise.all([left.start(), right.start()])).size, 2)
      assert.deepEqual(await queue.batch.awaitResult(restartId, wait), {
        values: ['persisted', 123, null],
        failed: 0,
        cancelled: 0
      })
      const after = await pool.query(
        `SELECT child_key,child_job_id FROM "${schema}".better_effect_mq_flow_children WHERE flow_id=$1 ORDER BY child_key`,
        [restartId]
      )
      assert.deepEqual(after.rows, before.rows)
      const audit = await pool.query<{ phase: string; count: number }>(
        `SELECT phase,count(*)::integer AS count FROM "${schema}".phase_audit WHERE job_id=$1 GROUP BY phase`,
        [restartId]
      )
      assert.equal(
        audit.rows.find((row) => row.phase === 'fanOut')?.count,
        1,
        'The persisted fan-out must not be replayed after restart'
      )
      assert.equal(
        audit.rows.find((row) => row.phase === 'collect')?.count,
        1,
        'A released fan-out delivery must not invoke user Collect'
      )
      console.log(
        'PASS durable manifest/child ids across SIGKILL and two independent replacement processes'
      )

      const inputs: z.output<typeof FlowValue>[] = [
        'true',
        '123',
        '{"looks":"json"}',
        '',
        'ação',
        123,
        false,
        null,
        ['array'],
        { text: 'object' }
      ]
      assert.deepEqual(await queue.batch.execute({ values: inputs }, { wait }), {
        values: inputs,
        failed: 0,
        cancelled: 0
      })
      assert.deepEqual(await queue.batch.execute({ values: [] }, { wait }), {
        values: [],
        failed: 0,
        cancelled: 0
      })
      assert.deepEqual(
        await queue.batch.execute({ values: ['bad', 'ok', null], fail: true }, { wait }),
        { values: ['ok', null], failed: 1, cancelled: 0 }
      )
      const timestamp = '2026-09-11T12:00:00.000Z'
      assert.deepEqual(await queue.dateParent.execute(timestamp, { wait }), new Date(timestamp))
      const nestedId = await queue.nested.enqueue(['a', 'b'])
      assert.equal(await queue.nested.awaitResult(nestedId, wait), 4)
      assert.equal((await service.get(nested, nestedId))?.depth, 1)
      console.log(
        'PASS scalar/null/Date codecs, paged outcomes, typed failure continuation, empty and nested flows'
      )

      const failId = await queue.fast.enqueue({ values: ['bad', 'cancelled'], hold: true })
      await assert.rejects(queue.fast.awaitResult(failId, wait), JobFailureException)
      await until(
        async () => (await service.get(fast, failId))?.state === 'failed',
        'Fail-fast parent must settle'
      )
      const failureAudit = await pool.query<{ count: number }>(
        `SELECT count(*)::integer AS count FROM "${schema}".phase_audit WHERE job_id=$1 AND phase='unexpected-fast-collect'`,
        [failId]
      )
      assert.equal(failureAudit.rows[0]?.count, 0)
      const cancelId = await queue.batch.enqueue({ values: ['hold', 'pending'], hold: true })
      await until(
        async () => (await service.get(batch, cancelId))?.counts.pending === 2,
        'Cancellation needs a materialized manifest'
      )
      await assert.rejects(service.get(item, cancelId), MqFlowException)
      await service.cancel(batch, cancelId)
      await until(
        async () => (await service.get(batch, cancelId))?.state === 'cancelled',
        'Parent cancellation must persist'
      )
      await assert.rejects(queue.batch.awaitResult(cancelId, wait))
      await until(async () => {
        const rows = await pool.query<{ count: number }>(
          `SELECT count(*)::integer AS count FROM "${schema}".better_effect_mq_jobs WHERE metadata->>'__better_effect_flow_v2.flowId'=$1 AND state NOT IN ('cancelled','completed','failed')`,
          [cancelId]
        )
        return rows.rows[0]?.count === 0
      }, 'Cascade cancellation must reach child jobs')
      console.log(
        'PASS fail-fast without Collect and cooperative cascade cancellation with contract isolation'
      )
    } finally {
      await producer.close()
    }
    assert.equal((await pool.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
  } finally {
    try {
      await Promise.all(processes.map((child) => child.close()))
    } finally {
      try {
        await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      } finally {
        await pool.end()
      }
    }
  }
}
const connectionString = process.env.MQ_TEST_DATABASE_URL
if (connectionString !== undefined) await verify(connectionString)
else
  console.log(
    'Packed flow contracts/types passed; independent-process PostgreSQL scenarios run in database CI'
  )
