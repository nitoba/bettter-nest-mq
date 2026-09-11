import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { setTimeout as sleep } from 'node:timers/promises'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  JobCancelledException,
  MqModule,
  MqQueueControlsService,
  QueueControlsException
} from 'better-nest-mq'
import { migratePostgres, postgres } from 'better-nest-mq/postgres'
import { ControlQueues, GlobalQueue, KeyQueue, RateQueue } from './controls-contracts.js'

const Ready = z.object({ type: z.literal('ready'), pid: z.int().positive() })

class WorkerProcess {
  readonly child: ChildProcess
  readonly ready: Promise<number>
  readonly exited: Promise<number | null>
  private errors = ''
  constructor(connectionString: string, schema: string, role: 'left' | 'right') {
    this.child = spawn('node', [fileURLToPath(new URL('./controls-worker.js', import.meta.url))], {
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      env: {
        ...process.env,
        MQ_TEST_DATABASE_URL: connectionString,
        MQ_CONTROL_SCHEMA: schema,
        MQ_CONTROL_ROLE: role
      }
    })
    this.child.stderr?.on('data', (chunk: Buffer) => {
      this.errors = `${this.errors}${chunk.toString()}`.slice(-16_000)
    })
    this.exited = new Promise((resolve) => {
      this.child.once('exit', resolve)
    })
    this.ready = new Promise((resolve, reject) => {
      this.child.once('error', reject)
      this.child.once('exit', (code) =>
        reject(new Error(`Worker exited before readiness: ${code}: ${this.errors}`))
      )
      this.child.on('message', (message) => {
        const parsed = Ready.safeParse(message)
        if (parsed.success) resolve(parsed.data.pid)
      })
    })
  }
  async stop(): Promise<void> {
    if (this.child.connected) this.child.send('stop')
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 10_000)
    try {
      assert.equal(await this.exited, 0, this.errors)
    } finally {
      clearTimeout(timer)
    }
  }
}

async function until(check: () => Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 10_000
  while (!(await check())) {
    assert.ok(Date.now() < deadline, `Timed out: ${label}`)
    await sleep(10)
  }
}

async function verifyDistributed(connectionString: string): Promise<void> {
  const schema = `mq_distributed_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString, max: 5 })
  const children: WorkerProcess[] = []
  const connection = postgres({ connectionString, schema, namespace: 'controls' })
  const wait = { timeoutMs: 10_000, pollIntervalMs: 10 }
  try {
    await migratePostgres({ pool: admin, schema })
    // Audit triggers observe claims in their real database transaction. They do not implement,
    // lock or influence the production claim/permit algorithm; only this random test schema uses them.
    await admin.query(`
      CREATE TABLE "${schema}".gates(name text PRIMARY KEY, opened boolean NOT NULL DEFAULT false);
      INSERT INTO "${schema}".gates(name,opened) VALUES ('global',false),('keys',false),('rate',true);
      CREATE TABLE "${schema}".handler_probe(job_id text PRIMARY KEY,queue text NOT NULL,key text NOT NULL,pid integer NOT NULL,finished boolean NOT NULL DEFAULT false);
      CREATE TABLE "${schema}".claim_audit(job_id text NOT NULL,queue text NOT NULL,key text,active_total integer NOT NULL,active_key integer NOT NULL,window_start bigint);
      CREATE FUNCTION "${schema}".audit_claim() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.state='active' AND OLD.state<>'active' THEN
          INSERT INTO "${schema}".claim_audit
          SELECT NEW.id,NEW.queue,NEW.dispatch_key,
            (SELECT count(*) FROM "${schema}".better_effect_mq_jobs WHERE namespace=NEW.namespace AND queue=NEW.queue AND state='active'),
            (SELECT count(*) FROM "${schema}".better_effect_mq_jobs WHERE namespace=NEW.namespace AND queue=NEW.queue AND dispatch_key=NEW.dispatch_key AND state='active'),
            (SELECT started_at_ms FROM "${schema}".better_effect_mq_rate_windows WHERE namespace=NEW.namespace AND queue=NEW.queue);
        END IF;
        RETURN NEW;
      END $$;
      CREATE TRIGGER test_audit_claim AFTER UPDATE OF state ON "${schema}".better_effect_mq_jobs FOR EACH ROW EXECUTE FUNCTION "${schema}".audit_claim();
    `)

    @Module({
      imports: [
        MqModule.forRoot({
          connections: { primary: connection },
          execution: { workers: false },
          controls: { mode: 'reconcile' }
        }),
        MqModule.forFeature(ControlQueues)
      ]
    })
    class DeployModule {}
    @Module({
      imports: [
        MqModule.forRoot({ connections: { primary: connection }, execution: { workers: false } }),
        MqModule.forFeature(ControlQueues)
      ]
    })
    class ProducerModule {}
    const deploy = await NestFactory.createApplicationContext(DeployModule, {
      logger: false,
      abortOnError: false
    })
    let revision: number
    try {
      const controls = deploy.get(MqQueueControlsService)
      const record = await controls.get(GlobalQueue)
      assert.ok(record)
      revision = record.revision
      await controls.reconcile()
      assert.equal((await controls.get(GlobalQueue))?.revision, revision)
    } finally {
      await deploy.close()
    }

    const producer = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      const left = new WorkerProcess(connectionString, schema, 'left')
      const right = new WorkerProcess(connectionString, schema, 'right')
      children.push(left, right)
      const readyTimeout = setTimeout(() => {
        left.child.kill('SIGKILL')
        right.child.kill('SIGKILL')
      }, 10_000)
      let pids: number[]
      try {
        pids = await Promise.all([left.ready, right.ready])
      } finally {
        clearTimeout(readyTimeout)
      }
      assert.equal(new Set(pids).size, 2, 'Two independent worker PIDs must actually start')
      assert.ok(pids.every((pid) => pid !== process.pid))
      const controls = producer.get(MqQueueControlsService)
      assert.equal((await controls.get(GlobalQueue))?.revision, revision)
      await assert.rejects(controls.reconcile(), QueueControlsException)
      const entered = async (id: string): Promise<boolean> =>
        (await admin.query(`SELECT 1 FROM "${schema}".handler_probe WHERE job_id=$1`, [id]))
          .rowCount === 1

      const global = producer.get(GlobalQueue)
      const gLeft = await global.left.enqueue({ key: 'left', gate: 'global' })
      await until(() => entered(gLeft), 'left global worker entered')
      const gRight = await global.right.enqueueMany(
        Array.from({ length: 5 }, (_, index) => ({
          payload: { key: `right-${index}`, gate: 'global' }
        }))
      )
      await until(
        async () =>
          (
            await admin.query(
              `SELECT 1 FROM "${schema}".handler_probe WHERE queue='distributed-global' AND NOT finished`
            )
          ).rowCount === 2,
        'global capacity reached across processes'
      )
      const activePids = await admin.query<{ pid: number }>(
        `SELECT DISTINCT pid FROM "${schema}".handler_probe WHERE queue='distributed-global' AND NOT finished`
      )
      assert.equal(activePids.rows.length, 2, 'Global concurrency must combine both processes')
      await admin.query(`UPDATE "${schema}".gates SET opened=true WHERE name='global'`)
      await Promise.all([
        global.left.awaitResult(gLeft, wait),
        ...gRight.map((id) => global.right.awaitResult(id, wait))
      ])
      const globalPeak = await admin.query<{ peak: number }>(
        `SELECT max(active_total)::integer AS peak FROM "${schema}".claim_audit WHERE queue='distributed-global'`
      )
      assert.equal(globalPeak.rows[0]?.peak, 2)
      console.log(
        'PASS global concurrency=2 across two independently running Node worker processes'
      )

      const keyed = producer.get(KeyQueue)
      const held = await keyed.left.enqueue({ key: 'A', gate: 'keys' })
      await until(() => entered(held), 'held key A entered')
      const blocked = await keyed.right.enqueue({ key: 'A', gate: 'keys' })
      const other = await keyed.right.enqueue({ key: 'B', gate: 'keys' })
      await until(() => entered(other), 'unrelated key B progresses while key A is saturated')
      assert.equal(await entered(blocked), false)
      await keyed.left.cancel(held)
      await assert.rejects(keyed.left.awaitResult(held, wait), JobCancelledException)
      await until(() => entered(blocked), 'cancellation released the key A permit')
      await admin.query(`UPDATE "${schema}".gates SET opened=true WHERE name='keys'`)
      await Promise.all([
        keyed.right.awaitResult(blocked, wait),
        keyed.right.awaitResult(other, wait)
      ])
      const keyPeak = await admin.query<{ peak: number }>(
        `SELECT max(active_key)::integer AS peak FROM "${schema}".claim_audit WHERE queue='distributed-keys'`
      )
      assert.equal(keyPeak.rows[0]?.peak, 1)
      console.log(
        'PASS per-key serialization, unrelated-key progress and permit release after active cancellation'
      )

      const rate = producer.get(RateQueue)
      const rLeft = await rate.left.enqueueMany(
        Array.from({ length: 4 }, (_, index) => ({
          payload: { key: `left-${index}`, gate: 'rate' }
        }))
      )
      const rRight = await rate.right.enqueueMany(
        Array.from({ length: 4 }, (_, index) => ({
          payload: { key: `right-${index}`, gate: 'rate' }
        }))
      )
      await Promise.all([
        ...rLeft.map((id) => rate.left.awaitResult(id, wait)),
        ...rRight.map((id) => rate.right.awaitResult(id, wait))
      ])
      const windows = await admin.query<{ window_start: string | null; total: number }>(
        `SELECT window_start,count(*)::integer AS total FROM "${schema}".claim_audit WHERE queue='distributed-rate' GROUP BY window_start ORDER BY window_start`
      )
      assert.ok(windows.rows.length >= 4)
      assert.ok(
        windows.rows.every((row) => row.window_start !== null && row.total <= 2),
        'No persisted fixed window may admit more than two jobs across processes'
      )
      assert.equal(
        windows.rows.reduce((sum, row) => sum + row.total, 0),
        8
      )
      console.log(
        'PASS shared fixed-window rate limit: all eight claims audited against persisted window identities'
      )

      const permits = await admin.query<{ total: number }>(
        `SELECT count(*)::integer AS total FROM "${schema}".better_effect_mq_controlled_permits`
      )
      assert.equal(
        permits.rows[0]?.total,
        0,
        'Completion and cancellation must release every permit'
      )
    } finally {
      await producer.close()
    }
    for (const child of children) await child.stop()
    children.length = 0
    const restarted = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      assert.equal(
        (await restarted.get(MqQueueControlsService).get(GlobalQueue))?.revision,
        revision
      )
    } finally {
      await restarted.close()
    }
    console.log(
      'PASS policy validation and revision persistence after all producer/worker contexts close'
    )
  } finally {
    await admin.query(`UPDATE "${schema}".gates SET opened=true`).catch((cause) => {
      console.error('Test gate cleanup failed', cause)
    })
    try {
      await Promise.all(children.map((child) => child.stop()))
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
if (connectionString !== undefined) await verifyDistributed(connectionString)
else
  console.log(
    'Packed controls declarations/types passed; independent-process execution runs in PostgreSQL CI'
  )
