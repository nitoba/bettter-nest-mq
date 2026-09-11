import 'reflect-metadata'
import { Inject, Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { setTimeout } from 'node:timers/promises'
import { Pool } from 'pg'
import { JobContext, JobData, MqModule, Process, Worker, type JobExecutionContext } from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'
import { ControlQueues, GlobalQueue, KeyQueue, RateQueue, WorkerEnvironment } from './controls-contracts.js'

const config = WorkerEnvironment.parse(process.env)
const schema = config.MQ_CONTROL_SCHEMA
const pool = new Pool({ connectionString: config.MQ_TEST_DATABASE_URL, max: 8 })

@Injectable()
class Probe {
  async run(payload: { key: string; gate: string }, context: JobExecutionContext) {
    await pool.query(`INSERT INTO "${schema}".handler_probe(job_id,queue,key,pid) VALUES($1,$2,$3,$4)`, [context.jobId, context.queue, payload.key, process.pid])
    try {
      while (!context.signal.aborted) {
        const result = await pool.query<{ opened: boolean }>(`SELECT opened FROM "${schema}".gates WHERE name=$1`, [payload.gate])
        if (result.rows[0]?.opened) return { pid: process.pid, key: payload.key }
        await setTimeout(10)
      }
      context.signal.throwIfAborted()
      return { pid: process.pid, key: payload.key }
    } finally { await pool.query(`UPDATE "${schema}".handler_probe SET finished=true WHERE job_id=$1`, [context.jobId]) }
  }
}

@Injectable()
@Worker({ name: 'distributed-left', concurrency: 8, pollIntervalMs: 10, leaseDurationMs: 2_000, heartbeatIntervalMs: 100 })
class LeftWorker {
  constructor(@Inject(Probe) private readonly probe: Probe) {}
  @Process(GlobalQueue, 'left')
  global(@JobData() payload: { key: string; gate: string }, @JobContext() context: JobExecutionContext) { return this.probe.run(payload, context) }
  @Process(KeyQueue, 'left')
  key(@JobData() payload: { key: string; gate: string }, @JobContext() context: JobExecutionContext) { return this.probe.run(payload, context) }
  @Process(RateQueue, 'left')
  rate(@JobData() payload: { key: string; gate: string }, @JobContext() context: JobExecutionContext) { return this.probe.run(payload, context) }
}

@Injectable()
@Worker({ name: 'distributed-right', concurrency: 8, pollIntervalMs: 10, leaseDurationMs: 2_000, heartbeatIntervalMs: 100 })
class RightWorker {
  constructor(@Inject(Probe) private readonly probe: Probe) {}
  @Process(GlobalQueue, 'right')
  global(@JobData() payload: { key: string; gate: string }, @JobContext() context: JobExecutionContext) { return this.probe.run(payload, context) }
  @Process(KeyQueue, 'right')
  key(@JobData() payload: { key: string; gate: string }, @JobContext() context: JobExecutionContext) { return this.probe.run(payload, context) }
  @Process(RateQueue, 'right')
  rate(@JobData() payload: { key: string; gate: string }, @JobContext() context: JobExecutionContext) { return this.probe.run(payload, context) }
}

@Module({ imports: [
  MqModule.forRoot({ connections: { primary: postgres({ connectionString: config.MQ_TEST_DATABASE_URL, schema, namespace: 'controls' }) } }),
  MqModule.forFeature(ControlQueues)
], providers: [Probe, config.MQ_CONTROL_ROLE === 'left' ? LeftWorker : RightWorker] })
class WorkerModule {}

const stopped = new Promise<void>((resolve) => {
  process.on('message', (message) => { if (message === 'stop') resolve() })
  process.once('disconnect', resolve)
})
const app = await NestFactory.createApplicationContext(WorkerModule, { logger: false, abortOnError: false })
try {
  process.send?.({ type: 'ready', pid: process.pid })
  await stopped
} finally {
  try { await app.close() }
  finally { await pool.end(); process.disconnect?.() }
}
