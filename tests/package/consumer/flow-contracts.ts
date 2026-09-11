import 'reflect-metadata'
import { Injectable, Inject } from '@nestjs/common'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  Job,
  Queue,
  QueueService,
  Flow,
  FanOut,
  Collect,
  FlowData,
  FlowChildren,
  flowJob,
  flowChildren,
  JobData,
  JobContext,
  JobFailureException,
  Worker,
  Process,
  type JobExecutionContext,
  type FlowResultsReader,
  type InputOf,
  type PayloadOf,
  type WorkerOptions
} from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

export const FlowValue = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
  z.array(z.string()),
  z.object({ text: z.string() })
])
const Failure = z.object({ reason: z.string() })
const Input = z.object({
  values: z.array(FlowValue),
  fail: z.boolean().optional(),
  hold: z.boolean().optional()
})
const Item = z.object({
  value: FlowValue,
  fail: z.boolean().optional(),
  hold: z.boolean().optional()
})
const Summary = z.object({ values: z.array(FlowValue), failed: z.int(), cancelled: z.int() })
const date = z.codec(z.iso.datetime(), z.date(), {
  decode: (value) => new Date(value),
  encode: (value) => value.toISOString()
})
@Injectable()
@Queue({ name: 'durable-flows', connection: 'primary' })
export class FlowQueue extends QueueService {
  @Job({ name: 'batch', version: 1 })
  readonly batch = this.job({ payload: Input, result: Summary, failure: Failure })
  @Job({ name: 'item', version: 1 })
  readonly item = this.job({ payload: Item, result: FlowValue, failure: Failure })
  @Job({ name: 'fast', version: 1 })
  readonly fast = this.job({ payload: Input, result: Summary, failure: Failure })
  @Job({ name: 'fast-item', version: 1 })
  readonly fastItem = this.job({ payload: Item, result: FlowValue, failure: Failure })
  @Job({ name: 'date-parent', version: 1 })
  readonly dateParent = this.job({ payload: zodCodec(date), result: zodCodec(date) })
  @Job({ name: 'date-item', version: 1 })
  readonly dateItem = this.job({ payload: zodCodec(date), result: zodCodec(date) })
  @Job({ name: 'nested', version: 1 })
  readonly nested = this.job({ payload: z.array(FlowValue), result: z.number(), failure: Failure })
  @Job({ name: 'length', version: 1 })
  readonly length = this.job({ payload: z.number(), result: z.number() })
}
export const batch = flowJob(FlowQueue, 'batch')
export const item = flowJob(FlowQueue, 'item')
export const fast = flowJob(FlowQueue, 'fast')
export const fastItem = flowJob(FlowQueue, 'fastItem')
export const dateParent = flowJob(FlowQueue, 'dateParent')
export const dateItem = flowJob(FlowQueue, 'dateItem')
export const nested = flowJob(FlowQueue, 'nested')
export const length = flowJob(FlowQueue, 'length')
export const FLOW_POOL = Symbol('FlowTestPool')
export const FLOW_SETTINGS = Symbol('FlowTestSettings')
export interface FlowTestSettings {
  readonly schema: string
  readonly hold: boolean
}
export const FlowEnvironment = z.object({
  MQ_TEST_DATABASE_URL: z.string().min(1),
  MQ_FLOW_SCHEMA: z.string().regex(/^mq_flow_[a-f0-9]+$/),
  MQ_FLOW_HOLD: z.enum(['true', 'false']).default('false')
})
const workerOptions: Omit<WorkerOptions, 'name'> = {
  concurrency: 1,
  pollIntervalMs: 5,
  flowSweepIntervalMs: 20,
  flowBatchSize: 2,
  leaseDurationMs: 1_000,
  heartbeatIntervalMs: 200,
  stalledIntervalMs: 100,
  maxStalledCount: 3
}
class PhaseAudit {
  constructor(
    @Inject(FLOW_POOL) protected readonly pool: Pool,
    @Inject(FLOW_SETTINGS) protected readonly settings: FlowTestSettings
  ) {}
  async audit(context: JobExecutionContext, phase: string): Promise<void> {
    await this.pool.query(
      `INSERT INTO "${this.settings.schema}".phase_audit(job_id,phase,delivery,pid) VALUES ($1,$2,$3,$4)`,
      [context.jobId, phase, context.delivery, process.pid]
    )
  }
  async handleItem(value: PayloadOf<FlowQueue['item']>, context: JobExecutionContext) {
    await this.audit(context, 'item')
    if (value.fail) throw new JobFailureException({ reason: 'rejected child' })
    if (this.settings.hold || value.hold) {
      await new Promise<void>((resolve) => {
        if (context.signal.aborted) {
          resolve()
          return
        }
        context.signal.addEventListener('abort', () => resolve(), { once: true })
      })
      context.signal.throwIfAborted()
    }
    return value.value
  }
}
@Worker({ name: 'flow-batch', ...workerOptions })
@Flow({
  name: 'batch',
  parent: batch,
  children: [item],
  onChildFailure: 'continue',
  maxChildren: 500
})
export class BatchWorker extends PhaseAudit {
  @FanOut()
  async split(
    @FlowData() input: PayloadOf<FlowQueue['batch']>,
    @JobContext() context: JobExecutionContext
  ) {
    await this.audit(context, 'fanOut')
    return [
      flowChildren(
        item,
        input.values.map((value, index) => ({
          key: `item-${String(index).padStart(3, '0')}`,
          payload: { value, fail: input.fail === true && index === 0, hold: input.hold ?? false }
        }))
      )
    ]
  }
  @Collect()
  async collect(
    @FlowChildren() results: FlowResultsReader,
    @JobContext() context: JobExecutionContext
  ) {
    await this.audit(context, 'collect')
    // Read several actual persisted pages; a caller must follow the manifest cursor.
    const values: z.output<typeof FlowValue>[] = []
    let cursor: string | undefined
    do {
      const page = await results.page(
        item,
        cursor === undefined ? { limit: 2 } : { limit: 2, cursor }
      )
      for (const child of page.items) {
        if (child.outcome === 'completed') values.push(child.result)
        if (child.outcome === 'failed' && child.failure?.reason !== 'rejected child')
          throw new Error('Typed child failure was not decoded')
      }
      cursor = page.nextCursor
    } while (cursor !== undefined)
    return { values, failed: results.counts.failed, cancelled: results.counts.cancelled }
  }
  @Process(FlowQueue, 'item')
  item(@JobData() input: PayloadOf<FlowQueue['item']>, @JobContext() context: JobExecutionContext) {
    return this.handleItem(input, context)
  }
}
@Worker({ name: 'flow-fast', ...workerOptions })
@Flow({
  name: 'fast',
  parent: fast,
  children: [fastItem],
  onChildFailure: 'fail',
  maxChildren: 500
})
export class FastWorker extends PhaseAudit {
  @FanOut()
  async split(
    @FlowData() input: PayloadOf<FlowQueue['fast']>,
    @JobContext() context: JobExecutionContext
  ) {
    await this.audit(context, 'fanOut')
    return [
      flowChildren(
        fastItem,
        input.values.map((value, index) => ({
          key: `item-${index}`,
          payload: { value, fail: index === 0, hold: input.hold ?? false }
        }))
      )
    ]
  }
  @Collect()
  async collect(@JobContext() context: JobExecutionContext) {
    await this.audit(context, 'unexpected-fast-collect')
    return { values: [], failed: 0, cancelled: 0 }
  }
  @Process(FlowQueue, 'fastItem')
  item(
    @JobData() input: PayloadOf<FlowQueue['fastItem']>,
    @JobContext() context: JobExecutionContext
  ) {
    return this.handleItem(input, context)
  }
}
@Worker({ name: 'flow-date', ...workerOptions })
@Flow({ name: 'date', parent: dateParent, children: [dateItem], onChildFailure: 'continue' })
export class DateWorker {
  @FanOut() split(@FlowData() value: Date) {
    return [flowChildren(dateItem, [{ key: 'date', payload: value.toISOString() }])]
  }
  @Collect() async collect(@FlowChildren() results: FlowResultsReader) {
    const children = await results.all(dateItem, { maxItems: 1 })
    const child = children[0]
    if (child?.outcome !== 'completed' || !(child.result instanceof Date))
      throw new Error('Date result was not decoded')
    return child.result
  }
  @Process(FlowQueue, 'dateItem') item(@JobData() value: Date) {
    return value
  }
}
@Worker({ name: 'flow-nested', ...workerOptions })
@Flow({
  name: 'nested',
  parent: nested,
  children: [batch, length],
  onChildFailure: 'continue',
  maxDepth: 3
})
export class NestedWorker {
  @FanOut() split(@FlowData() values: InputOf<FlowQueue['nested']>) {
    return [
      flowChildren(batch, [{ key: 'batch', payload: { values } }]),
      flowChildren(length, [{ key: 'length', payload: values.length }])
    ]
  }
  @Collect() async collect(@FlowChildren() results: FlowResultsReader) {
    const batches = await results.all(batch, { maxItems: 1 })
    const lengths = await results.all(length, { maxItems: 1 })
    const current = batches[0]
    const count = lengths[0]
    if (current?.outcome !== 'completed' || count?.outcome !== 'completed')
      throw new Error('Nested child did not complete')
    return current.result.values.length + count.result
  }
  @Process(FlowQueue, 'length') length(@JobData() value: number) {
    return value
  }
}
export const FlowWorkers = [BatchWorker, FastWorker, DateWorker, NestedWorker]
