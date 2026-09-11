import { Inject, Injectable } from '@nestjs/common'
import { z } from 'zod'
import {
  Job, JobContext, JobData, JobFailureException, JobTimeout, Process, Queue, QueueService,
  Retry, Worker, type JobExecutionContext, type PayloadOf, type ResultOf
} from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

const timestamp = z.codec(z.iso.datetime(), z.date(), {
  decode: (value) => new Date(value), encode: (value) => value.toISOString()
})

@Injectable()
@Queue({ name: 'execution', connection: 'primary' })
export class ExecutionQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.object({ value: z.string().min(1) }), result: z.object({ value: z.string() }) })

  @Job({ name: 'codec', version: 1 })
  readonly codec = this.job({ payload: zodCodec(z.object({ timestamp })), result: zodCodec(z.object({ timestamp })) })

  @Job({ name: 'retry', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'linear', initialDelayMs: 5, incrementMs: 5 } })
  readonly retrying = this.job({
    payload: z.object({ succeedAt: z.int().positive(), retryable: z.boolean() }),
    result: z.object({ attempt: z.int() }),
    failure: z.object({ code: z.literal('unavailable'), retryable: z.boolean() }),
    retryable: (failure) => failure.retryable
  })

  @Job({ name: 'unique', version: 1 })
  readonly unique = this.job({ payload: z.object({ key: z.string() }), result: z.string(), idempotencyKey: (payload) => payload.key })

  @Job({ name: 'defect', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'fixed', delayMs: 5 } })
  readonly defect = this.job({ payload: z.string(), result: z.string() })

  @Job({ name: 'invalid-result', version: 1 })
  readonly invalidResult = this.job({ payload: z.string(), result: z.object({ count: z.int() }) })

  @Job({ name: 'timeout', version: 1 })
  @JobTimeout(30)
  readonly timeout = this.job({ payload: z.string(), result: z.string() })
}

@Injectable()
export class ExecutionLog {
  readonly contexts: JobExecutionContext[] = []
  readonly defects: string[] = []
  readonly decodedDates: Date[] = []
}

@Injectable()
@Worker({ name: 'execution-worker', concurrency: 4, pollIntervalMs: 5, leaseDurationMs: 300, heartbeatIntervalMs: 50 })
export class ExecutionWorker {
  constructor(@Inject(ExecutionLog) private readonly log: ExecutionLog) {}

  @Process(ExecutionQueue, 'echo')
  async echo(@JobData() payload: PayloadOf<ExecutionQueue['echo']>, @JobContext() context: JobExecutionContext): Promise<ResultOf<ExecutionQueue['echo']>> {
    this.log.contexts.push(context)
    return { value: payload.value.toUpperCase() }
  }

  @Process(ExecutionQueue, 'codec')
  async codec(@JobData() payload: PayloadOf<ExecutionQueue['codec']>): Promise<ResultOf<ExecutionQueue['codec']>> {
    this.log.decodedDates.push(payload.timestamp)
    return { timestamp: new Date(payload.timestamp.getTime() + 1_000) }
  }

  @Process(ExecutionQueue, 'retrying')
  async retrying(@JobData() payload: PayloadOf<ExecutionQueue['retrying']>, @JobContext() context: JobExecutionContext) {
    this.log.contexts.push(context)
    if (context.attempt < payload.succeedAt) throw new JobFailureException({ code: 'unavailable', retryable: payload.retryable })
    return { attempt: context.attempt }
  }

  @Process(ExecutionQueue, 'unique')
  unique(@JobData() payload: PayloadOf<ExecutionQueue['unique']>) { return payload.key }

  @Process(ExecutionQueue, 'defect')
  defect(@JobData() payload: string): never {
    this.log.defects.push(payload)
    throw new Error('unexpected defect')
  }

  @Process(ExecutionQueue, 'invalidResult')
  invalidResult(@JobData() payload: string) { return { count: payload } }

  @Process(ExecutionQueue, 'timeout')
  async timeout(@JobContext() context: JobExecutionContext): Promise<string> {
    this.log.contexts.push(context)
    await new Promise<void>((resolve) => {
      if (context.signal.aborted) return resolve()
      context.signal.addEventListener('abort', () => resolve(), { once: true })
    })
    return 'too late'
  }
}
