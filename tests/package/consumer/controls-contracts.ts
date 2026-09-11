import 'reflect-metadata'
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, Queue, QueueControls, QueueService } from 'better-nest-mq'

export const WorkPayload = z.object({ key: z.string(), gate: z.string() })
export const WorkResult = z.object({ pid: z.int(), key: z.string() })

class WorkQueue extends QueueService {
  @Job({ name: 'left', version: 1 })
  readonly left = this.job({
    payload: WorkPayload,
    result: WorkResult,
    dispatchKey: (payload) => payload.key
  })
  @Job({ name: 'right', version: 1 })
  readonly right = this.job({
    payload: WorkPayload,
    result: WorkResult,
    dispatchKey: (payload) => payload.key
  })
}

@Injectable()
@Queue({ name: 'distributed-global', connection: 'primary' })
@QueueControls({ globalConcurrency: 2 })
export class GlobalQueue extends WorkQueue {}

@Injectable()
@Queue({ name: 'distributed-keys', connection: 'primary' })
@QueueControls({ globalConcurrency: 4, perKeyConcurrency: 1 })
export class KeyQueue extends WorkQueue {}

@Injectable()
@Queue({ name: 'distributed-rate', connection: 'primary' })
@QueueControls({ rateLimit: { max: 2, durationMs: 250 } })
export class RateQueue extends WorkQueue {}

export const ControlQueues = [GlobalQueue, KeyQueue, RateQueue]
export const WorkerEnvironment = z.object({
  MQ_TEST_DATABASE_URL: z.string().min(1),
  MQ_CONTROL_SCHEMA: z.string().regex(/^mq_distributed_[a-f0-9]+$/),
  MQ_CONTROL_ROLE: z.enum(['left', 'right'])
})
