import type { Type } from '@nestjs/common'
import type { JobContract } from '../contracts/job-definition.ts'
import { QueueService } from '../contracts/queue-service.ts'
import { MqFlowException } from './errors.ts'
import type { FlowChildInput } from './types.ts'
import { copyFlowInput, flowFields, flowName, flowChildOptions } from './validation.ts'
import { hardFlowMaxChildren, maxFlowChildKeyLength } from 'better-effect-mq'

type JobKey<Queue extends QueueService> = {
  [Key in keyof Queue]: Queue[Key] extends JobContract ? Key : never
}[keyof Queue] &
  string
export class FlowJobReference<Job extends JobContract = JobContract> {
  /** Type witness only. No job instance is constructed by a reference. */
  declare readonly contract: Job
  constructor(
    readonly queue: Type<QueueService>,
    readonly property: string
  ) {
    if (!(queue.prototype instanceof QueueService))
      throw new MqFlowException('definition', 'A flow job requires a QueueService class')
    flowName(property, 'flow.job.property', 256)
    Object.freeze(this)
  }
}
export function flowJob<Queue extends QueueService, Key extends JobKey<Queue>>(
  queue: Type<Queue>,
  property: Key
): FlowJobReference<Extract<Queue[Key], JobContract>> {
  return new FlowJobReference(queue, property)
}
export class FlowChildPlan<Job extends JobContract = JobContract> {
  readonly items: readonly FlowChildInput<Job>[]
  constructor(
    readonly job: FlowJobReference<Job>,
    items: readonly FlowChildInput<Job>[]
  ) {
    if (
      !(job instanceof FlowJobReference) ||
      !Array.isArray(items) ||
      items.length > hardFlowMaxChildren
    )
      throw new MqFlowException(
        'definition',
        'Child plans require a flowJob reference and a bounded array'
      )
    const keys = new Set<string>()
    this.items = Object.freeze(
      items.map((item) => {
        flowFields(item, ['key', 'payload', 'options'])
        const key = flowName(item.key, 'flow.child.key', maxFlowChildKeyLength)
        if (keys.has(key)) throw new MqFlowException('definition', 'Duplicate child key')
        keys.add(key)
        const base = { key, payload: copyFlowInput(item.payload) }
        return Object.freeze(
          item.options === undefined ? base : { ...base, options: flowChildOptions(item.options) }
        )
      })
    )
    Object.freeze(this)
  }
}
export function flowChildren<Job extends JobContract>(
  job: FlowJobReference<Job>,
  items: readonly FlowChildInput<NoInfer<Job>>[]
): FlowChildPlan<Job> {
  return new FlowChildPlan(job, items)
}
