import { Inject, Injectable } from '@nestjs/common'
import type { FlowJobReference } from './references.ts'
import type { FlowMonitor } from './types.ts'
export const FLOW_MONITOR = Symbol('MqFlowsMonitor')
@Injectable()
export class MqFlowsService {
  constructor(@Inject(FLOW_MONITOR) private readonly monitor: FlowMonitor) {}
  get(job: FlowJobReference, id: string) {
    return this.monitor.get(job, id)
  }
  cancel(job: FlowJobReference, id: string) {
    return this.monitor.cancel(job, id)
  }
}
