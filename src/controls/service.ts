import { Inject, Injectable, type Type } from '@nestjs/common'
import type { QueueService } from '../contracts/queue-service.ts'
import type { QueueControlsMonitor, QueueControlsReport, QueueControlsSnapshot } from './types.ts'

export const QUEUE_CONTROLS_MONITOR = Symbol('MqQueueControlsMonitor')

@Injectable()
export class MqQueueControlsService implements QueueControlsMonitor {
  constructor(@Inject(QUEUE_CONTROLS_MONITOR) private readonly monitor: QueueControlsMonitor) {}
  get(queue: Type<QueueService>): Promise<QueueControlsSnapshot | undefined> { return this.monitor.get(queue) }
  reconcile(): Promise<readonly QueueControlsReport[]> { return this.monitor.reconcile() }
}
