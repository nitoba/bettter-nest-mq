import { Inject, Injectable } from '@nestjs/common'
import type { WorkerIdleOptions, WorkerMonitor, WorkerSnapshot } from './types.ts'

export const WORKER_MONITOR = Symbol('MqWorkerMonitor')

@Injectable()
export class MqWorkersService implements WorkerMonitor {
  constructor(@Inject(WORKER_MONITOR) private readonly monitor: WorkerMonitor) {}
  workers(): ReadonlyArray<WorkerSnapshot> { return this.monitor.workers() }
  awaitIdle(options?: WorkerIdleOptions): Promise<void> { return this.monitor.awaitIdle(options) }
}
