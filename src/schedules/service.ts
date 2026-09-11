import { Inject, Injectable, type Type } from '@nestjs/common'
import type { QueueService } from '../contracts/queue-service.ts'
import type { ScheduleListOptions, ScheduleOptions, ScheduledInput, ScheduledJobKey, SchedulesMonitor } from './types.ts'

export const SCHEDULE_MONITOR = Symbol('MqSchedulesMonitor')
@Injectable()
export class MqSchedulesService {
  constructor(@Inject(SCHEDULE_MONITOR) private readonly monitor: SchedulesMonitor) {}
  get<Queue extends QueueService, Key extends ScheduledJobKey<Queue>>(queue: Type<Queue>, property: Key, key: string, group?: string) { return this.monitor.get(queue, property, key, group) }
  list<Queue extends QueueService, Key extends ScheduledJobKey<Queue>>(queue: Type<Queue>, property: Key, options?: ScheduleListOptions) { return this.monitor.list(queue, property, options) }
  upsert<Queue extends QueueService, Key extends ScheduledJobKey<Queue>>(queue: Type<Queue>, property: Key, options: ScheduleOptions<ScheduledInput<Queue, Key>>) { return this.monitor.upsert(queue, property, options) }
  pause<Queue extends QueueService, Key extends ScheduledJobKey<Queue>>(queue: Type<Queue>, property: Key, key: string, group?: string) { return this.monitor.pause(queue, property, key, group) }
  resume<Queue extends QueueService, Key extends ScheduledJobKey<Queue>>(queue: Type<Queue>, property: Key, key: string, group?: string) { return this.monitor.resume(queue, property, key, group) }
  remove<Queue extends QueueService, Key extends ScheduledJobKey<Queue>>(queue: Type<Queue>, property: Key, key: string, group?: string) { return this.monitor.remove(queue, property, key, group) }
  reconcile() { return this.monitor.reconcile() }
  scheduler() { return this.monitor.scheduler() }
  sweep() { return this.monitor.sweep() }
}
