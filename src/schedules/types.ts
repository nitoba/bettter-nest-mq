import type { Type } from '@nestjs/common'
import type { JobContract, InputOf } from '../contracts/job-definition.ts'
import type { QueueService } from '../contracts/queue-service.ts'
import type { RetryOptions } from '../contracts/policies.ts'
import type { JobJsonValue, PreparedBackoff } from '../jobs/types.ts'

export type ScheduleMisfire =
  | { readonly strategy: 'skip' | 'run-once' }
  | { readonly strategy: 'catch-up'; readonly maxOccurrences: number }
export type ScheduleCadence =
  | { readonly cron: string; readonly everyMs?: never }
  | { readonly everyMs: number; readonly cron?: never }
export type ScheduleOptions<Input = JobJsonValue> = ScheduleCadence & {
  readonly key: string
  readonly group?: string
  readonly payload: Input
  readonly timeZone?: string
  readonly metadata?: Readonly<Record<string, string>>
  readonly retry?: RetryOptions
  readonly priority?: number
  readonly timeoutMs?: number
  readonly misfire?: ScheduleMisfire
  readonly overlap?: 'allow' | 'skip'
}
export interface MqScheduleOptions {
  readonly mode?: 'validate' | 'reconcile'
  readonly group?: string
  readonly sweepIntervalMs?: number
  readonly batchSize?: number
  readonly maxStoreRetries?: number
  readonly retryDelayMs?: number
}
export interface ScheduleSnapshot {
  readonly connection: string
  readonly group: string
  readonly key: string
  readonly job: { readonly queue: string; readonly name: string; readonly version: number }
  readonly queue: string
  readonly cron: string | undefined
  readonly everyMs: number | undefined
  readonly timeZone: string | undefined
  readonly payload: JobJsonValue
  readonly metadata: Readonly<Record<string, string>>
  readonly priority: number
  readonly attemptsMax: number
  readonly timeoutMs: number | undefined
  readonly backoff: PreparedBackoff | undefined
  readonly misfire: ScheduleMisfire
  readonly overlap: 'allow' | 'skip'
  readonly paused: boolean
  readonly revision: number
  readonly nextRunAtMs: number
  readonly lastScheduledAtMs: number | undefined
  readonly lastJobId: string | undefined
  readonly createdAtMs: number
  readonly updatedAtMs: number
}
export interface ScheduleReport {
  readonly connection: string
  readonly group: string
  readonly created: readonly string[]
  readonly updated: readonly string[]
  readonly unchanged: readonly string[]
}
export interface ScheduleListOptions {
  readonly group?: string
  readonly paused?: boolean
  readonly limit?: number
}
export interface SchedulerSnapshot {
  readonly state: 'running' | 'quiescing' | 'draining' | 'stopped'
  readonly activeTickCount: number
  readonly reportedErrors: number
}
export type ScheduledJobKey<Queue extends QueueService> = {
  [Key in keyof Queue]: Queue[Key] extends JobContract ? Key : never
}[keyof Queue] &
  string
export type ScheduledInput<
  Queue extends QueueService,
  Key extends ScheduledJobKey<Queue>
> = Queue[Key] extends JobContract ? InputOf<Queue[Key]> : never

export interface SchedulesMonitor {
  get(
    queue: Type<QueueService>,
    property: string,
    key: string,
    group?: string
  ): Promise<ScheduleSnapshot | undefined>
  list(
    queue: Type<QueueService>,
    property: string,
    options?: ScheduleListOptions
  ): Promise<readonly ScheduleSnapshot[]>
  upsert<Input>(
    queue: Type<QueueService>,
    property: string,
    options: ScheduleOptions<Input>
  ): Promise<ScheduleSnapshot>
  pause(queue: Type<QueueService>, property: string, key: string, group?: string): Promise<void>
  resume(queue: Type<QueueService>, property: string, key: string, group?: string): Promise<void>
  remove(queue: Type<QueueService>, property: string, key: string, group?: string): Promise<boolean>
  reconcile(): Promise<readonly ScheduleReport[]>
  scheduler(): SchedulerSnapshot | undefined
  sweep(): Promise<void>
}
