import type { JobContract, InputOf, ResultOf, FailureOf } from '../contracts/job-definition.ts'
import type { JobIdentity } from '../contracts/queue-definition.ts'
import type { RetryOptions } from '../contracts/policies.ts'
import type { FlowJobReference, FlowChildPlan } from './references.ts'

export interface FlowChildOptions {
  readonly priority?: number
  readonly retry?: RetryOptions
  readonly timeoutMs?: number
  readonly metadata?: Readonly<Record<string, string>>
}
export interface FlowChildInput<Job extends JobContract> {
  readonly key: string
  readonly payload: InputOf<Job>
  readonly options?: FlowChildOptions
}
export interface FlowOptions {
  readonly name: string
  readonly parent: FlowJobReference
  readonly children: readonly FlowJobReference[]
  readonly onChildFailure: 'fail' | 'continue'
  readonly maxChildren?: number
  readonly maxDepth?: number
}
export type FlowManifest = readonly FlowChildPlan[]
export interface FlowCounts {
  readonly pending: number
  readonly completed: number
  readonly failed: number
  readonly cancelled: number
}
export type FlowChildResult<Job extends JobContract> = {
  readonly key: string
  readonly job: FlowJobReference<Job>
} & (
  | { readonly outcome: 'completed'; readonly result: ResultOf<Job> }
  | { readonly outcome: 'failed'; readonly failure: FailureOf<Job> | undefined }
  | { readonly outcome: 'cancelled' }
)
export interface FlowPageOptions {
  readonly cursor?: string
  readonly limit?: number
}
export interface FlowChildPage<Job extends JobContract> {
  readonly items: readonly FlowChildResult<Job>[]
  /** Cursor over the entire manifest, even when this page contains no selected job. */
  readonly nextCursor: string | undefined
}
export interface FlowResultsReader {
  /** Counts for the complete flow, not only a selected child reference. */
  readonly counts: FlowCounts
  page<Job extends JobContract>(
    job: FlowJobReference<Job>,
    options?: FlowPageOptions
  ): Promise<FlowChildPage<Job>>
  all<Job extends JobContract>(
    job: FlowJobReference<Job>,
    options: { readonly maxItems: number }
  ): Promise<readonly FlowChildResult<Job>[]>
}
export interface FlowSnapshot {
  readonly id: string
  readonly name: string
  readonly parent: JobIdentity
  readonly state: 'active' | 'waiting-children' | 'waiting' | 'completed' | 'failed' | 'cancelled'
  readonly depth: number
  readonly counts: FlowCounts
  readonly children: number
}
export interface FlowMonitor {
  get(job: FlowJobReference, id: string): Promise<FlowSnapshot | undefined>
  cancel(job: FlowJobReference, id: string): Promise<void>
}
