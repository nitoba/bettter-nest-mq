import type { RetryOptions } from '../contracts/policies.ts'

/** Portable JSON data; this type deliberately has no dependency on engine protocol brands. */
export type JobJsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JobJsonValue[]
  | { readonly [key: string]: JobJsonValue }
export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'cancelled'
export type JobFailureKind =
  | 'typed'
  | 'defect'
  | 'encode'
  | 'decode'
  | 'timeout'
  | 'stalled'
  | 'cancelled'
export type JobAttemptOutcome =
  | 'completed'
  | 'retried'
  | 'failed'
  | 'cancelled'
  | 'stalled'
  | 'released'

export type JobScheduleOptions =
  | { readonly delayMs?: never; readonly at?: never }
  | { readonly delayMs: number; readonly at?: never }
  | { readonly delayMs?: never; readonly at: number }

export type JobEnqueueOptions = JobScheduleOptions & {
  readonly jobId?: string
  readonly idempotencyKey?: string
  readonly dispatchKey?: string
  readonly metadata?: Readonly<Record<string, string>>
  readonly priority?: number
  readonly timeoutMs?: number
  readonly retry?: RetryOptions
}

export interface JobEnqueueItem<Input> {
  readonly payload: Input
  readonly options?: JobEnqueueOptions
}

export interface JobWaitOptions {
  readonly strategy?: 'polling'
  readonly pollIntervalMs?: number
  readonly timeoutMs?: number
  readonly signal?: AbortSignal
}

/** Separate options prevent confusing the execution timeout with the caller's waiting timeout. */
export interface JobExecuteOptions {
  readonly enqueue?: JobEnqueueOptions
  readonly wait?: JobWaitOptions
}

interface JobFailureDetails {
  readonly message: string
  readonly code?: string
  readonly retryable: boolean
  readonly recordedAt: number
}

export type JobFailureView<Failure> = JobFailureDetails &
  (
    | { readonly kind: 'typed'; readonly data: Failure }
    | { readonly kind: Exclude<JobFailureKind, 'typed'>; readonly data?: JobJsonValue }
  )

/** Safe job view: the mutable lease token is intentionally not exposed. */
export interface JobSnapshot<Success, Failure = never> {
  readonly id: string
  readonly queue: string
  readonly name: string
  readonly version: number
  readonly state: JobState
  readonly payload: JobJsonValue
  readonly metadata: Readonly<Record<string, string>>
  readonly priority: number
  readonly runAt: number
  readonly attemptsMax: number
  readonly attemptsMade: number
  readonly deliveryCount: number
  readonly stalledCount: number
  readonly createdAt: number
  readonly updatedAt: number
  readonly processedAt: number | undefined
  readonly finishedAt: number | undefined
  readonly cancellationRequestedAt: number | undefined
  readonly result: Success | undefined
  readonly failure: JobFailureView<Failure> | undefined
}

export interface JobAttempt<Success, Failure = never> {
  readonly attempt: number
  readonly delivery: number
  readonly startedAt: number | undefined
  readonly finishedAt: number
  readonly outcome: JobAttemptOutcome
  readonly result: Success | undefined
  readonly failure: JobFailureView<Failure> | undefined
  readonly retryAt?: number
  readonly retryDelayMs?: number
}

export interface PreparedBackoff {
  readonly type: 'constant' | 'linear' | 'exponential'
  readonly delayMs: number
  readonly incrementMs?: number
  readonly factor?: number
  readonly maxDelayMs?: number
  readonly jitter?: number
}

/** Encoded, storage-neutral data. Preparation neither enqueues nor joins a domain transaction. */
export interface PreparedJob {
  readonly connection: string
  readonly request: {
    readonly protocolVersion: 1
    readonly identity: { readonly queue: string; readonly name: string; readonly version: number }
    readonly id?: string
    readonly idempotencyKey?: string
    readonly payload: JobJsonValue
    readonly metadata: Readonly<Record<string, string>>
    readonly priority: number
    readonly runAt: number
    readonly attemptsMax: number
    readonly now: number
    readonly dispatchKey?: string
    readonly timeoutMs?: number
    readonly backoff?: PreparedBackoff
  }
}
