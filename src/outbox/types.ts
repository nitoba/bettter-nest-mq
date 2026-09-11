import type { PreparedJob } from '../jobs/types.ts'

export type OutboxState = 'pending' | 'active' | 'published' | 'failed'
export interface OutboxEntry {
  readonly id: string
  readonly job: PreparedJob
  /** Publication attempts, independent of the job's handler-attempt budget. */
  readonly attempts?: number
  readonly runAtMs?: number
}
export interface MqOutboxOptions {
  readonly concurrency?: number
  readonly leaseDurationMs?: number
  readonly heartbeatIntervalMs?: number
  readonly pollIntervalMs?: number
  readonly retryBaseDelayMs?: number
  readonly retryMaxDelayMs?: number
}
export interface OutboxFailure {
  readonly kind:
    | 'target-missing'
    | 'request-invalid'
    | 'store-transient'
    | 'store-permanent'
    | 'settlement-uncertain'
  readonly code?: string
  readonly message: string
  readonly retryable: boolean
  readonly recordedAtMs: number
}
/** Read model deliberately excludes the active lease token. */
export interface OutboxSnapshot {
  readonly id: string
  readonly source: string
  readonly target: string
  readonly state: OutboxState
  readonly request: PreparedJob['request']
  readonly attemptsMax: number
  readonly attemptsMade: number
  readonly runAtMs: number
  readonly createdAtMs: number
  readonly updatedAtMs: number
  readonly publishedAtMs: number | undefined
  readonly failure: OutboxFailure | undefined
}
export interface OutboxAppendResult {
  readonly record: OutboxSnapshot
  readonly duplicate: boolean
}
export interface OutboxCounts {
  readonly pending: number
  readonly active: number
  readonly published: number
  readonly failed: number
  readonly total: number
}
export interface OutboxListOptions {
  readonly state?: OutboxState | readonly OutboxState[]
  readonly target?: string
  readonly limit?: number
}
export interface OutboxPublisherSnapshot {
  readonly id: string
  readonly state: 'running' | 'stopping' | 'stopped'
  readonly activeCount: number
}
export interface OutboxMonitor {
  get(source: string, id: string): Promise<OutboxSnapshot | undefined>
  list(source: string, options?: OutboxListOptions): Promise<readonly OutboxSnapshot[]>
  counts(source: string): Promise<OutboxCounts>
  publisher(): OutboxPublisherSnapshot | undefined
}
