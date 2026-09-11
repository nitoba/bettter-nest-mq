export interface WorkerOptions {
  readonly name: string
  readonly concurrency?: number
  readonly leaseDurationMs?: number
  readonly heartbeatIntervalMs?: number
  readonly stalledIntervalMs?: number
  readonly maxStalledCount?: number
  readonly pollIntervalMs?: number
  readonly retryDefects?: boolean
}

export interface ProcessOptions { readonly concurrency?: number }

/** This is a job attempt context, never an HTTP Request or a mutable lease handle. */
export interface JobExecutionContext {
  readonly jobId: string
  readonly queue: string
  readonly name: string
  readonly version: number
  readonly connection: string
  readonly attempt: number
  readonly attemptsMax: number
  readonly delivery: number
  readonly workerId: string
  readonly metadata: Readonly<Record<string, string>>
  readonly signal: AbortSignal
}

export interface WorkerSnapshot {
  readonly name: string
  readonly id: string
  readonly state: 'running' | 'stopping' | 'stopped'
  readonly activeCount: number
}

export interface WorkerIdleOptions { readonly timeoutMs?: number; readonly signal?: AbortSignal }
export interface WorkerMonitor {
  workers(): ReadonlyArray<WorkerSnapshot>
  awaitIdle(options?: WorkerIdleOptions): Promise<void>
}
