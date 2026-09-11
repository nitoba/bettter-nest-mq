import type { Type } from '@nestjs/common'
import type { QueueService } from '../contracts/queue-service.ts'

export interface QueueControlsOptions {
  readonly globalConcurrency?: number
  readonly perKeyConcurrency?: number
  readonly rateLimit?: { readonly max: number; readonly durationMs: number }
}

export interface MqControlsOptions {
  /** Replica startup validates only; policy deployment requires explicit reconciliation. */
  readonly mode?: 'validate' | 'reconcile'
  readonly group?: string
}

export interface QueueControlsSnapshot {
  readonly connection: string
  readonly queue: string
  readonly group: string
  readonly enabled: boolean
  readonly revision: number
  readonly globalConcurrency: number | undefined
  readonly perKeyConcurrency: number | undefined
  readonly rateLimit: { readonly max: number; readonly durationMs: number } | undefined
  readonly createdAtMs: number
  readonly updatedAtMs: number
}

export interface QueueControlsReport {
  readonly connection: string
  readonly created: readonly string[]
  readonly updated: readonly string[]
  readonly unchanged: readonly string[]
  readonly records: readonly QueueControlsSnapshot[]
}

export interface QueueControlsMonitor {
  get(queue: Type<QueueService>): Promise<QueueControlsSnapshot | undefined>
  reconcile(): Promise<readonly QueueControlsReport[]>
}
