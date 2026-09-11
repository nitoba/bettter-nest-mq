import type { Type } from '@nestjs/common'
import {
  Queue as EngineQueue,
  QueueControls as EngineControls,
  makeQueueName
} from 'better-effect-mq'
import type { QueueControlsRecord, JobStoreContract } from 'better-effect-mq'
import { Result } from 'better-result'
import { isDeepStrictEqual } from 'node:util'
import { QueueControlsException } from '../controls/errors.ts'
import type {
  MqControlsOptions,
  QueueControlsMonitor,
  QueueControlsOptions,
  QueueControlsReport,
  QueueControlsSnapshot
} from '../controls/types.ts'
import { getOwnQueueMetadata } from '../contracts/decorators.ts'
import type { QueueDefinition } from '../contracts/queue-definition.ts'
import type { QueueService } from '../contracts/queue-service.ts'
import type { MqRegistry } from '../module/mq.registry.ts'
import type { EngineSession } from './engine-session.ts'
import { requireControlledStore } from './controlled-store.ts'

function snapshot(connection: string, record: QueueControlsRecord): QueueControlsSnapshot {
  return Object.freeze({
    ...record,
    connection,
    rateLimit: record.rateLimit === undefined ? undefined : Object.freeze({ ...record.rateLimit })
  })
}

function assertCapabilities(store: JobStoreContract, options: QueueControlsOptions): void {
  requireControlledStore(store)
  if (
    (options.globalConcurrency !== undefined || options.perKeyConcurrency !== undefined) &&
    !store.descriptor.capabilities.globalConcurrency
  )
    throw new QueueControlsException(
      'capability',
      'The adapter does not support distributed concurrency'
    )
  if (options.rateLimit !== undefined && !store.descriptor.capabilities.rateLimiting)
    throw new QueueControlsException(
      'capability',
      'The adapter does not support distributed rate limiting'
    )
}

function samePolicy(record: QueueControlsRecord, desired: QueueControlsOptions): boolean {
  return (
    record.enabled &&
    record.globalConcurrency === desired.globalConcurrency &&
    record.perKeyConcurrency === desired.perKeyConcurrency &&
    isDeepStrictEqual(record.rateLimit, desired.rateLimit)
  )
}

export class QueueControlsCoordinator implements QueueControlsMonitor {
  constructor(
    private readonly session: EngineSession,
    private readonly registry: MqRegistry,
    private readonly options: Readonly<Required<MqControlsOptions>>
  ) {}

  async initialize(): Promise<void> {
    await this.synchronize(this.options.mode)
  }

  async get(queue: Type<QueueService>): Promise<QueueControlsSnapshot | undefined> {
    const metadata = getOwnQueueMetadata(queue)
    const definition =
      metadata === undefined
        ? undefined
        : this.registry
            .queues()
            .find(
              (entry) => entry.name === metadata.name && entry.connection === metadata.connection
            )
    if (definition === undefined)
      throw new QueueControlsException(
        'configuration',
        'Queue controls can only be read for a registered queue identity'
      )
    return this.session.withStore(definition.connection, async (store) => {
      const extension = requireControlledStore(store)
      const name = makeQueueName(definition.name)
      if (Result.isError(name))
        throw new QueueControlsException('configuration', 'Invalid queue identity', {
          cause: name.error
        })
      const result = await extension.getControls({ queue: name.value })
      if (Result.isError(result))
        throw new QueueControlsException('operation', 'Unable to read persisted queue controls', {
          cause: result.error
        })
      return result.value === undefined ? undefined : snapshot(definition.connection, result.value)
    })
  }

  async reconcile(): Promise<readonly QueueControlsReport[]> {
    if (this.options.mode !== 'reconcile')
      throw new QueueControlsException(
        'mode',
        'This application is configured for read-only policy validation'
      )
    return this.synchronize('reconcile')
  }

  private async synchronize(
    mode: 'validate' | 'reconcile'
  ): Promise<readonly QueueControlsReport[]> {
    const queues = this.registry.queues().filter((queue) => queue.controls !== undefined)
    const groups = new Map<string, QueueDefinition[]>()
    // Finish every capability and ownership check before the first persisted policy write.
    for (const queue of queues) {
      const desired = queue.controls
      if (desired === undefined) continue
      await this.session.withStore(queue.connection, async (store) => {
        assertCapabilities(store, desired)
        const extension = requireControlledStore(store)
        const name = makeQueueName(queue.name)
        if (Result.isError(name))
          throw new QueueControlsException('configuration', 'Invalid queue identity', {
            cause: name.error
          })
        const result = await extension.getControls({ queue: name.value })
        if (Result.isError(result))
          throw new QueueControlsException(
            'operation',
            'Unable to read queue policy during startup',
            { cause: result.error }
          )
        const record = result.value
        if (record !== undefined && record.group !== this.options.group)
          throw new QueueControlsException(
            'ownership',
            `Queue ${queue.name} belongs to another policy group`
          )
        if (mode === 'validate') {
          if (record === undefined)
            throw new QueueControlsException(
              'missing',
              `Queue ${queue.name} has no persisted policy; deploy controls explicitly first`
            )
          if (!samePolicy(record, desired))
            throw new QueueControlsException(
              'drift',
              `Queue ${queue.name} differs from its persisted policy`
            )
        }
      })
      const group = groups.get(queue.connection) ?? []
      group.push(queue)
      groups.set(queue.connection, group)
    }
    if (mode === 'validate') return Object.freeze([])
    const reports: QueueControlsReport[] = []
    for (const [connection, group] of groups) {
      const report = await this.session.withStore(connection, async (store) => {
        const definitions = group.map((queue) =>
          EngineControls.define(EngineQueue.define(queue.name), queue.controls ?? {})
        )
        const result = await requireControlledStore(store).reconcile(
          EngineControls.registry({ group: this.options.group, controls: definitions }),
          { removal: 'ignore' }
        )
        if (Result.isError(result))
          throw new QueueControlsException('operation', 'Queue policy reconciliation failed', {
            cause: result.error
          })
        return Object.freeze({
          connection,
          created: Object.freeze(result.value.created.map((record) => record.queue)),
          updated: Object.freeze(result.value.updated.map((record) => record.queue)),
          unchanged: Object.freeze(result.value.unchanged.map((record) => record.queue)),
          records: Object.freeze(result.value.records.map((record) => snapshot(connection, record)))
        })
      })
      reports.push(report)
    }
    return Object.freeze(reports)
  }
}
