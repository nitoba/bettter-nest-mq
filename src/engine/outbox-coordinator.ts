import { makeOutboxId } from 'better-effect-mq-outbox'
import type { OutboxRecord, OutboxStore } from 'better-effect-mq-outbox'
import { Result } from 'better-result'
import { isDeepStrictEqual } from 'node:util'
import type { MqRegistry } from '../module/mq.registry.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { MqOutboxService } from '../outbox/service.ts'
import { copyOutboxRetryOptions } from '../outbox/retry-options.ts'
import type { OutboxRetryOptions } from '../outbox/retry-options.ts'
import type {
  OutboxCounts,
  OutboxEntry,
  OutboxListOptions,
  OutboxMonitor,
  OutboxPublisherSnapshot,
  OutboxSnapshot
} from '../outbox/types.ts'
import type { EngineSession } from './engine-session.ts'
import { compileOutboxRecord, outboxSnapshot } from './outbox-record.ts'
import { outboxRetryAdapter, retryOutboxRecord } from './outbox-retry.ts'

export type PrepareOutboxEntry = (entry: OutboxEntry) => Promise<OutboxRecord>
export class OutboxCoordinator implements OutboxMonitor {
  constructor(
    private readonly session: EngineSession,
    private readonly registry: MqRegistry
  ) {}
  withSource<Value>(
    source: string,
    operation: (store: OutboxStore, prepare: PrepareOutboxEntry) => Value | PromiseLike<Value>
  ): Promise<Value> {
    // Capture the admitted registry so destruction cannot change a transaction's contracts.
    const jobs = this.registry.jobs()
    return this.session.withOutbox(source, (store) =>
      operation(store, (entry) => compileOutboxRecord(source, entry, jobs))
    )
  }
  async retryFailed(
    source: string,
    id: string,
    options: OutboxRetryOptions
  ): Promise<OutboxSnapshot> {
    const copied = copyOutboxRetryOptions(options)
    const parsed = makeOutboxId(id)
    if (Result.isError(parsed))
      throw new MqOutboxException('retry', 'Invalid outbox id', { cause: parsed.error })
    return this.withSource(source, async (store, prepare) => {
      const adapter = outboxRetryAdapter(store)
      const result = await store.get(parsed.value)
      if (Result.isError(result))
        throw new MqOutboxException('read', 'Unable to inspect the failed publication', {
          cause: result.error
        })
      const record = result.value
      if (record === undefined)
        throw new MqOutboxException('retry', 'The outbox record does not exist')
      // Check the optimistic guard and bounds before asynchronous schema validation.
      retryOutboxRecord(record, copied, Date.now())
      const validated = await prepare({
        id: record.id,
        job: { connection: record.target, request: record.request },
        attempts: record.attemptsMax
      })
      if (!isDeepStrictEqual(validated.request, record.request))
        throw new MqOutboxException('retry', 'Retry cannot change the original prepared request')
      const next = retryOutboxRecord(record, copied, Date.now())
      await adapter.retry(record, next)
      return outboxSnapshot(source, next)
    })
  }
  async get(source: string, id: string): Promise<OutboxSnapshot | undefined> {
    const parsed = makeOutboxId(id)
    if (Result.isError(parsed))
      throw new MqOutboxException('read', 'Invalid outbox id', { cause: parsed.error })
    return this.withSource(source, async (store) => {
      const result = await store.get(parsed.value)
      if (Result.isError(result))
        throw new MqOutboxException('read', 'Unable to read the outbox record', {
          cause: result.error
        })
      return result.value === undefined ? undefined : outboxSnapshot(source, result.value)
    })
  }
  async list(source: string, options?: OutboxListOptions): Promise<readonly OutboxSnapshot[]> {
    return this.withSource(source, async (store) => {
      const result = await store.list(options)
      if (Result.isError(result))
        throw new MqOutboxException('read', 'Unable to list outbox records', {
          cause: result.error
        })
      return Object.freeze(result.value.map((record) => outboxSnapshot(source, record)))
    })
  }
  async counts(source: string): Promise<OutboxCounts> {
    return this.withSource(source, async (store) => {
      const result = await store.counts()
      if (Result.isError(result))
        throw new MqOutboxException('read', 'Unable to count outbox records', {
          cause: result.error
        })
      return Object.freeze({ ...result.value })
    })
  }
  publisher(): OutboxPublisherSnapshot | undefined {
    return this.session.outboxPublisher()
  }
}
const coordinators = new WeakMap<MqOutboxService, OutboxCoordinator>()
export function bindOutboxService(
  service: MqOutboxService,
  coordinator: OutboxCoordinator
): MqOutboxService {
  coordinators.set(service, coordinator)
  return service
}
export function outboxCoordinator(service: MqOutboxService): OutboxCoordinator {
  const coordinator = coordinators.get(service)
  if (coordinator === undefined)
    throw new MqOutboxException(
      'unavailable',
      'The outbox service does not belong to a configured application'
    )
  return coordinator
}
