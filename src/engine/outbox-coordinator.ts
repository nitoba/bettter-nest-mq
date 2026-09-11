import { makeOutboxId } from 'better-effect-mq-outbox'
import type { OutboxRecord, OutboxStore } from 'better-effect-mq-outbox'
import { Result } from 'better-result'
import type { MqRegistry } from '../module/mq.registry.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { MqOutboxService } from '../outbox/service.ts'
import type { OutboxCounts, OutboxEntry, OutboxListOptions, OutboxMonitor, OutboxPublisherSnapshot, OutboxSnapshot } from '../outbox/types.ts'
import type { EngineSession } from './engine-session.ts'
import { compileOutboxRecord, outboxSnapshot } from './outbox-record.ts'

export type PrepareOutboxEntry = (entry: OutboxEntry) => Promise<OutboxRecord>

export class OutboxCoordinator implements OutboxMonitor {
  constructor(private readonly session: EngineSession, private readonly registry: MqRegistry) {}

  withSource<Value>(source: string, operation: (store: OutboxStore, prepare: PrepareOutboxEntry) => Value | PromiseLike<Value>): Promise<Value> {
    // Keep already-admitted transactions independent of registry destruction during shutdown.
    const jobs = this.registry.jobs()
    return this.session.withOutbox(source, (store) => operation(store, (entry) => compileOutboxRecord(source, entry, jobs)))
  }
  async get(source: string, id: string): Promise<OutboxSnapshot | undefined> {
    const parsed = makeOutboxId(id)
    if (Result.isError(parsed)) throw new MqOutboxException('read', 'Invalid outbox id', { cause: parsed.error })
    return this.withSource(source, async (store) => {
      const result = await store.get(parsed.value)
      if (Result.isError(result)) throw new MqOutboxException('read', 'Unable to read the outbox record', { cause: result.error })
      return result.value === undefined ? undefined : outboxSnapshot(source, result.value)
    })
  }
  async list(source: string, options?: OutboxListOptions): Promise<readonly OutboxSnapshot[]> {
    return this.withSource(source, async (store) => {
      const result = await store.list(options)
      if (Result.isError(result)) throw new MqOutboxException('read', 'Unable to list outbox records', { cause: result.error })
      return Object.freeze(result.value.map((record) => outboxSnapshot(source, record)))
    })
  }
  async counts(source: string): Promise<OutboxCounts> {
    return this.withSource(source, async (store) => {
      const result = await store.counts()
      if (Result.isError(result)) throw new MqOutboxException('read', 'Unable to count outbox records', { cause: result.error })
      return Object.freeze({ ...result.value })
    })
  }
  publisher(): OutboxPublisherSnapshot | undefined { return this.session.outboxPublisher() }
}

// Per-service bindings keep internal native access out of the public declarations. Each
// coordinator still delegates admission/lifetime to its own application runtime.
const coordinators = new WeakMap<MqOutboxService, OutboxCoordinator>()
export function bindOutboxService(service: MqOutboxService, coordinator: OutboxCoordinator): MqOutboxService {
  coordinators.set(service, coordinator)
  return service
}
export function outboxCoordinator(service: MqOutboxService): OutboxCoordinator {
  const coordinator = coordinators.get(service)
  if (coordinator === undefined) throw new MqOutboxException('unavailable', 'The outbox service does not belong to a configured application')
  return coordinator
}
