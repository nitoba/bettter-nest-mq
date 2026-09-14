import { validateOutboxRecord } from 'better-effect-mq-outbox'
import type { OutboxRecord, OutboxStore } from 'better-effect-mq-outbox'
import { Result } from 'better-result'
import { MqOutboxException } from '../outbox/errors.ts'
import type { OutboxRetryOptions } from '../outbox/retry-options.ts'

export function retryOutboxRecord(record: OutboxRecord, options: OutboxRetryOptions, nowMs: number): OutboxRecord {
  if (record.state !== 'failed') throw new MqOutboxException('retry', 'Only failed publications can be retried')
  const expected = options.expected
  if (record.updatedAtMs !== expected.updatedAtMs || record.attemptsMade !== expected.attemptsMade || record.attemptsMax !== expected.attemptsMax) {
    throw new MqOutboxException('conflict', 'The outbox record changed after it was inspected')
  }
  if (record.request.id === undefined) throw new MqOutboxException('retry', 'Retry requires the original stable job id')
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) throw new MqOutboxException('retry', 'Invalid retry timestamp')
  const updatedAtMs = Math.max(nowMs, record.updatedAtMs + 1)
  const attemptsMax = record.attemptsMade + options.attempts
  if (!Number.isSafeInteger(updatedAtMs) || !Number.isSafeInteger(attemptsMax)) throw new MqOutboxException('retry', 'Retry would overflow the supported timestamp or attempt range')
  const next = validateOutboxRecord({
    ...record,
    state: 'pending',
    attemptsMax,
    updatedAtMs,
    runAtMs: Math.max(options.runAtMs ?? updatedAtMs, updatedAtMs)
  })
  if (Result.isError(next)) throw new MqOutboxException('retry', 'The failed publication cannot be requeued safely', { cause: next.error })
  return next.value
}

export type OutboxRetryWrite = (previous: OutboxRecord, next: OutboxRecord) => Promise<void>

/** An optional adapter capability. It is bound to an acquired store, not a process-wide runtime. */
export class OutboxRetryAdapter {
  private closed = false
  private readonly pending = new Set<Promise<void>>()
  constructor(private readonly write: OutboxRetryWrite) {}
  retry(previous: OutboxRecord, next: OutboxRecord): Promise<void> {
    if (this.closed) return Promise.reject(new MqOutboxException('unavailable', 'Outbox recovery is closed'))
    const operation = Promise.resolve().then(() => this.write(previous, next))
    const drained = operation.then(() => undefined, () => undefined)
    this.pending.add(drained)
    void drained.then(() => { this.pending.delete(drained) })
    return operation
  }
  async close(): Promise<void> {
    this.closed = true
    await Promise.all(this.pending)
  }
}
const adapters = new WeakMap<OutboxStore, OutboxRetryAdapter>()
export function bindOutboxRetry(store: OutboxStore, write: OutboxRetryWrite): OutboxRetryAdapter {
  if (adapters.has(store)) throw new MqOutboxException('configuration', 'Outbox retry capability already bound')
  const adapter = new OutboxRetryAdapter(write)
  adapters.set(store, adapter)
  return adapter
}
export function outboxRetryAdapter(store: OutboxStore): OutboxRetryAdapter {
  const adapter = adapters.get(store)
  if (adapter === undefined) throw new MqOutboxException('unavailable', 'This outbox adapter does not support administrative retry')
  return adapter
}
export async function closeOutboxRetry(store: OutboxStore): Promise<void> {
  const adapter = adapters.get(store)
  adapters.delete(store)
  await adapter?.close()
}
