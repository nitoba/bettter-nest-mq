import { Injectable } from '@nestjs/common'
import type {
  OutboxCounts,
  OutboxListOptions,
  OutboxMonitor,
  OutboxPublisherSnapshot,
  OutboxSnapshot
} from './types.ts'

/** The module supplies this facade through a factory; application code injects it normally. */
@Injectable()
export class MqOutboxService implements OutboxMonitor {
  constructor(private readonly monitor: OutboxMonitor) {}
  get(source: string, id: string): Promise<OutboxSnapshot | undefined> {
    return this.monitor.get(source, id)
  }
  list(source: string, options?: OutboxListOptions): Promise<readonly OutboxSnapshot[]> {
    return this.monitor.list(source, options)
  }
  counts(source: string): Promise<OutboxCounts> {
    return this.monitor.counts(source)
  }
  publisher(): OutboxPublisherSnapshot | undefined {
    return this.monitor.publisher()
  }
}
