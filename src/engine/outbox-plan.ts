import { Logger } from '@nestjs/common'
import { Layer } from 'better-effect'
import { OutboxPublisher, OutboxRoutes, OutboxStore } from 'better-effect-mq-outbox'
import type { OutboxStoreInstance, OutboxStoreToken, OutboxPublisherServiceInstance } from 'better-effect-mq-outbox'
import type { MqOutboxOptions } from '../outbox/types.ts'
import { operationStoreToken } from './operation-store.ts'

export type NamedOutbox = OutboxStoreInstance<`nestjs/${string}`>
export type NamedOutboxToken = OutboxStoreToken<`nestjs/${string}`>
export type EngineOutboxPublisher = OutboxPublisherServiceInstance<'nestjs/outbox-publisher'>
export type OutboxLayerFactory = (name: string) => Layer<NamedOutbox, never>
export const Publisher = OutboxPublisher.service('nestjs/outbox-publisher')

export function outboxToken(name: string): NamedOutboxToken {
  const tag: `nestjs/${string}` = `nestjs/${name}`
  return OutboxStore.named(tag)
}

export function outboxPublisherLayer(sources: readonly string[], targets: readonly string[], options: MqOutboxOptions) {
  const logger = new Logger('BetterNestMqOutbox')
  const routes = OutboxRoutes.make(Object.fromEntries(targets.map((name) => [name, operationStoreToken(name)])))
  return Publisher.layer(() => ({
    ...options,
    outboxes: sources.map(outboxToken),
    routes,
    onError: () => { logger.warn('An outbox publisher operation failed; inspect persisted records and connection health') }
  }))
}
