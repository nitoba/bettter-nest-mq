import { expect, test } from 'bun:test'
import { MqOutboxService } from '../../src/index.ts'

test('exposes explicit failed-publication retry through the Nest outbox Service', () => {
  expect(Object.getOwnPropertyNames(MqOutboxService.prototype)).toContain('retryFailed')
})
