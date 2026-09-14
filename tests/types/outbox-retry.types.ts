import type { MqOutboxService, OutboxRetryOptions, OutboxSnapshot } from '../../src/index.ts'

export const retry: OutboxRetryOptions = {
  expected: { updatedAtMs: 200, attemptsMade: 2, attemptsMax: 2 },
  attempts: 3
}
// @ts-expect-error Recovery requires an inspected version rather than a blind reset.
export const unguarded: OutboxRetryOptions = { attempts: 3 }
// @ts-expect-error A new explicit publication budget is required.
export const implicit: OutboxRetryOptions = { expected: retry.expected }
// @ts-expect-error Payload replacement is not an administrative retry.
export const rewrite: OutboxRetryOptions = { ...retry, payload: {} }
export async function recover(service: MqOutboxService): Promise<OutboxSnapshot> {
  return service.retryFailed('primary', 'publication', retry)
}
