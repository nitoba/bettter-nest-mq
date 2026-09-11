import { MqOutboxException } from './errors.ts'
import type { MqOutboxOptions } from './types.ts'

export function resolveOutboxOptions(options: MqOutboxOptions = {}): Readonly<Required<MqOutboxOptions>> {
  const leaseDurationMs = options.leaseDurationMs ?? 30_000
  const resolved = {
    concurrency: options.concurrency ?? 1,
    leaseDurationMs,
    heartbeatIntervalMs: options.heartbeatIntervalMs ?? Math.max(1, Math.floor(leaseDurationMs / 3)),
    pollIntervalMs: options.pollIntervalMs ?? 100,
    retryBaseDelayMs: options.retryBaseDelayMs ?? 1_000,
    retryMaxDelayMs: options.retryMaxDelayMs ?? 60_000
  }
  for (const [field, value] of Object.entries(resolved)) {
    if (!Number.isSafeInteger(value) || value < 1 || value > 2_147_483_647) throw new MqOutboxException('configuration', `outbox.${field} must be a positive integer within the supported timer range`)
  }
  if (resolved.heartbeatIntervalMs >= resolved.leaseDurationMs) throw new MqOutboxException('configuration', 'Outbox heartbeat must be shorter than its lease')
  if (resolved.retryMaxDelayMs < resolved.retryBaseDelayMs) throw new MqOutboxException('configuration', 'Outbox maximum retry delay cannot be below its base delay')
  return Object.freeze(resolved)
}
