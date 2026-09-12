import { requireInteger } from '../contracts/policies.ts'
import { MqJobException } from '../jobs/errors.ts'
import type { JobWaitOptions } from '../jobs/types.ts'

/** Validate before allocating timers or entering the engine so JavaScript callers
 * receive facade errors and oversized delays cannot collapse into a busy loop. */
export function validateWaitOptions(options: JobWaitOptions): void {
  if (
    options.strategy !== undefined &&
    options.strategy !== 'polling' &&
    options.strategy !== 'events'
  ) {
    throw new MqJobException('awaitResult', 'Wait strategy must be polling or events')
  }
  if (options.strategy === 'events' && options.pollIntervalMs !== undefined) {
    throw new MqJobException('awaitResult', 'Event waits use pollFallbackMs, not pollIntervalMs')
  }
  if (options.strategy !== 'events' && options.pollFallbackMs !== undefined) {
    throw new MqJobException('awaitResult', 'pollFallbackMs requires the events strategy')
  }
  for (const [name, value] of Object.entries({
    timeoutMs: options.timeoutMs,
    pollIntervalMs: options.pollIntervalMs,
    pollFallbackMs: options.pollFallbackMs
  })) {
    if (value === undefined) continue
    requireInteger(value, `wait.${name}`, name === 'timeoutMs' ? 0 : 1)
    if (value > 2_147_483_647)
      throw new RangeError(`wait.${name} exceeds the supported timer range`)
  }
}
