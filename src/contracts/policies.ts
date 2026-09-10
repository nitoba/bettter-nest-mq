import { ContractDefinitionException } from './errors.ts'

interface BackoffBounds {
  readonly maxDelayMs?: number
  readonly jitter?: number
}

export type RetryBackoff =
  | ({ readonly type: 'fixed'; readonly delayMs: number } & BackoffBounds)
  | ({ readonly type: 'linear'; readonly initialDelayMs: number; readonly incrementMs: number } & BackoffBounds)
  | ({ readonly type: 'exponential'; readonly initialDelayMs: number; readonly factor: number } & BackoffBounds)
  | { readonly type: 'custom'; readonly policy: string; readonly version: number }

export interface RetryOptions {
  readonly attempts: number
  readonly backoff?: RetryBackoff | undefined
}

export interface JobPolicy {
  readonly retry?: RetryOptions | undefined
  readonly timeoutMs?: number | undefined
  readonly priority?: number | undefined
  readonly delayMs?: number | undefined
}

export interface ResolvedJobPolicy {
  readonly retry: RetryOptions
  readonly timeoutMs: number | undefined
  readonly priority: number
  readonly delayMs: number
}

export function requireName(value: string, label: string): void {
  if (value.length === 0 || value.trim() !== value) {
    throw new ContractDefinitionException(`${label} must be non-empty without surrounding whitespace`)
  }
}

export function requireInteger(value: number, label: string, minimum = 0): void {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new ContractDefinitionException(`${label} must be a safe integer greater than or equal to ${minimum}`)
  }
}

function copyBackoff(backoff: RetryBackoff): RetryBackoff {
  if (backoff.type === 'custom') {
    requireName(backoff.policy, 'retry.backoff.policy')
    requireInteger(backoff.version, 'retry.backoff.version', 1)
    return Object.freeze({ ...backoff })
  }

  if (backoff.jitter !== undefined && (!Number.isFinite(backoff.jitter) || backoff.jitter < 0 || backoff.jitter > 1)) {
    throw new ContractDefinitionException('retry.backoff.jitter must be between zero and one')
  }
  if (backoff.maxDelayMs !== undefined) requireInteger(backoff.maxDelayMs, 'retry.backoff.maxDelayMs')

  switch (backoff.type) {
    case 'fixed':
      requireInteger(backoff.delayMs, 'retry.backoff.delayMs')
      break
    case 'linear':
      requireInteger(backoff.initialDelayMs, 'retry.backoff.initialDelayMs')
      requireInteger(backoff.incrementMs, 'retry.backoff.incrementMs')
      break
    case 'exponential':
      requireInteger(backoff.initialDelayMs, 'retry.backoff.initialDelayMs')
      if (!Number.isFinite(backoff.factor) || backoff.factor < 1) {
        throw new ContractDefinitionException('retry.backoff.factor must be finite and at least one')
      }
      break
    default:
      throw new ContractDefinitionException('Unsupported retry backoff type')
  }
  return Object.freeze({ ...backoff })
}

export function copyRetry(retry: RetryOptions): RetryOptions {
  requireInteger(retry.attempts, 'retry.attempts', 1)
  return Object.freeze({
    ...retry,
    backoff: retry.backoff === undefined ? undefined : copyBackoff(retry.backoff)
  })
}

export function copyJobPolicy(policy: JobPolicy): JobPolicy {
  if (policy.timeoutMs !== undefined) requireInteger(policy.timeoutMs, 'timeoutMs')
  if (policy.delayMs !== undefined) requireInteger(policy.delayMs, 'delayMs')
  if (policy.priority !== undefined) requireInteger(policy.priority, 'priority')
  return Object.freeze({
    ...policy,
    retry: policy.retry === undefined ? undefined : copyRetry(policy.retry)
  })
}

/** Later layers override earlier layers. A retry policy is replaced as a complete unit. */
export function resolveJobPolicy(...layers: ReadonlyArray<JobPolicy>): ResolvedJobPolicy {
  let resolved: ResolvedJobPolicy = {
    retry: Object.freeze({ attempts: 1 }),
    timeoutMs: undefined,
    priority: 0,
    delayMs: 0
  }
  for (const layer of layers) {
    const policy = copyJobPolicy(layer)
    resolved = {
      retry: policy.retry ?? resolved.retry,
      timeoutMs: policy.timeoutMs ?? resolved.timeoutMs,
      priority: policy.priority ?? resolved.priority,
      delayMs: policy.delayMs ?? resolved.delayMs
    }
  }
  return Object.freeze(resolved)
}
