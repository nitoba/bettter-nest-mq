import { MqOutboxException } from './errors.ts'

/** A version guard from the last inspected publication, not a lease or an authorization token. */
export interface OutboxRetryExpected {
  readonly updatedAtMs: number
  readonly attemptsMade: number
  readonly attemptsMax: number
}
export interface OutboxRetryOptions {
  readonly expected: OutboxRetryExpected
  /** Publication attempts available after recovery; does not change job execution attempts. */
  readonly attempts: number
  /** Absolute publication eligibility; defaults to the accepted retry time. */
  readonly runAtMs?: number
}

function fields<Options extends object>(options: Options, allowed: readonly string[]): void {
  if (options === null || options === undefined) throw new MqOutboxException('retry', 'Retry options are required')
  const prototype = Object.getPrototypeOf(options)
  if (prototype !== Object.prototype && prototype !== null) throw new MqOutboxException('retry', 'Retry options must be plain data')
  for (const key of Reflect.ownKeys(options)) {
    if (key !== String(key) || !allowed.includes(String(key))) throw new MqOutboxException('retry', `Unsupported retry option ${String(key)}`)
    const descriptor = Object.getOwnPropertyDescriptor(options, key)
    if (descriptor?.get !== undefined || descriptor?.set !== undefined) throw new MqOutboxException('retry', 'Retry option accessors are not supported')
  }
}
function integer(value: number, field: string, minimum: number): void {
  if (!Number.isSafeInteger(value) || value < minimum) throw new MqOutboxException('retry', `${field} must be a safe integer >= ${minimum}`)
}
export function copyOutboxRetryOptions(options: OutboxRetryOptions): OutboxRetryOptions {
  fields(options, ['expected', 'attempts', 'runAtMs'])
  fields(options.expected, ['updatedAtMs', 'attemptsMade', 'attemptsMax'])
  integer(options.attempts, 'attempts', 1)
  integer(options.expected.updatedAtMs, 'expected.updatedAtMs', 0)
  integer(options.expected.attemptsMade, 'expected.attemptsMade', 0)
  integer(options.expected.attemptsMax, 'expected.attemptsMax', 1)
  if (options.expected.attemptsMade > options.expected.attemptsMax) throw new MqOutboxException('retry', 'Expected attemptsMade exceeds attemptsMax')
  if (options.runAtMs !== undefined) integer(options.runAtMs, 'runAtMs', 0)
  const copied = { expected: Object.freeze({ ...options.expected }), attempts: options.attempts }
  return Object.freeze(options.runAtMs === undefined ? copied : { ...copied, runAtMs: options.runAtMs })
}
