import { ContractDefinitionException } from '../contracts/errors.ts'
import { copyRetry } from '../contracts/policies.ts'
import type { RetryOptions } from '../contracts/policies.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'

const RETRY_REFERENCE = '__better_nest_mq_retry'

export function retryReference(job: RegisteredJob): string | undefined {
  const backoff = job.policy.retry.backoff
  return backoff?.type === 'custom' ? JSON.stringify([backoff.policy, backoff.version]) : undefined
}

/** A custom decision belongs to the versioned contract, not a per-message executable override. */
export function publicationRetry(job: RegisteredJob, override?: RetryOptions): RetryOptions {
  const retry = copyRetry(override ?? job.policy.retry)
  const supplied = retry.backoff
  const expected = retryReference(job)
  const actual =
    supplied?.type === 'custom' ? JSON.stringify([supplied.policy, supplied.version]) : undefined
  if (actual !== expected)
    throw new ContractDefinitionException(
      'Custom retry overrides must retain the declared policy name and version'
    )
  return retry
}

export function publicationMetadata(
  job: RegisteredJob,
  metadata: Readonly<Record<string, string>> = {}
): Readonly<Record<string, string>> {
  if (
    metadata === null ||
    Object(metadata) !== metadata ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(metadata))
  )
    throw new ContractDefinitionException('Job metadata must be a data object')
  for (const key of Reflect.ownKeys(metadata)) {
    const descriptor = Object.getOwnPropertyDescriptor(metadata, key)
    if (
      key !== String(key) ||
      descriptor === undefined ||
      !('value' in descriptor) ||
      Object(descriptor.value) === descriptor.value ||
      descriptor.value !== String(descriptor.value)
    )
      throw new ContractDefinitionException('Job metadata must contain only own string data fields')
  }
  if (Object.hasOwn(metadata, RETRY_REFERENCE))
    throw new ContractDefinitionException(
      'Job metadata cannot override the internal retry reference'
    )
  const reference = retryReference(job)
  return Object.freeze(
    reference === undefined ? { ...metadata } : { ...metadata, [RETRY_REFERENCE]: reference }
  )
}

/** Fail closed before user code when a rolling deployment no longer agrees with a saved policy. */
export function assertRetryReference(
  job: RegisteredJob,
  metadata: Readonly<Record<string, string>>
): void {
  if (metadata[RETRY_REFERENCE] !== retryReference(job))
    throw new ContractDefinitionException(
      'Persisted retry policy differs from this job contract; retain the original versioned contract'
    )
}
