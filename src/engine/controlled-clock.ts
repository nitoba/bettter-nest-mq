import { JobDefinitionError } from 'better-effect-mq'
import type { JobId, JobStoreContract, JobStoreError } from 'better-effect-mq'
import { Result } from 'better-result'

/** A heartbeat may commit after the supervisor sampled its mutation time. The upstream
 * reducer rejects that mutation before applying it. Refresh only this explicit rejection:
 * never retry an ambiguous write, a lease failure, or the user's handler here. */
export async function withFreshControlledClock<Value>(
  store: JobStoreContract,
  jobId: JobId,
  sampledNow: number,
  mutate: (now: number) => PromiseLike<Result<Value, JobStoreError>>
): Promise<Result<Value, JobStoreError>> {
  let now = sampledNow
  let result = await mutate(now)
  for (let refresh = 0; refresh < 3 && Result.isError(result); refresh += 1) {
    const error = result.error
    if (
      !(error instanceof JobDefinitionError) ||
      error.field !== 'now' ||
      error.message !== 'must not be earlier than updatedAt'
    ) return result

    const current = await store.getJob({ jobId })
    if (Result.isError(current)) return Result.err(current.error)
    if (current.value === undefined) return result
    const refreshedNow = Math.max(now, current.value.updatedAt, Date.now())
    if (refreshedNow <= now) return result
    now = refreshedNow
    // mutate retains the original lease token. Every replay rechecks that token and its
    // expiration inside the store; refreshing a timestamp never extends the lease.
    result = await mutate(now)
  }
  return result
}
