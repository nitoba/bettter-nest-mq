import type { Effect } from 'better-effect'
import type { AttemptRecord, AttemptRecordV2, CountsRequest, FlowStoreV2, JobCountsV2, JobDefinitionError, JobId, JobRecord, JobRecordV2, JobStoreContract, JobStoreFailure } from 'better-effect-mq'
import { JobStoreFailure as StoreFailure } from 'better-effect-mq'
import { Result } from 'better-result'

export type FlowReadError = JobDefinitionError | JobStoreFailure
export interface FlowJobReads {
  getJob(id: JobId): Promise<Result<JobRecordV2 | undefined, FlowReadError>>
  getAttempts(id: JobId): Promise<Result<readonly AttemptRecordV2[], FlowReadError>>
  counts(request?: CountsRequest): Promise<Result<JobCountsV2, FlowReadError>>
  recoveryIds(flowNames: readonly string[]): Promise<readonly JobId[]>
}

/** A read-only v2 projection for the pinned v1-annotated engine. No state is rewritten,
 * and mutations/leases remain implemented by the actual upstream stores. */
export function flowReadStore(store: JobStoreContract, reads: FlowJobReads | undefined, flow: () => FlowStoreV2 | undefined): JobStoreContract {
  if (reads === undefined) return store
  const view: Pick<JobStoreContract, 'getJob' | 'getAttempts' | 'counts' | 'cancel'> = {
    async getJob(request) {
      const value = await reads.getJob(request.jobId)
      // SAFETY: the upstream read/observation paths accept v2 state strings at runtime.
      // The projection validates with validateJobRecordV2 and preserves the original state;
      // the facade's public state union includes waiting-children. No reducer receives it.
      return value as Effect<JobRecord | undefined, FlowReadError>
    },
    async getAttempts(request) {
      const value = await reads.getAttempts(request.jobId)
      // SAFETY: v2 adds only the validated fanned-out ledger outcome. Public attempt views
      // include that outcome; the pinned reader otherwise treats these records as data.
      return value as Effect<readonly AttemptRecord[], FlowReadError>
    },
    async counts(request) {
      const value = await reads.counts(request)
      // SAFETY: JobCountsV2 extends the v1 count object, and Result is Effect's runtime value.
      return value as Effect<JobCountsV2, FlowReadError>
    },
    async cancel(request) {
      const record = await reads.getJob(request.jobId)
      if (Result.isError(record)) {
        // SAFETY: a completed error Result requires no runtime services.
        return record as Effect<never, FlowReadError>
      }
      if (record.value?.state !== 'waiting-children') return store.cancel(request)
      const target = flow()
      if (target === undefined) {
        // SAFETY: the completed error reports absent acquisition, never a simulated cancellation.
        return Result.err(new StoreFailure({ operation: 'cancelFlow', message: 'The flow store is not ready', retryable: false })) as Effect<never, JobStoreFailure>
      }
      const cancelled = await target.cancel({ flowId: request.jobId, now: request.now })
      if (Result.isError(cancelled)) {
        // SAFETY: FlowStore errors are the same validated protocol error family used by jobs.
        return cancelled as Effect<never, FlowReadError>
      }
      const updated = await reads.getJob(request.jobId)
      if (Result.isError(updated) || updated.value === undefined || updated.value.state === 'waiting-children') {
        // SAFETY: successful cancellation must be observable as a terminal parent record.
        return Result.err(new StoreFailure({ operation: 'cancelFlow', message: 'Cancelled parent could not be read', retryable: false })) as Effect<never, JobStoreFailure>
      }
      // SAFETY: this record was validated as v2 and is no longer in its additional suspended state.
      return Result.ok(updated.value) as Effect<JobRecord, never>
    }
  }
  return new Proxy(store, {
    get(target, key) {
      if (Object.hasOwn(view, key)) return Reflect.get(view, key)
      const value = Reflect.get(target, key, target)
      return value instanceof Function ? value.bind(target) : value
    }
  })
}
