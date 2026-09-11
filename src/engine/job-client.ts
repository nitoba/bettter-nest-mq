import { publicationRetry, publicationMetadata } from './retry-reference.ts'
import type {
  JobAttemptView,
  JobEnqueueOptions as EngineEnqueueOptions,
  JobRecordView,
  DecodedJobFailure
} from 'better-effect-mq'
import { JobExecutionCancelledError, JobNotCancellableError } from 'better-effect-mq'
import { Result } from 'better-result'

import { validateDispatchKey } from '../controls/decorator.ts'
import { ContractDefinitionException, JobFailureException } from '../contracts/errors.ts'
import { requireInteger } from '../contracts/policies.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import type { JobClient } from '../jobs/binding.ts'
import {
  JobCancelledException,
  JobWaitAbortedException,
  JobWaitTimeoutException,
  MqJobException
} from '../jobs/errors.ts'
import type { JobOperationName } from '../jobs/errors.ts'
import type {
  JobAttempt,
  JobEnqueueOptions,
  JobFailureView,
  JobSnapshot,
  JobWaitOptions
} from '../jobs/types.ts'
import { compileBackoff } from './job-compiler.ts'
import type { CompiledJob, ContractValue, DomainFailure } from './job-compiler.ts'
import type { EngineSession } from './engine-session.ts'

function enqueueOptions(
  registered: RegisteredJob,
  options: JobEnqueueOptions = {}
): EngineEnqueueOptions {
  if (options.dispatchKey !== undefined) validateDispatchKey(options.dispatchKey)
  if (registered.controls?.perKeyConcurrency !== undefined && options.dispatchKey === undefined)
    throw new ContractDefinitionException(
      'A per-key-limited queue requires a dispatchKey on every new job'
    )
  const { policy } = registered
  const retry = publicationRetry(registered, options.retry)
  const fields = {
    priority: options.priority ?? policy.priority,
    attempts: retry.attempts
  }
  const timeoutMs = options.timeoutMs ?? policy.timeoutMs
  if (timeoutMs !== undefined) requireInteger(timeoutMs, 'timeoutMs', 1)
  let result: EngineEnqueueOptions = {
    ...fields,
    metadata: publicationMetadata(registered, options.metadata)
  }
  const backoff = compileBackoff(retry)
  if (backoff !== undefined) result = { ...result, backoff }
  if (timeoutMs !== undefined) result = { ...result, timeoutMs }
  if (options.jobId !== undefined) result = { ...result, jobId: options.jobId }
  if (options.idempotencyKey !== undefined)
    result = { ...result, idempotencyKey: options.idempotencyKey }
  if (options.dispatchKey !== undefined) result = { ...result, dispatchKey: options.dispatchKey }
  if (options.at !== undefined) {
    if (options.delayMs !== undefined)
      throw new MqJobException('enqueue', 'delayMs and at cannot be combined')
    return { ...result, at: options.at }
  }
  return { ...result, delayMs: options.delayMs ?? policy.delayMs }
}

function failureView(
  failure: DecodedJobFailure<DomainFailure> | undefined
): JobFailureView<ContractValue> | undefined {
  if (failure === undefined) return undefined
  if (failure.kind === 'typed') return { ...failure, data: failure.data.failure }
  return failure
}

function snapshot(
  record: JobRecordView<ContractValue, DomainFailure>
): JobSnapshot<ContractValue, ContractValue> {
  return Object.freeze({
    id: record.id,
    queue: record.queue,
    name: record.name,
    version: record.version,
    state: record.state,
    payload: record.payload,
    metadata: record.metadata,
    priority: record.priority,
    runAt: record.runAt,
    attemptsMax: record.attemptsMax,
    attemptsMade: record.attemptsMade,
    deliveryCount: record.deliveryCount,
    stalledCount: record.stalledCount,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
    processedAt: record.processedAt,
    finishedAt: record.finishedAt,
    cancellationRequestedAt: record.cancellationRequestedAt,
    result: record.result,
    failure: failureView(record.failure)
  })
}

function attemptView(
  attempt: JobAttemptView<ContractValue, DomainFailure>
): JobAttempt<ContractValue, ContractValue> {
  return Object.freeze({ ...attempt, failure: failureView(attempt.failure) })
}

function unwrap<Value, Failure>(
  result: Result<Value, Failure>,
  operation: JobOperationName
): Value {
  if (Result.isOk(result)) return result.value
  if (result.error instanceof JobFailureException) throw result.error
  if (result.error instanceof JobExecutionCancelledError)
    throw new JobCancelledException(result.error.jobId, { cause: result.error })
  throw new MqJobException(operation, `The ${operation} operation could not complete`, {
    cause: result.error
  })
}

async function boundedWait(
  session: EngineSession,
  job: CompiledJob,
  id: string,
  options: JobWaitOptions = {}
): Promise<ContractValue> {
  if (options.strategy !== undefined && options.strategy !== 'polling')
    throw new MqJobException('awaitResult', 'Only polling waits are supported by this integration')
  if (options.timeoutMs !== undefined) requireInteger(options.timeoutMs, 'wait.timeoutMs')
  if (options.pollIntervalMs !== undefined)
    requireInteger(options.pollIntervalMs, 'wait.pollIntervalMs', 1)
  if (options.timeoutMs !== undefined && options.timeoutMs > 2_147_483_647)
    throw new RangeError('wait.timeoutMs exceeds the supported timer range')
  if (options.signal?.aborted)
    throw new JobWaitAbortedException(id, { cause: options.signal.reason })
  const timeout = new AbortController()
  const signal =
    options.signal === undefined
      ? timeout.signal
      : AbortSignal.any([timeout.signal, options.signal])
  const timer =
    options.timeoutMs === undefined
      ? undefined
      : setTimeout(() => timeout.abort(), options.timeoutMs)
  try {
    const result = await session.runOperation(() =>
      job.awaitResult(id, { signal, pollIntervalMs: options.pollIntervalMs ?? 100 })
    )
    if (timeout.signal.aborted) throw new JobWaitTimeoutException(id, options.timeoutMs ?? 0)
    if (options.signal?.aborted)
      throw new JobWaitAbortedException(id, { cause: options.signal.reason })
    return unwrap(result, 'awaitResult')
  } catch (cause) {
    if (timeout.signal.aborted) throw new JobWaitTimeoutException(id, options.timeoutMs ?? 0)
    if (options.signal?.aborted)
      throw new JobWaitAbortedException(id, { cause: options.signal.reason })
    throw cause
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

async function cancelJob(
  session: EngineSession,
  registered: RegisteredJob,
  job: CompiledJob,
  id: string
): Promise<void> {
  const cancelled = await session.runOperation(() => job.cancel(id))
  if (Result.isOk(cancelled)) return
  if (!(cancelled.error instanceof JobNotCancellableError) || cancelled.error.state !== 'active') {
    unwrap(cancelled, 'cancel')
    return
  }
  // Active cancellation is requested without stealing the owning worker's lease.
  const record = unwrap(await session.runOperation(() => job.poll(id)), 'cancel')
  if (record === undefined) throw new MqJobException('cancel', 'The job no longer exists')
  unwrap(
    await session.withStore(registered.identity.connection, (store) =>
      store.requestCancellation({ jobId: record.id, now: Date.now() })
    ),
    'cancel'
  )
}

export function createJobClient(
  session: EngineSession,
  registered: RegisteredJob,
  job: CompiledJob
): JobClient<ContractValue, ContractValue> {
  return {
    async enqueue(payload, options) {
      const configured = enqueueOptions(registered, options)
      const value = JSON.parse(payload)
      return unwrap(await session.runOperation(() => job.enqueue(value, configured)), 'enqueue')
    },
    async enqueueMany(items) {
      const inputs = items.map((item) => ({
        payload: JSON.parse(item.payload),
        options: enqueueOptions(registered, item.options)
      }))
      return unwrap(await session.runOperation(() => job.enqueueMany(inputs)), 'enqueueMany')
    },
    async prepare(payload, options) {
      const configured = enqueueOptions(registered, options)
      const value = JSON.parse(payload)
      const request = unwrap(
        await session.runOperation(() => job.prepare(value, configured)),
        'prepare'
      )
      return Object.freeze({ connection: registered.identity.connection, request })
    },
    async poll(id) {
      const record = unwrap(await session.runOperation(() => job.poll(id)), 'poll')
      return record === undefined ? undefined : snapshot(record)
    },
    async attempts(id) {
      const attempts = unwrap(await session.runOperation(() => job.attempts(id)), 'attempts')
      return Object.freeze(attempts.map(attemptView))
    },
    async awaitResult(id, options) {
      return boundedWait(session, job, id, options)
    },
    async cancel(id) {
      await cancelJob(session, registered, job, id)
    },
    async retry(id, options) {
      unwrap(await session.runOperation(() => job.retry(id, options)), 'retry')
    },
    async promote(id) {
      unwrap(await session.runOperation(() => job.promote(id)), 'promote')
    }
  }
}
