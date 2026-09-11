import type { Effect } from 'better-effect'
import {
  ControlsRevisionMismatchError,
  JobStoreFailure,
  LeaseLostError,
  SettlementConflictError,
  JobNotCancellableError,
  makeQueueName
} from 'better-effect-mq'
import type {
  ControlledJobStoreContract,
  JobStoreContract,
  JobStoreError,
  QueueControlsRecord,
  QueueName,
  RecoverStalledResult
} from 'better-effect-mq'
import { Result } from 'better-result'
import { QueueControlsException } from '../controls/errors.ts'
import type { QueueDefinition } from '../contracts/queue-definition.ts'

/** A capability bit alone does not prove that its operational extension exists. */
export function controlledStore(store: JobStoreContract): ControlledJobStoreContract | undefined {
  if (
    !(
      'getControls' in store &&
      store.getControls instanceof Function &&
      'reconcile' in store &&
      store.reconcile instanceof Function &&
      'claimControlled' in store &&
      store.claimControlled instanceof Function &&
      'settleControlled' in store &&
      store.settleControlled instanceof Function &&
      'releaseControlled' in store &&
      store.releaseControlled instanceof Function &&
      'recoverStalledControlled' in store &&
      store.recoverStalledControlled instanceof Function &&
      'cancelControlled' in store &&
      store.cancelControlled instanceof Function
    )
  )
    return undefined
  // SAFETY: a protocol-validated upstream adapter supplies the checked controlled operation set.
  return store as JobStoreContract & ControlledJobStoreContract
}

export function requireControlledStore(store: JobStoreContract): ControlledJobStoreContract {
  const extension = controlledStore(store)
  if (extension === undefined)
    throw new QueueControlsException(
      'capability',
      'The adapter does not implement the controlled-store protocol'
    )
  return extension
}

function success<Value>(value: Value): Effect<Value, never> {
  // SAFETY: completed Results are the runtime representation of Effect; this value requires no Services.
  return Result.ok(value) as Effect<Value, never>
}
function failure<Failure>(error: Failure): Effect<never, Failure> {
  // SAFETY: the error Result adds only the declaration-only Effect facade with no Service requirements.
  return Result.err(error) as Effect<never, Failure>
}

function normalize<Value>(
  result: Result<Value, JobStoreError>,
  operation: string
): Effect<Value, JobStoreFailure> {
  if (Result.isOk(result)) return success(result.value)
  if (result.error instanceof JobStoreFailure) return failure(result.error)
  const error = new JobStoreFailure({
    operation,
    retryable: result.error instanceof ControlsRevisionMismatchError,
    message: `Controlled ${operation} failed`
  })
  Object.defineProperty(error, 'cause', { value: result.error })
  return failure(error)
}

/** Dispatch only: database claims, permits, lease fencing and rate-window algorithms remain upstream. */
export function dispatchStore(
  store: JobStoreContract,
  queues: readonly QueueDefinition[]
): JobStoreContract {
  const candidate = controlledStore(store)
  if (candidate === undefined) return store
  const extension = candidate
  const required = new Set(
    queues.filter((queue) => queue.controls !== undefined).map((queue) => queue.name)
  )

  async function policy(
    queue: QueueName,
    failClosed = false
  ): Promise<Effect<QueueControlsRecord | undefined, JobStoreFailure>> {
    const result = normalize(await extension.getControls({ queue }), 'getControls')
    if (Result.isError(result)) return failure(result.error)
    if (failClosed && required.has(queue) && !result.value?.enabled)
      return failure(
        new JobStoreFailure({
          operation: 'claim',
          retryable: false,
          message: 'A declared queue control policy is missing or disabled'
        })
      )
    return success(result.value?.enabled ? result.value : undefined)
  }

  async function jobPolicy(
    jobId: Parameters<JobStoreContract['getJob']>[0]['jobId']
  ): Promise<Effect<QueueControlsRecord | undefined, JobStoreFailure>> {
    const job = normalize(await store.getJob({ jobId }), 'getJob')
    if (Result.isError(job)) return failure(job.error)
    return job.value === undefined ? success(undefined) : policy(job.value.queue)
  }

  const forwarded: JobStoreContract = {
    descriptor: store.descriptor,
    enqueue: store.enqueue.bind(store),
    enqueueMany: store.enqueueMany.bind(store),
    heartbeat: store.heartbeat.bind(store),
    awaitWake: store.awaitWake.bind(store),
    getJob: store.getJob.bind(store),
    getAttempts: store.getAttempts.bind(store),
    list: store.list.bind(store),
    counts: store.counts.bind(store),
    retry: store.retry.bind(store),
    requestCancellation: store.requestCancellation.bind(store),
    promote: store.promote.bind(store),
    remove: store.remove.bind(store),
    pause: store.pause.bind(store),
    resume: store.resume.bind(store),
    pausedQueues: store.pausedQueues.bind(store),
    async claim(request) {
      const controls = await policy(request.queue, true)
      if (Result.isError(controls)) return failure(controls.error)
      if (controls.value === undefined) return store.claim(request)
      const claimed = normalize(
        await extension.claimControlled({ ...request, controlsRevision: controls.value.revision }),
        'claim'
      )
      if (Result.isError(claimed)) return failure(claimed.error)
      return success({
        jobs: claimed.value.jobs,
        wakeToken: claimed.value.wakeToken,
        nextRunAt: claimed.value.nextEligibleAtMs ?? claimed.value.nextRunAtMs
      })
    },
    async settle(request) {
      const controls = await jobPolicy(request.jobId)
      if (Result.isError(controls)) return failure(controls.error)
      if (controls.value === undefined) return store.settle(request)
      const settled = await extension.settleControlled({
        ...request,
        controlsRevision: controls.value.revision
      })
      if (
        Result.isError(settled) &&
        (settled.error instanceof LeaseLostError ||
          settled.error instanceof SettlementConflictError)
      )
        return failure(settled.error)
      return normalize(settled, 'settle')
    },
    async release(request) {
      const controls = await jobPolicy(request.jobId)
      if (Result.isError(controls)) return failure(controls.error)
      if (controls.value === undefined) return store.release(request)
      const released = await extension.releaseControlled({
        ...request,
        controlsRevision: controls.value.revision
      })
      if (Result.isError(released) && released.error instanceof LeaseLostError)
        return failure(released.error)
      return normalize(released, 'release')
    },
    async cancel(request) {
      const controls = await jobPolicy(request.jobId)
      if (Result.isError(controls)) return failure(controls.error)
      if (controls.value === undefined) return store.cancel(request)
      const cancelled = await extension.cancelControlled({
        ...request,
        controlsRevision: controls.value.revision
      })
      if (Result.isError(cancelled) && cancelled.error instanceof JobNotCancellableError)
        return failure(cancelled.error)
      return normalize(cancelled, 'cancel')
    },
    async recoverStalled(request) {
      if (request.queue !== undefined) {
        const controls = await policy(request.queue)
        if (Result.isError(controls)) return failure(controls.error)
        if (controls.value === undefined) return store.recoverStalled(request)
        return normalize(
          await extension.recoverStalledControlled({
            ...request,
            queue: request.queue,
            controlsRevision: controls.value.revision
          }),
          'recoverStalled'
        )
      }
      if (queues.length === 0) return store.recoverStalled(request)
      const transitions: RecoverStalledResult['transitions'][number][] = []
      let nativeSweepNeeded = false
      for (const queue of queues) {
        const name = makeQueueName(queue.name)
        if (Result.isError(name))
          return failure(
            new JobStoreFailure({
              operation: 'recoverStalled',
              retryable: false,
              message: 'Invalid registered queue name'
            })
          )
        const controls = await policy(name.value)
        if (Result.isError(controls)) return failure(controls.error)
        if (controls.value === undefined) {
          nativeSweepNeeded = true
          continue
        }
        const limit = request.limit === undefined ? undefined : request.limit - transitions.length
        if (limit === 0) return success({ transitions, recovered: transitions.length })
        const controlled = {
          ...request,
          queue: name.value,
          controlsRevision: controls.value.revision
        }
        const recovered = normalize(
          await extension.recoverStalledControlled(
            limit === undefined ? controlled : { ...controlled, limit }
          ),
          'recoverStalled'
        )
        if (Result.isError(recovered)) return failure(recovered.error)
        transitions.push(...recovered.value.transitions)
      }
      // Ordinary adapters may support only a namespace-wide recovery DTO. Preserve that
      // original request instead of inventing an unsupported queue field. Controlled queues
      // have already been processed at this same timestamp through their fenced operation.
      if (nativeSweepNeeded) {
        const limit = request.limit === undefined ? undefined : request.limit - transitions.length
        if (limit !== 0) {
          const recovered = await store.recoverStalled(
            limit === undefined ? request : { ...request, limit }
          )
          if (Result.isError(recovered)) return failure(recovered.error)
          transitions.push(...recovered.value.transitions)
        }
      }
      return success({ transitions, recovered: transitions.length })
    }
  }
  return forwarded
}
