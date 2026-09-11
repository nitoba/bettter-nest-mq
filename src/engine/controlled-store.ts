import { ControlsRevisionMismatchError, JobStoreFailure, LeaseLostError, SettlementConflictError, JobNotCancellableError } from 'better-effect-mq'
import type { ControlledJobStoreContract, JobStoreContract, JobStoreError, QueueControlsRecord, QueueName, JobStoreOperation, RecoverStalledResult } from 'better-effect-mq'
import { Result } from 'better-result'
import { QueueControlsException } from '../controls/errors.ts'
import type { QueueDefinition } from '../contracts/queue-definition.ts'

/** The protocol descriptor alone is not proof that its operational extension exists. */
export function controlledStore(store: JobStoreContract): ControlledJobStoreContract | undefined {
  if (!('getControls' in store && store.getControls instanceof Function &&
    'reconcile' in store && store.reconcile instanceof Function &&
    'claimControlled' in store && store.claimControlled instanceof Function &&
    'settleControlled' in store && store.settleControlled instanceof Function &&
    'releaseControlled' in store && store.releaseControlled instanceof Function &&
    'recoverStalledControlled' in store && store.recoverStalledControlled instanceof Function &&
    'cancelControlled' in store && store.cancelControlled instanceof Function)) return undefined
  // SAFETY: a protocol-validated upstream adapter supplies the checked controlled operation set.
  return store as JobStoreContract & ControlledJobStoreContract
}

export function requireControlledStore(store: JobStoreContract): ControlledJobStoreContract {
  const extension = controlledStore(store)
  if (extension === undefined) throw new QueueControlsException('capability', 'The adapter does not implement the controlled-store protocol')
  return extension
}

function normalize<Value>(result: Result<Value, JobStoreError>, operation: string): Result<Value, JobStoreFailure> {
  if (Result.isOk(result)) return result
  if (result.error instanceof JobStoreFailure) return Result.err(result.error)
  const failure = new JobStoreFailure({ operation, retryable: result.error instanceof ControlsRevisionMismatchError, message: `Controlled ${operation} failed` })
  Object.defineProperty(failure, 'cause', { value: result.error })
  return Result.err(failure)
}

/** Adapts only protocol dispatch. Claims, permits, lease fences and rate windows remain upstream. */
export function dispatchStore(store: JobStoreContract, queues: readonly QueueDefinition[]): JobStoreContract {
  const extension = controlledStore(store)
  if (extension === undefined) return store
  const required = new Set(queues.filter((queue) => queue.controls !== undefined).map((queue) => queue.name))

  async function policy(queue: QueueName, failClosed = false): Promise<Result<QueueControlsRecord | undefined, JobStoreFailure>> {
    const result = normalize(await extension!.getControls({ queue }), 'getControls')
    if (Result.isError(result)) return result
    if (failClosed && required.has(queue) && !result.value?.enabled) return Result.err(new JobStoreFailure({ operation: 'claim', retryable: false, message: 'A declared queue control policy is missing or disabled' }))
    return Result.ok(result.value?.enabled ? result.value : undefined)
  }

  async function jobPolicy(jobId: Parameters<JobStoreContract['getJob']>[0]['jobId']) {
    const job = normalize(await store.getJob({ jobId }), 'getJob')
    if (Result.isError(job)) return job
    return job.value === undefined ? Result.ok(undefined) : policy(job.value.queue)
  }

  const forwarded: JobStoreContract = {
    descriptor: store.descriptor,
    enqueue: store.enqueue.bind(store), enqueueMany: store.enqueueMany.bind(store),
    heartbeat: store.heartbeat.bind(store), awaitWake: store.awaitWake.bind(store),
    getJob: store.getJob.bind(store), getAttempts: store.getAttempts.bind(store), list: store.list.bind(store), counts: store.counts.bind(store),
    retry: store.retry.bind(store), requestCancellation: store.requestCancellation.bind(store), promote: store.promote.bind(store), remove: store.remove.bind(store),
    pause: store.pause.bind(store), resume: store.resume.bind(store), pausedQueues: store.pausedQueues.bind(store),
    async claim(request) {
      const controls = await policy(request.queue, true)
      if (Result.isError(controls)) return controls
      if (controls.value === undefined) return store.claim(request)
      const claimed = normalize(await extension.claimControlled({ ...request, controlsRevision: controls.value.revision }), 'claim')
      if (Result.isError(claimed)) return claimed
      return Result.ok({ jobs: claimed.value.jobs, wakeToken: claimed.value.wakeToken, nextRunAt: claimed.value.nextEligibleAtMs ?? claimed.value.nextRunAtMs })
    },
    async settle(request) {
      const controls = await jobPolicy(request.jobId)
      if (Result.isError(controls)) return controls
      if (controls.value === undefined) return store.settle(request)
      const settled = await extension.settleControlled({ ...request, controlsRevision: controls.value.revision })
      if (Result.isError(settled) && (settled.error instanceof LeaseLostError || settled.error instanceof SettlementConflictError)) return Result.err(settled.error)
      return normalize(settled, 'settle')
    },
    async release(request) {
      const controls = await jobPolicy(request.jobId)
      if (Result.isError(controls)) return controls
      if (controls.value === undefined) return store.release(request)
      const released = await extension.releaseControlled({ ...request, controlsRevision: controls.value.revision })
      if (Result.isError(released) && released.error instanceof LeaseLostError) return Result.err(released.error)
      return normalize(released, 'release')
    },
    async cancel(request) {
      const controls = await jobPolicy(request.jobId)
      if (Result.isError(controls)) return controls
      if (controls.value === undefined) return store.cancel(request)
      const cancelled = await extension.cancelControlled({ ...request, controlsRevision: controls.value.revision })
      if (Result.isError(cancelled) && cancelled.error instanceof JobNotCancellableError) return Result.err(cancelled.error)
      return normalize(cancelled, 'cancel')
    },
    async recoverStalled(request) {
      if (request.queue === undefined) {
        // Workers normally supply a queue. A broad call is expanded only over this host's
        // registered queues, so controlled recovery never bypasses permits on another queue.
        if (queues.length === 0) return store.recoverStalled(request)
        const transitions: RecoverStalledResult['transitions'][number][] = []
        for (const queue of queues) {
          const queueName = queue.jobs[0]?.identity.queue
          if (queueName === undefined) continue
          const limit = request.limit === undefined ? undefined : request.limit - transitions.length
          if (limit === 0) break
          // SAFETY: registry construction validated this queue identity using the engine compiler.
          const named = queueName as QueueName
          const next = limit === undefined ? { ...request, queue: named } : { ...request, queue: named, limit }
          const recovered = await forwarded.recoverStalled(next)
          if (Result.isError(recovered)) return recovered
          transitions.push(...recovered.value.transitions)
        }
        return Result.ok({ transitions, recovered: transitions.length })
      }
      const controls = await policy(request.queue)
      if (Result.isError(controls)) return controls
      if (controls.value === undefined) return store.recoverStalled(request)
      return normalize(await extension.recoverStalledControlled({ ...request, queue: request.queue, controlsRevision: controls.value.revision }), 'recoverStalled')
    }
  }
  return forwarded
}
