import { CurrentAbortSignal, Effect } from 'better-effect'
import type { Layer } from 'better-effect'
import { JobContext, Worker } from 'better-effect-mq'
import type {
  WorkerServiceInstance,
  WorkerServiceToken,
  FlowHandler,
  JobId
} from 'better-effect-mq'
import { Result } from 'better-result'

import { ContractDefinitionException, JobFailureException } from '../contracts/errors.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import type { JobExecutionContext, ProcessOptions, WorkerOptions } from '../workers/types.ts'
import type { MqShutdownOptions } from '../module/mq-module.options.ts'
import { operationStoreToken } from './operation-store.ts'
import type { OperationStore } from './operation-store.ts'
import type { FlowJobReads } from './flow-read-store.ts'
import type { CompiledJob, ContractValue } from './job-compiler.ts'
import type { OperationFlow } from './flow-plan.ts'
import type { EngineFlowDefinition } from './flow-discovery.ts'
import { validateDomainFailure } from './job-compiler.ts'

export type EngineWorker = WorkerServiceInstance<`nestjs.worker/${string}`>
export interface WorkerPlan {
  recover(readers: ReadonlyMap<string, FlowJobReads>): Promise<void>
  readonly name: string
  readonly token: WorkerServiceToken<`nestjs.worker/${string}`>
  readonly layer: Layer<EngineWorker, OperationStore | OperationFlow>
}

export interface WorkerInvocation {
  readonly registered: RegisteredJob
  readonly compiled: CompiledJob
  readonly options: ProcessOptions
  invoke(payload: ContractValue, context: JobExecutionContext): Promise<ContractValue>
}

export function compileWorker(
  options: WorkerOptions,
  invocations: ReadonlyArray<WorkerInvocation>,
  shutdown: MqShutdownOptions,
  flows: readonly (
    | EngineFlowDefinition
    | FlowHandler<EngineFlowDefinition, JobContext, JobContext | OperationStore>
  )[] = []
): WorkerPlan {
  // The pinned supervisor keys handlers by queue/name/version, without connection. Keep
  // its limitation explicit rather than failing lazily after database resources are open.
  const identities = new Set<string>()
  for (const invocation of invocations) {
    const identity = invocation.registered.identity
    const key = JSON.stringify([identity.queue, identity.name, identity.version])
    if (identities.has(key)) {
      throw new ContractDefinitionException(
        'One Worker cannot process identical queue/name/version across connections; use separate Worker Services'
      )
    }
    identities.add(key)
  }
  for (const registration of flows) {
    if (!('fanOut' in registration)) continue
    const identity = registration.flow.parent.identity
    const key = JSON.stringify([identity.queue, identity.name, identity.version])
    if (identities.has(key))
      throw new ContractDefinitionException(
        'A flow parent cannot share queue/name/version with another handler in this Worker, even across connections'
      )
    identities.add(key)
  }
  const tag: `nestjs.worker/${string}` = `nestjs.worker/${options.name}`
  const token = Worker.service(tag)
  const handlers = invocations.map((invocation) =>
    Worker.handle(
      invocation.compiled,
      (payload) =>
        Effect.fn(async function* () {
          const context = yield* JobContext
          const signal = yield* CurrentAbortSignal
          const jobContext: JobExecutionContext = Object.freeze({
            jobId: context.jobId,
            queue: context.queue,
            name: context.name,
            version: context.version,
            connection: invocation.registered.identity.connection,
            attempt: context.attempt,
            attemptsMax: context.attemptsMax,
            delivery: context.delivery,
            workerId: context.workerId,
            metadata: Object.freeze({ ...context.metadata }),
            signal
          })
          try {
            return Result.ok(await invocation.invoke(payload, jobContext))
          } catch (cause) {
            if (cause instanceof JobFailureException)
              return Result.err(await validateDomainFailure(invocation.registered, cause))
            throw cause
          }
        }),
      invocation.options
    )
  )
  let recoveryIds: readonly JobId[] = []
  const { name: _name, ...settings } = options
  return {
    name: options.name,
    token,
    async recover(readers) {
      const ids = new Set<JobId>()
      for (const [connection, reader] of readers) {
        const route = operationStoreToken(connection).serviceTag
        const definitions = flows.map((entry) => ('fanOut' in entry ? entry.flow : entry))
        const names = definitions
          .filter((entry) => entry.parent.store.serviceTag === route)
          .map((entry) => entry.name)
        for (const id of await reader.recoveryIds(names)) ids.add(id)
      }
      recoveryIds = Object.freeze([...ids])
    },
    layer: token.layer(() => ({
      ...settings,
      retryDefects: options.retryDefects ?? false,
      handlers,
      flows,
      flowSweepFlowIds: recoveryIds,
      shutdown
    }))
  }
}
