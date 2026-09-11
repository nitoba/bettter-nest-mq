import { CurrentAbortSignal, Effect } from 'better-effect'
import type { Layer } from 'better-effect'
import { JobContext, Worker } from 'better-effect-mq'
import type { WorkerServiceInstance, WorkerServiceToken } from 'better-effect-mq'
import { Result } from 'better-result'

import { JobFailureException } from '../contracts/errors.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import type { JobExecutionContext, ProcessOptions, WorkerOptions } from '../workers/types.ts'
import type { MqShutdownOptions } from '../module/mq-module.options.ts'
import type { NamedStore } from './connection-definition.ts'
import type { CompiledJob, ContractValue } from './job-compiler.ts'
import { validateDomainFailure } from './job-compiler.ts'

export type EngineWorker = WorkerServiceInstance<`nestjs.worker/${string}`>
export interface WorkerPlan {
  readonly name: string
  readonly token: WorkerServiceToken<`nestjs.worker/${string}`>
  readonly layer: Layer<EngineWorker, NamedStore>
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
  shutdown: MqShutdownOptions
): WorkerPlan {
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
  const { name: _name, ...settings } = options
  return {
    name: options.name,
    token,
    layer: token.layer(() => ({ ...settings, handlers, shutdown }))
  }
}
