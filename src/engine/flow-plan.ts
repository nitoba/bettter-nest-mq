import { isDeepStrictEqual } from 'node:util'
import { publicationRetry, publicationMetadata } from './retry-reference.ts'
import { CurrentAbortSignal, Effect, Layer } from 'better-effect'
import { Flow as EngineFlow, FlowStore, JobContext } from 'better-effect-mq'
import type {
  FlowChildGroup,
  FlowChildOptions as EngineChildOptions,
  FlowStoreInstance
} from 'better-effect-mq'
import { Result } from 'better-result'
import { JobFailureException } from '../contracts/errors.ts'
import { FlowChildPlan } from '../flows/references.ts'
import { MqFlowException } from '../flows/errors.ts'
import { validateSchema, encodeSchema } from '../contracts/schema.ts'
import type { JobExecutionContext } from '../workers/types.ts'
import type { FlowResultsReader } from '../flows/types.ts'
import { namedStoreToken } from './connection-definition.ts'
import type { NamedStoreToken, NamedStore } from './connection-definition.ts'
import { operationStoreToken } from './operation-store.ts'
import type { OperationStoreToken } from './operation-store.ts'
import { compileBackoff, validateDomainFailure } from './job-compiler.ts'
import type { CompiledJob, ContractValue } from './job-compiler.ts'
import type { CompiledFlow } from './flow-discovery.ts'
import { flowChildEntry } from './flow-discovery.ts'
import { FlowResultReader } from './flow-results.ts'

export type NamedFlow = FlowStoreInstance<NamedStoreToken>
export type OperationFlow = FlowStoreInstance<OperationStoreToken>
export type FlowLayerFactory = (name: string) => Layer<NamedFlow, NamedStore>
export function flowToken(name: string) {
  return FlowStore.for(namedStoreToken(name))
}
export function flowAlias(name: string): Layer<OperationFlow, NamedFlow> {
  return Layer.gen(FlowStore.for(operationStoreToken(name)), async function* () {
    return yield* flowToken(name)
  })
}
export type FlowInvocation = (
  payload: ContractValue,
  context: JobExecutionContext,
  results?: FlowResultsReader
) => Promise<ContractValue>

export async function compileFlowChildren(
  flow: CompiledFlow,
  value: ContractValue
): Promise<readonly FlowChildGroup<CompiledJob>[]> {
  if (!Array.isArray(value))
    throw new MqFlowException('definition', 'FanOut must return an array of flowChildren plans')
  const groups: FlowChildGroup<CompiledJob>[] = []
  const keys = new Set<string>()
  for (const plan of value) {
    if (!(plan instanceof FlowChildPlan))
      throw new MqFlowException('definition', 'FanOut items must be created by flowChildren')
    const entry = flowChildEntry(flow, plan.job)
    const items = []
    for (const item of plan.items) {
      if (keys.has(item.key) || keys.size >= flow.definition.maxChildren)
        throw new MqFlowException(
          'definition',
          'FanOut exceeds its bound or repeats a child key across groups'
        )
      keys.add(item.key)
      const payload = await validateSchema(entry.registered.contract.schemas.payload, item.payload)
      const input = JSON.parse(
        await encodeSchema(entry.registered.contract.schemas.payload, payload)
      )
      if (entry.registered.contract.getDispatchKey(payload) !== undefined)
        throw new MqFlowException(
          'definition',
          'The pinned Flow child protocol cannot persist a derived dispatch key'
        )
      if (entry.registered.contract.getIdempotencyKey(payload) !== undefined)
        throw new MqFlowException(
          'definition',
          'Flow child identity belongs to its manifest; global child idempotency keys are not supported'
        )
      const requested = item.options
      const retry = publicationRetry(entry.registered, requested?.retry)
      let options: EngineChildOptions = {
        attempts: retry.attempts,
        metadata: publicationMetadata(entry.registered, requested?.metadata)
      }
      if (requested?.priority !== undefined) options = { ...options, priority: requested.priority }
      if (requested?.timeoutMs !== undefined)
        options = { ...options, timeoutMs: requested.timeoutMs }
      // Flow.children normalizes a policy, but the pinned supervisor forwards it to
      // Job.prepare's persisted-backoff boundary. Inherit defaults; never silently lose an override.
      if (
        requested?.retry !== undefined &&
        !isDeepStrictEqual(compileBackoff(retry), entry.compiled.defaults.backoff)
      )
        throw new MqFlowException(
          'definition',
          'The pinned engine cannot override a flow child backoff; retain the declared policy and change only attempts'
        )
      // Job.prepare consumes codec input, not the already decoded domain value.
      items.push({ key: item.key, payload: input, options })
    }
    groups.push(EngineFlow.children(entry.compiled, items))
  }
  return Object.freeze(groups)
}
export function compileFlowHandler(
  flow: CompiledFlow,
  fanOut: FlowInvocation,
  collect: FlowInvocation
) {
  const parent = flow.parent.registered
  const contextFor = (context: JobContext, signal: AbortSignal): JobExecutionContext =>
    Object.freeze({
      jobId: context.jobId,
      queue: context.queue,
      name: context.name,
      version: context.version,
      connection: parent.identity.connection,
      attempt: context.attempt,
      attemptsMax: context.attemptsMax,
      delivery: context.delivery,
      workerId: context.workerId,
      metadata: Object.freeze({ ...context.metadata }),
      signal
    })
  return EngineFlow.handle(flow.definition, {
    fanOut: (payload) =>
      Effect.fn(async function* () {
        const context = yield* JobContext
        const signal = yield* CurrentAbortSignal
        try {
          return Result.ok(
            await compileFlowChildren(flow, await fanOut(payload, contextFor(context, signal)))
          )
        } catch (cause) {
          if (cause instanceof JobFailureException)
            return Result.err(await validateDomainFailure(parent, cause))
          throw cause
        }
      }),
    collect: (payload, results) =>
      Effect.fn(async function* () {
        const context = yield* JobContext
        const signal = yield* CurrentAbortSignal
        // PostgreSQL and the reference adapter clear the fan-out lease when the manifest
        // becomes waiting-children. The pinned supervisor may reach Collect before its
        // next heartbeat observes that loss. Never invoke a user phase from that stale
        // delivery; the ordinary store claim path will re-admit the ready parent.
        const store = yield* flow.parent.compiled.store
        const current = await store.getJob({ jobId: context.jobId })
        if (Result.isError(current))
          throw new MqFlowException('operation', 'Unable to verify the Collect delivery', {
            cause: current.error
          })
        const job = current.value
        if (
          signal.aborted ||
          job?.state !== 'active' ||
          job.leaseOwner !== context.workerId ||
          job.deliveryCount !== context.delivery ||
          job.leaseExpiresAt === undefined ||
          job.leaseExpiresAt <= Date.now()
        )
          throw new MqFlowException(
            'operation',
            'The Collect delivery no longer owns the parent lease'
          )
        const reader = new FlowResultReader(flow, results)
        try {
          return Result.ok(await collect(payload, contextFor(context, signal), reader))
        } catch (cause) {
          if (cause instanceof JobFailureException)
            return Result.err(await validateDomainFailure(parent, cause))
          throw cause
        } finally {
          await reader.close()
        }
      })
  })
}
