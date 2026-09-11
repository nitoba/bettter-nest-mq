import { Scope } from '@nestjs/common'
import {
  EXCEPTION_FILTERS_METADATA,
  GUARDS_METADATA,
  INTERCEPTORS_METADATA,
  PIPES_METADATA
} from '@nestjs/common/constants'
import { ContextIdFactory, DiscoveryService, ModuleRef } from '@nestjs/core'

import { ContractDefinitionException } from '../contracts/errors.ts'
import { JobContract } from '../contracts/job-definition.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { parameterMetadata, processMetadata, workerMetadata } from '../workers/decorators.ts'
import type { MqShutdownOptions } from '../module/mq-module.options.ts'
import type { CompiledJob } from './job-compiler.ts'
import { compileFlowHandler } from './flow-plan.ts'
import type { FlowInvocation } from './flow-plan.ts'
import type { CompiledFlow } from './flow-discovery.ts'
import { MqFlowException } from '../flows/errors.ts'
import { compileWorker } from './worker-plan.ts'
import type { WorkerInvocation, WorkerPlan } from './worker-plan.ts'

type ProviderWrapper = ReturnType<DiscoveryService['getProviders']>[number]
interface CompiledEntry {
  readonly registered: RegisteredJob
  readonly compiled: CompiledJob
}

function methodDescriptor<Target extends object>(
  target: Target,
  method: string
): PropertyDescriptor | undefined {
  let current = target
  while (current !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(current, method)
    if (descriptor !== undefined) return descriptor
    current = Object.getPrototypeOf(current)
  }
  return undefined
}

function rejectEnhancers<Target extends object>(target: Target): void {
  for (const key of [
    EXCEPTION_FILTERS_METADATA,
    GUARDS_METADATA,
    INTERCEPTORS_METADATA,
    PIPES_METADATA
  ]) {
    if (Reflect.hasMetadata(key, target))
      throw new ContractDefinitionException(
        'MQ handlers do not yet support Nest HTTP enhancer metadata; it cannot be silently ignored'
      )
  }
}

function invocationFor(
  wrapper: ProviderWrapper,
  method: string,
  entry: CompiledEntry,
  moduleRef: ModuleRef,
  phase?: 'fanOut' | 'collect'
): FlowInvocation {
  const target = wrapper.metatype
  if (target === undefined || target === null)
    throw new ContractDefinitionException('Workers must be registered as class providers')
  const descriptor = methodDescriptor(target.prototype, method)
  if (
    descriptor === undefined ||
    descriptor.get !== undefined ||
    !(descriptor.value instanceof Function)
  )
    throw new ContractDefinitionException(`Processor ${method} must be a method, not an accessor`)
  const handler = descriptor.value
  rejectEnhancers(target)
  rejectEnhancers(handler)
  const parameters = parameterMetadata(target.prototype, method)
  const length = Math.max(handler.length, ...[...parameters.keys()].map((index) => index + 1))
  const kinds = Array.from({ length }, (_, index) => {
    const kind = parameters.get(index)
    if (kind === undefined)
      throw new ContractDefinitionException(
        `Processor ${method} parameter ${index} needs @JobData or @JobContext`
      )
    if (kind === 'children' && phase !== 'collect')
      throw new MqFlowException('definition', '@FlowChildren is only valid in Collect methods')
    return kind
  })
  const scoped =
    wrapper.scope === Scope.REQUEST ||
    wrapper.scope === Scope.TRANSIENT ||
    !wrapper.isDependencyTreeStatic()
  return async (payload, context, results) => {
    const instance = scoped
      ? await moduleRef.resolve(wrapper.token, ContextIdFactory.create(), { strict: false })
      : wrapper.instance
    if (instance === undefined || instance === null)
      throw new ContractDefinitionException(
        `Worker for ${entry.registered.identity.key} was not resolved`
      )
    return await handler.apply(
      instance,
      kinds.map((kind) => (kind === 'payload' ? payload : kind === 'context' ? context : results))
    )
  }
}

export function discoverWorkers(
  discovery: DiscoveryService,
  moduleRef: ModuleRef,
  entries: ReadonlyMap<JobContract, CompiledEntry>,
  shutdown: MqShutdownOptions,
  flows: readonly CompiledFlow[] = []
): readonly WorkerPlan[] {
  const names = new Set<string>()
  const processed = new Set<string>()
  const plans: WorkerPlan[] = []
  for (const wrapper of discovery.getProviders()) {
    if (wrapper.isAlias || wrapper.metatype === undefined || wrapper.metatype === null) continue
    const target = wrapper.metatype
    // Factory providers may be arrow functions; unlike classes, they have no prototype metadata.
    if (target.prototype === undefined || target.prototype === null) continue
    const methods = processMetadata(target.prototype)
    const options = workerMetadata(target)
    if (options === undefined) {
      if (methods.size > 0)
        throw new ContractDefinitionException(
          'A @Process method requires @Worker on its provider class'
        )
      continue
    }
    if (names.has(options.name))
      throw new ContractDefinitionException(`Duplicate worker name ${options.name}`)
    names.add(options.name)
    if (methods.size === 0)
      throw new ContractDefinitionException(`Worker ${options.name} has no processors`)
    const invocations: WorkerInvocation[] = []
    for (const [method, metadata] of methods) {
      let queue
      try {
        queue = moduleRef.get(metadata.queue, { strict: false })
      } catch (cause) {
        throw new ContractDefinitionException(
          `Worker queue ${metadata.queue.name} is not registered: ${cause instanceof Error ? cause.name : 'resolution failed'}`
        )
      }
      const contract = Object.getOwnPropertyDescriptor(queue, metadata.property)?.value
      const entry = contract instanceof JobContract ? entries.get(contract) : undefined
      if (entry === undefined)
        throw new ContractDefinitionException(
          `Processor ${method} refers to an unregistered job property`
        )
      if (
        flows.some((flow) => flow.parent.registered.identity.key === entry.registered.identity.key)
      )
        throw new MqFlowException(
          'definition',
          'A flow parent cannot also have a plain Process handler'
        )
      if (processed.has(entry.registered.identity.key))
        throw new ContractDefinitionException(
          `Duplicate processor for ${entry.registered.identity.key}`
        )
      processed.add(entry.registered.identity.key)
      invocations.push({
        ...entry,
        options: metadata.options,
        invoke: invocationFor(wrapper, method, entry, moduleRef)
      })
    }
    const registrations = flows.map((flow) =>
      flow.owner !== options.name
        ? flow.definition
        : compileFlowHandler(
            flow,
            invocationFor(wrapper, flow.fanOutMethod, flow.parent, moduleRef, 'fanOut'),
            invocationFor(wrapper, flow.collectMethod, flow.parent, moduleRef, 'collect')
          )
    )
    plans.push(compileWorker(options, invocations, shutdown, registrations))
  }
  return plans
}
