import type { DiscoveryService, ModuleRef } from '@nestjs/core'
import { Flow as EngineFlow } from 'better-effect-mq'
import type { FlowDefinition } from 'better-effect-mq'
import { JobContract } from '../contracts/job-definition.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { flowMetadata, flowPhaseMetadata } from '../flows/decorators.ts'
import type { FlowOptions } from '../flows/types.ts'
import type { FlowJobReference } from '../flows/references.ts'
import { MqFlowException } from '../flows/errors.ts'
import { workerMetadata, processMetadata } from '../workers/decorators.ts'
import type { CompiledJob } from './job-compiler.ts'

export interface FlowEntry {
  readonly registered: RegisteredJob
  readonly compiled: CompiledJob
}
export type EngineFlowDefinition = FlowDefinition<CompiledJob, readonly CompiledJob[]>
export interface CompiledFlow {
  readonly owner: string
  readonly options: Readonly<FlowOptions>
  readonly parent: FlowEntry
  readonly children: ReadonlyMap<FlowJobReference, FlowEntry>
  readonly definition: EngineFlowDefinition
  readonly fanOutMethod: string
  readonly collectMethod: string
}
export function resolveFlowReference(
  reference: FlowJobReference,
  moduleRef: ModuleRef,
  entries: ReadonlyMap<JobContract, FlowEntry>
): FlowEntry {
  try {
    const queue = moduleRef.get(reference.queue, { strict: false })
    const contract = Object.getOwnPropertyDescriptor(queue, reference.property)?.value
    const entry = contract instanceof JobContract ? entries.get(contract) : undefined
    if (entry === undefined)
      throw new MqFlowException('identity', 'Flow references must target registered job properties')
    return entry
  } catch (cause) {
    if (cause instanceof MqFlowException) throw cause
    throw new MqFlowException('identity', 'The flow queue is not registered', { cause })
  }
}
export function discoverFlows(
  discovery: DiscoveryService,
  moduleRef: ModuleRef,
  entries: ReadonlyMap<JobContract, FlowEntry>
): readonly CompiledFlow[] {
  const flows: CompiledFlow[] = []
  const names = new Set<string>()
  const parents = new Set<string>()
  for (const wrapper of discovery.getProviders()) {
    if (wrapper.isAlias || wrapper.metatype?.prototype === undefined) continue
    const target = wrapper.metatype
    const options = flowMetadata(target)
    const phases = flowPhaseMetadata(target.prototype)
    if (options === undefined) {
      if (phases.size > 0)
        throw new MqFlowException(
          'definition',
          'Flow phase methods require @Flow on their concrete Service'
        )
      continue
    }
    const worker = workerMetadata(target)
    if (worker === undefined)
      throw new MqFlowException('definition', 'A flow Service requires @Worker')
    if (processMetadata(target.prototype).size === 0)
      throw new MqFlowException(
        'definition',
        'The pinned Worker requires at least one real @Process handler; flow-only Workers are unsupported'
      )
    const fanOut = [...phases].filter(([, phase]) => phase === 'fanOut')
    const collect = [...phases].filter(([, phase]) => phase === 'collect')
    if (fanOut.length !== 1 || collect.length !== 1)
      throw new MqFlowException(
        'definition',
        'A flow Service needs exactly one @FanOut and one @Collect method'
      )
    const fanOutMethod = fanOut[0]?.[0]
    const collectMethod = collect[0]?.[0]
    if (fanOutMethod === undefined || collectMethod === undefined)
      throw new MqFlowException('definition', 'Flow phases are missing')
    for (const method of phases.keys())
      if (processMetadata(target.prototype).has(method))
        throw new MqFlowException(
          'definition',
          'Flow phase methods cannot also be @Process handlers'
        )
    const parent = resolveFlowReference(options.parent, moduleRef, entries)
    if (names.has(options.name) || parents.has(parent.registered.identity.key))
      throw new MqFlowException('definition', 'Flow names and parent job identities must be unique')
    names.add(options.name)
    parents.add(parent.registered.identity.key)
    if (parent.registered.controls !== undefined)
      throw new MqFlowException(
        'definition',
        'Flow parents cannot hold distributed queue permits while awaiting children; use a separate unbounded parent queue'
      )
    const children = new Map<FlowJobReference, FlowEntry>()
    // The pinned result resolver uses name/version/store, without queue. Reject that ambiguity.
    const identities = new Set<string>()
    for (const reference of options.children) {
      const child = resolveFlowReference(reference, moduleRef, entries)
      const identity = child.registered.identity
      const key = JSON.stringify([identity.connection, identity.name, identity.version])
      if (identities.has(key))
        throw new MqFlowException(
          'definition',
          'A flow cannot declare duplicate name/version within the same connection, even across queues'
        )
      identities.add(key)
      if (child.registered.controls?.perKeyConcurrency !== undefined)
        throw new MqFlowException(
          'definition',
          'The pinned child-plan protocol cannot carry per-key dispatch keys'
        )
      if (child.registered.policy.delayMs !== 0)
        throw new MqFlowException(
          'definition',
          'Flow children cannot inherit a relative enqueue delay'
        )
      children.set(reference, child)
    }
    try {
      const { name: _name, parent: _parent, children: _children, ...policy } = options
      const definition = EngineFlow.define(options.name, {
        ...policy,
        parent: parent.compiled,
        children: [...children.values()].map((entry) => entry.compiled)
      })
      flows.push({
        owner: worker.name,
        options,
        parent,
        children,
        definition,
        fanOutMethod,
        collectMethod
      })
    } catch (cause) {
      throw new MqFlowException('definition', 'Flow compilation failed', { cause })
    }
  }
  // Durable nested flows are graph edges between parent job contracts. Detect cycles before I/O.
  const byParent = new Map(flows.map((flow) => [flow.parent.registered.identity.key, flow]))
  const visit = (flow: CompiledFlow, path: Set<string>): void => {
    if (path.has(flow.options.name))
      throw new MqFlowException('definition', 'Nested flow definitions contain a cycle')
    const next = new Set(path)
    next.add(flow.options.name)
    for (const child of flow.children.values()) {
      const nested = byParent.get(child.registered.identity.key)
      if (nested !== undefined) visit(nested, next)
    }
  }
  for (const flow of flows) visit(flow, new Set())
  return Object.freeze(flows)
}
export function flowChildEntry(flow: CompiledFlow, reference: FlowJobReference): FlowEntry {
  const exact = flow.children.get(reference)
  if (exact !== undefined) return exact
  const equivalent = [...flow.children].find(
    ([known]) => known.queue === reference.queue && known.property === reference.property
  )?.[1]
  if (equivalent === undefined)
    throw new MqFlowException('identity', 'Child reference is not declared by this flow')
  return equivalent
}
