import type { ModuleRef, DiscoveryService } from '@nestjs/core'
import { makeJobId, makeQueueName } from 'better-effect-mq'
import type { FlowSnapshot as StoredFlow } from 'better-effect-mq'
import { Result } from 'better-result'
import { JobContract } from '../contracts/job-definition.ts'
import { MqFlowException } from '../flows/errors.ts'
import type { FlowJobReference } from '../flows/references.ts'
import type { FlowMonitor, FlowSnapshot } from '../flows/types.ts'
import type { EngineSession } from './engine-session.ts'
import { discoverFlows, resolveFlowReference } from './flow-discovery.ts'
import type { CompiledFlow, FlowEntry } from './flow-discovery.ts'
import { controlledStore } from './controlled-store.ts'

function unwrap<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result))
    throw new MqFlowException('operation', 'Flow storage operation failed', { cause: result.error })
  return result.value
}

/** A materialized manifest has its own v2 identity, including a stable parent-store route.
 * Do not read it through the v1 JobStore decoder, which rejects waiting-children. */
function assertFlowIdentity(value: StoredFlow, definition: CompiledFlow): void {
  if (
    value.parent.flowName !== definition.options.name ||
    value.parent.parentStoreKey !== definition.parent.compiled.store.serviceTag
  ) {
    throw new MqFlowException(
      'identity',
      'The flow id belongs to a different flow definition or parent store'
    )
  }
}

export class FlowsCoordinator implements FlowMonitor {
  private entries: ReadonlyMap<JobContract, FlowEntry> = new Map()
  private definitions: readonly CompiledFlow[] = []
  constructor(
    private readonly session: EngineSession,
    private readonly moduleRef: ModuleRef
  ) {}

  prepare(
    discovery: DiscoveryService,
    entries: ReadonlyMap<JobContract, FlowEntry>
  ): readonly CompiledFlow[] {
    this.entries = entries
    this.definitions = discoverFlows(discovery, this.moduleRef, entries)
    return this.definitions
  }
  async initialize(): Promise<void> {
    for (const flow of this.definitions) {
      for (const entry of [flow.parent, ...flow.children.values()]) {
        await this.session.withStore(entry.registered.identity.connection, async (store) => {
          const controls = controlledStore(store)
          if (controls === undefined) return
          const current = unwrap(
            await controls.getControls({
              queue: unwrap(makeQueueName(entry.registered.identity.queue))
            })
          )
          if (
            current?.enabled &&
            (entry === flow.parent || current.perKeyConcurrency !== undefined)
          ) {
            throw new MqFlowException(
              'definition',
              'Persisted queue controls are incompatible with this flow role; parent permits and child dispatch keys cannot be bypassed'
            )
          }
        })
      }
    }
  }
  private definition(reference: FlowJobReference): CompiledFlow {
    const entry = resolveFlowReference(reference, this.moduleRef, this.entries)
    const definition = this.definitions.find(
      (flow) => flow.parent.registered.contract === entry.registered.contract
    )
    if (definition === undefined)
      throw new MqFlowException('identity', 'The reference is not a registered flow parent')
    return definition
  }
  async get(reference: FlowJobReference, id: string): Promise<FlowSnapshot | undefined> {
    const definition = this.definition(reference)
    const identity = definition.parent.registered.identity
    const flowId = unwrap(makeJobId(id))
    const value = await this.session.withFlows(identity.connection, async (store) =>
      unwrap(await store.getFlow({ flowId }))
    )
    if (value === undefined) return undefined
    assertFlowIdentity(value, definition)
    const parent = value.parent
    return Object.freeze({
      id,
      name: parent.flowName,
      parent: identity,
      state: parent.state,
      depth: parent.depth,
      children: value.children.length,
      counts: Object.freeze({
        pending: parent.flow.pending,
        completed: parent.flow.completed,
        failed: parent.flow.failed,
        cancelled: parent.flow.cancelled
      })
    })
  }
  async cancel(reference: FlowJobReference, id: string): Promise<void> {
    const definition = this.definition(reference)
    const identity = definition.parent.registered.identity
    const flowId = unwrap(makeJobId(id))
    await this.session.withFlows(identity.connection, async (store) => {
      const current = unwrap(await store.getFlow({ flowId }))
      if (current !== undefined) {
        assertFlowIdentity(current, definition)
        unwrap(await store.cancel({ flowId, now: Date.now() }))
        return
      }
      // Before fan-out the parent is an ordinary job. Fan-out may commit between these
      // reads, so on a failed v1 read recheck the manifest instead of weakening validation.
      await this.session.withStore(identity.connection, async (jobs) => {
        const queried = await jobs.getJob({ jobId: flowId })
        if (Result.isError(queried)) {
          const materialized = unwrap(await store.getFlow({ flowId }))
          if (materialized === undefined) {
            unwrap(queried)
            return
          }
          assertFlowIdentity(materialized, definition)
          unwrap(await store.cancel({ flowId, now: Date.now() }))
          return
        }
        const record = queried.value
        if (
          record === undefined ||
          record.queue !== identity.queue ||
          record.name !== identity.name ||
          record.version !== identity.version
        ) {
          throw new MqFlowException(
            'identity',
            'The flow parent job does not exist or belongs to another contract'
          )
        }
        const cancelled = await (record.state === 'active'
          ? jobs.requestCancellation({ jobId: flowId, now: Date.now() })
          : jobs.cancel({ jobId: flowId, now: Date.now() }))
        if (Result.isOk(cancelled)) return
        const materialized = unwrap(await store.getFlow({ flowId }))
        if (materialized === undefined) {
          unwrap(cancelled)
          return
        }
        assertFlowIdentity(materialized, definition)
        unwrap(await store.cancel({ flowId, now: Date.now() }))
      })
    })
  }
}
