import type { ModuleRef, DiscoveryService } from '@nestjs/core'
import { makeJobId, makeQueueName } from 'better-effect-mq'
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
          )
            throw new MqFlowException(
              'definition',
              'Persisted queue controls are incompatible with this flow role; parent permits and child dispatch keys cannot be bypassed'
            )
        })
      }
    }
  }
  private async checked(reference: FlowJobReference, id: string) {
    const entry = resolveFlowReference(reference, this.moduleRef, this.entries)
    const jobId = unwrap(makeJobId(id))
    const record = await this.session.withStore(
      entry.registered.identity.connection,
      async (store) => unwrap(await store.getJob({ jobId }))
    )
    const wanted = entry.registered.identity
    if (
      record !== undefined &&
      (record.queue !== wanted.queue ||
        record.name !== wanted.name ||
        record.version !== wanted.version)
    )
      throw new MqFlowException('identity', 'The flow id belongs to a different job contract')
    return { entry, jobId, record }
  }
  async get(reference: FlowJobReference, id: string): Promise<FlowSnapshot | undefined> {
    const { entry, jobId, record } = await this.checked(reference, id)
    if (record === undefined) return undefined
    const value = await this.session.withFlows(
      entry.registered.identity.connection,
      async (store) => unwrap(await store.getFlow({ flowId: jobId }))
    )
    if (value === undefined) return undefined
    const parent = value.parent
    return Object.freeze({
      id,
      name: parent.flowName,
      parent: entry.registered.identity,
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
    const { entry, jobId, record } = await this.checked(reference, id)
    if (record === undefined)
      throw new MqFlowException('identity', 'The flow parent job does not exist')
    await this.session.withFlows(entry.registered.identity.connection, async (store) => {
      const current = unwrap(await store.getFlow({ flowId: jobId }))
      if (current !== undefined) {
        unwrap(await store.cancel({ flowId: jobId, now: Date.now() }))
        return
      }
      await this.session.withStore(entry.registered.identity.connection, async (jobs) => {
        if (record.state === 'active')
          unwrap(await jobs.requestCancellation({ jobId, now: Date.now() }))
        else unwrap(await jobs.cancel({ jobId, now: Date.now() }))
      })
    })
  }
}
