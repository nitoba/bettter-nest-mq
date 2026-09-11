import { flowToken, flowAlias } from './flow-plan.ts'
import type { NamedFlow, OperationFlow } from './flow-plan.ts'
import type { CompiledFlow } from './flow-discovery.ts'
import type { FlowStoreV2 } from 'better-effect-mq'
import { MqFlowException } from '../flows/errors.ts'
import { JobSchedules } from 'better-effect-mq'
import type { JobScheduleStoreContract, JobSchedulerHandle } from 'better-effect-mq'
import { resolveScheduleOptions } from '../schedules/decorator.ts'
import { MqScheduleException } from '../schedules/errors.ts'
import type { MqScheduleOptions, SchedulerSnapshot } from '../schedules/types.ts'
import {
  scheduleToken,
  scheduleAlias,
  scheduleRegistries,
  schedulerLayer,
  Scheduler
} from './schedule-plan.ts'
import type {
  NamedSchedule,
  OperationSchedule,
  EngineScheduler,
  ScheduleRegistry,
  ScheduleDrafts
} from './schedule-plan.ts'
import type { CompiledSchedule } from './schedule-compiler.ts'
import { Effect, Layer, Runtime } from 'better-effect'
import { Clock, ClockLive } from 'better-effect/standard-services'
import { assertJobStoreProtocolCompatible } from 'better-effect-mq'
import type { JobOperation, JobStoreContract, WorkerHandle } from 'better-effect-mq'
import { Result } from 'better-result'
import type { OutboxStore, OutboxPublisherHandle } from 'better-effect-mq-outbox'
import { MqOutboxException } from '../outbox/errors.ts'
import { resolveOutboxOptions } from '../outbox/options.ts'
import type { MqOutboxOptions, OutboxPublisherSnapshot } from '../outbox/types.ts'
import { outboxToken, outboxPublisherLayer, Publisher } from './outbox-plan.ts'
import type { NamedOutbox, EngineOutboxPublisher } from './outbox-plan.ts'

import type {
  MqConnectionMap,
  MqConnectionMonitor,
  MqConnectionSnapshot,
  MqEngineState
} from '../connections/connection.ts'
import { MqConnectionException, MqEngineStateException } from '../connections/errors.ts'
import type { QueueDefinition } from '../contracts/queue-definition.ts'
import { MqJobException } from '../jobs/errors.ts'
import type { MqShutdownOptions } from '../module/mq-module.options.ts'
import type { WorkerIdleOptions, WorkerMonitor, WorkerSnapshot } from '../workers/types.ts'
import { connectionDefinition, copyConnections, namedStoreToken } from './connection-definition.ts'
import type { AcquiredConnection, NamedStore } from './connection-definition.ts'
import { operationStoreLayer } from './operation-store.ts'
import type { OperationStore, OperationStoreToken } from './operation-store.ts'
import type { EngineWorker, WorkerPlan } from './worker-plan.ts'

interface ReadyConnection {
  readonly store: JobStoreContract
  readonly snapshot: MqConnectionSnapshot
}
interface RunningWorker {
  readonly name: string
  readonly handle: WorkerHandle
}

function cleanupError<Cause>(cause: Cause): Error {
  return cause instanceof Error ? cause : new Error('An MQ resource failed to close', { cause })
}

/** The only owner of an application's runtime; stores, Clock and Workers share its lifetime. */
export class EngineSession implements MqConnectionMonitor, WorkerMonitor {
  private currentState: MqEngineState = 'idle'
  private readonly configured: MqConnectionMap | undefined
  private readonly shutdown: Readonly<Required<MqShutdownOptions>>
  private runtime:
    | Runtime<
        | NamedStore
        | OperationStore
        | EngineWorker
        | Clock
        | NamedOutbox
        | EngineOutboxPublisher
        | NamedFlow
        | OperationFlow
        | NamedSchedule
        | OperationSchedule
        | EngineScheduler
      >
    | undefined
  private acquired: AcquiredConnection[] = []
  private readonly scheduleOptions: Required<MqScheduleOptions>
  private readonly schedulerEnabled: boolean
  private scheduledDefinitions: readonly CompiledSchedule[] = []
  private flowDefinitions: readonly CompiledFlow[] = []
  private readyFlows = new Map<string, FlowStoreV2>()
  private readySchedules = new Map<string, JobScheduleStoreContract>()
  private runningScheduler: JobSchedulerHandle | undefined
  private scheduling: Promise<void> | undefined
  private hasScheduler = false
  private schedulerErrors = 0
  private readonly outboxOptions: Readonly<Required<MqOutboxOptions>>
  private readonly publisherEnabled: boolean
  private readyOutboxes = new Map<string, OutboxStore>()
  private runningPublisher: OutboxPublisherHandle | undefined
  private publishing: Promise<void> | undefined
  private hasPublisher = false
  private ready = new Map<string, ReadyConnection>()
  private snapshots: ReadonlyArray<MqConnectionSnapshot> = Object.freeze([])
  private plans: readonly WorkerPlan[] = []
  private runningWorkers: RunningWorker[] = []
  private starting: Promise<void> | undefined
  private activating: Promise<void> | undefined
  private closing: Promise<void> | undefined
  private cleaning: Promise<void> | undefined
  private stopRequested = false
  private runtimeCleanupErrors: Error[] = []

  constructor(
    connections: MqConnectionMap | undefined,
    shutdown: MqShutdownOptions = {},
    outbox: { enabled?: boolean; options?: MqOutboxOptions } = {},
    schedules: { enabled?: boolean; options?: MqScheduleOptions } = {}
  ) {
    this.outboxOptions = resolveOutboxOptions(outbox.options)
    this.scheduleOptions = resolveScheduleOptions(schedules.options)
    this.schedulerEnabled = schedules.enabled ?? true
    this.publisherEnabled = outbox.enabled ?? true
    this.configured = copyConnections(connections)
    this.shutdown = Object.freeze({
      gracePeriodMs: shutdown.gracePeriodMs ?? 30_000,
      abortAfterGracePeriod: shutdown.abortAfterGracePeriod ?? true
    })
  }

  get state(): MqEngineState {
    return this.currentState
  }
  connections(): ReadonlyArray<MqConnectionSnapshot> {
    return this.snapshots
  }

  start(
    queues: ReadonlyArray<QueueDefinition>,
    plans: readonly WorkerPlan[] = [],
    schedules: readonly CompiledSchedule[] = [],
    flows: readonly CompiledFlow[] = []
  ): Promise<void> {
    if (this.stopRequested) return Promise.reject(new MqEngineStateException(this.state, 'start'))
    if (this.starting !== undefined) return this.starting
    this.plans = plans
    this.scheduledDefinitions = schedules
    this.flowDefinitions = flows
    this.currentState = 'starting'
    this.starting = this.initialize(queues)
    return this.starting
  }

  private assertStarting(): void {
    if (this.stopRequested) throw new MqEngineStateException(this.state, 'finish startup')
  }

  private async initialize(queues: ReadonlyArray<QueueDefinition>): Promise<void> {
    let acquiring = '<engine>'
    try {
      this.assertStarting()
      if (this.configured === undefined) {
        this.currentState = 'disabled'
        return
      }
      const entries = Object.entries(this.configured)
      const names = new Set(entries.map(([name]) => name))
      for (const queue of queues) {
        if (!names.has(queue.connection))
          throw new MqConnectionException(queue.connection, 'configuration')
      }
      const boundaries: Array<{ boundary: object | string; scope: string }> = []
      for (const [name, connection] of entries) {
        const definition = connectionDefinition(connection, name)
        if (
          boundaries.some(
            (previous) =>
              previous.boundary === definition.boundary && previous.scope === definition.scope
          )
        ) {
          throw new MqConnectionException(name, 'configuration', {
            cause: new Error('The same storage boundary was configured twice')
          })
        }
        boundaries.push(definition)
      }
      const bindings = []
      for (const [name, connection] of entries) {
        this.assertStarting()
        acquiring = name
        const definition = connectionDefinition(connection, name)
        const token = namedStoreToken(name)
        const resource = await definition.acquire(token)
        this.acquired.push(resource)
        bindings.push({ name, connection, definition, token, resource })
      }
      this.assertStarting()
      if (bindings.length === 0 && this.plans.length === 0) {
        this.currentState = 'ready'
        return
      }
      const stores = Layer.merge(...bindings.map((binding) => binding.resource.layer))
      const operations = Layer.merge(
        ...bindings.map((binding) => operationStoreLayer(binding.name, queues))
      )
      const workers = Layer.merge(...this.plans.map((plan) => plan.layer))
      const outboxBindings = bindings.flatMap((binding) => {
        const factory = binding.resource.outbox
        return factory === undefined
          ? []
          : [{ name: binding.name, token: outboxToken(binding.name), layer: factory(binding.name) }]
      })
      const flowBindings = bindings.flatMap((binding) => {
        const factory = binding.resource.flows
        return factory === undefined
          ? []
          : [{ name: binding.name, token: flowToken(binding.name), layer: factory(binding.name) }]
      })
      const flowNames = new Set(flowBindings.map((binding) => binding.name))
      for (const definition of this.flowDefinitions) {
        for (const entry of [definition.parent, ...definition.children.values()]) {
          if (!flowNames.has(entry.registered.identity.connection))
            throw new MqFlowException(
              'unavailable',
              'Every flow participant requires an explicitly enabled flow store'
            )
        }
      }
      const flowStores = Layer.merge(...flowBindings.map((binding) => binding.layer))
      const flowAliases = Layer.merge(...flowBindings.map((binding) => flowAlias(binding.name)))
      const scheduleBindings = bindings.flatMap((binding) => {
        const factory = binding.resource.schedules
        return factory === undefined
          ? []
          : [
              {
                name: binding.name,
                token: scheduleToken(binding.name),
                layer: factory(binding.name)
              }
            ]
      })
      const scheduleNames = scheduleBindings.map((binding) => binding.name)
      for (const entry of this.scheduledDefinitions) {
        if (!scheduleNames.includes(entry.registered.identity.connection))
          throw new MqScheduleException(
            'unavailable',
            'Declared schedules require an explicitly enabled schedule store'
          )
      }
      const scheduleStores = Layer.merge(...scheduleBindings.map((binding) => binding.layer))
      const scheduleAliases = Layer.merge(...scheduleNames.map(scheduleAlias))
      const registries = scheduleRegistries(
        scheduleNames,
        this.scheduledDefinitions,
        this.scheduleOptions.group
      )
      this.hasScheduler = this.schedulerEnabled && scheduleBindings.length > 0
      const scheduler = schedulerLayer(registries, this.scheduleOptions, () => {
        this.schedulerErrors += 1
      })
      const outboxes = Layer.merge(...outboxBindings.map((binding) => binding.layer))
      this.hasPublisher = this.publisherEnabled && outboxBindings.length > 0
      // The layer is inert. Only hasPublisher controls acquisition/activation below.
      const publisher = outboxPublisherLayer(
        outboxBindings.map((binding) => binding.name),
        bindings.map((binding) => binding.name),
        this.outboxOptions
      )
      this.runtime = await Runtime.make(
        Layer.merge(
          ClockLive,
          stores,
          operations,
          workers,
          outboxes,
          publisher,
          flowStores,
          flowAliases,
          scheduleStores,
          scheduleAliases,
          scheduler
        ),
        {
          onCleanupFailure: (diagnostic) => {
            this.runtimeCleanupErrors.push(
              new Error('MQ runtime cleanup diagnostic', { cause: diagnostic })
            )
          }
        }
      )
      const runtime = this.runtime
      const ready = new Map<string, ReadyConnection>()
      for (const binding of bindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() =>
          Effect.gen(async function* () {
            return Result.ok(yield* binding.token)
          })
        )
        if (Result.isError(resolved))
          throw new MqConnectionException(binding.name, 'acquire', { cause: resolved.error })
        const store = resolved.value
        let descriptor
        try {
          descriptor = assertJobStoreProtocolCompatible(store.descriptor)
          for (const capability of binding.definition.requirements) {
            if (descriptor.capabilities[capability] !== true)
              throw new Error(`Unsupported store capability: ${capability}`)
          }
        } catch (cause) {
          throw new MqConnectionException(binding.name, 'protocol', { cause })
        }
        await runtime.run(async () => {
          const result = await store.counts()
          if (Result.isError(result))
            throw new MqConnectionException(binding.name, 'probe', { cause: result.error })
        })
        ready.set(binding.name, {
          store,
          snapshot: Object.freeze({
            name: binding.name,
            adapter: descriptor.adapter,
            adapterVersion: descriptor.adapterVersion,
            protocolVersion: descriptor.protocolVersion,
            layoutVersion: descriptor.layoutVersion,
            ownership: binding.connection.ownership,
            capabilities: Object.freeze({ ...descriptor.capabilities })
          })
        })
      }
      const readyFlows = new Map<string, FlowStoreV2>()
      for (const binding of flowBindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() =>
          Effect.gen(async function* () {
            return Result.ok(yield* binding.token)
          })
        )
        if (Result.isError(resolved))
          throw new MqFlowException('unavailable', 'Flow store acquisition failed', {
            cause: resolved.error
          })
        if (
          resolved.value.descriptor.protocolVersion !== 2 ||
          !['complete', 'not-required'].includes(resolved.value.descriptor.migration.status)
        )
          throw new MqFlowException(
            'unavailable',
            'Flow protocol v2 and a complete migration are required'
          )
        const probe = await resolved.value.peekOutbox({ limit: 1 })
        if (Result.isError(probe))
          throw new MqFlowException('unavailable', 'Flow store probe failed', {
            cause: probe.error
          })
        readyFlows.set(binding.name, resolved.value)
      }
      const readySchedules = new Map<string, JobScheduleStoreContract>()
      for (const binding of scheduleBindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() =>
          Effect.gen(async function* () {
            return Result.ok(yield* binding.token)
          })
        )
        if (Result.isError(resolved))
          throw new MqScheduleException('unavailable', 'Schedule store acquisition failed', {
            cause: resolved.error
          })
        const descriptor = resolved.value.descriptor
        if (
          descriptor.extension !== 'better-effect-mq/schedules' ||
          descriptor.extensionVersion !== 1 ||
          descriptor.jobStoreProtocolVersion !== 1
        )
          throw new MqScheduleException('unavailable', 'Incompatible schedule store protocol')
        const probe = await runtime.run(async () => ({
          value: await resolved.value.listSchedules({ limit: 1 })
        }))
        if (Result.isError(probe.value))
          throw new MqScheduleException('unavailable', 'Schedule store probe failed', {
            cause: probe.value.error
          })
        readySchedules.set(binding.name, resolved.value)
      }
      const readyOutboxes = new Map<string, OutboxStore>()
      for (const binding of outboxBindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() =>
          Effect.gen(async function* () {
            return Result.ok(yield* binding.token)
          })
        )
        if (Result.isError(resolved))
          throw new MqOutboxException('unavailable', 'Outbox store acquisition failed', {
            cause: resolved.error
          })
        if (resolved.value.descriptor.protocolVersion !== 1)
          throw new MqOutboxException('configuration', 'Unsupported outbox protocol')
        const counts = await runtime.run(async () => ({ value: await resolved.value.counts() }))
        if (Result.isError(counts.value))
          throw new MqOutboxException('unavailable', 'Outbox storage probe failed', {
            cause: counts.value.error
          })
        readyOutboxes.set(binding.name, resolved.value)
      }
      this.assertStarting()
      this.readyOutboxes = readyOutboxes
      this.readySchedules = readySchedules
      this.readyFlows = readyFlows
      this.ready = ready
      this.snapshots = Object.freeze([...ready.values()].map((value) => value.snapshot))
      this.currentState = 'ready'
    } catch (cause) {
      const primary =
        cause instanceof MqConnectionException ||
        cause instanceof MqEngineStateException ||
        cause instanceof MqFlowException ||
        cause instanceof MqScheduleException ||
        cause instanceof MqOutboxException
          ? cause
          : new MqConnectionException(acquiring, 'acquire', { cause })
      try {
        await this.cleanup()
      } catch (cleanupCause) {
        throw new AggregateError(
          [primary, cleanupCause],
          'MQ startup failed and resource cleanup also failed',
          { cause: primary }
        )
      } finally {
        if (!this.stopRequested) this.currentState = 'failed'
      }
      throw primary
    }
  }

  async withFlows<Value>(
    name: string,
    operation: (store: FlowStoreV2) => Value | PromiseLike<Value>
  ): Promise<Value> {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqFlowException('unavailable', 'The MQ engine is not ready')
    const store = this.readyFlows.get(name)
    if (store === undefined)
      throw new MqFlowException('unavailable', 'This connection has no enabled flow store')
    const result = await runtime.run(async () => ({ value: await operation(store) }))
    return result.value
  }

  scheduleSources(): readonly string[] {
    return Object.freeze([...this.readySchedules.keys()])
  }
  async withSchedules<Value>(
    name: string,
    operation: (store: JobScheduleStoreContract) => Value | PromiseLike<Value>
  ): Promise<Value> {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqScheduleException('unavailable', 'The MQ engine is not ready')
    const store = this.readySchedules.get(name)
    if (store === undefined)
      throw new MqScheduleException('unavailable', 'This connection has no enabled schedule store')
    const result = await runtime.run(async () => ({ value: await operation(store) }))
    return result.value
  }
  async reconcileSchedules(definition: ScheduleRegistry) {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqScheduleException('unavailable', 'The MQ engine is not ready')
    const result = await runtime.run(() =>
      Effect.gen(async function* () {
        return Result.ok(
          yield* JobSchedules.reconcile<string, ScheduleDrafts, readonly OperationStoreToken[]>(
            definition,
            { removal: 'warn' }
          )
        )
      })
    )
    if (Result.isError(result))
      throw new MqScheduleException('operation', 'Schedule reconciliation failed', {
        cause: result.error
      })
    return result.value
  }
  scheduler(): SchedulerSnapshot | undefined {
    const handle = this.runningScheduler
    return handle === undefined
      ? undefined
      : Object.freeze({
          state: handle.state,
          activeTickCount: handle.activeTickCount,
          reportedErrors: this.schedulerErrors
        })
  }
  async sweepSchedules(): Promise<void> {
    if (this.state !== 'ready' || this.runningScheduler === undefined || this.runtime === undefined)
      throw new MqScheduleException(
        'unavailable',
        'No running scheduler is enabled in this application'
      )
    const handle = this.runningScheduler
    const errors = this.schedulerErrors
    await this.runtime.run(async () => {
      await handle.sweep()
    })
    if (errors !== this.schedulerErrors)
      throw new MqScheduleException(
        'operation',
        'One or more scheduler operations failed during the sweep'
      )
  }
  activateScheduler(): Promise<void> {
    this.scheduling ??= this.startScheduler()
    return this.scheduling
  }
  private async startScheduler(): Promise<void> {
    if (!this.hasScheduler) return
    const runtime = this.runtime
    if (runtime === undefined || this.state !== 'ready')
      throw new MqScheduleException(
        'unavailable',
        'Cannot start a scheduler before storage is ready'
      )
    this.assertStarting()
    const result = await runtime.run(() =>
      Effect.gen(async function* () {
        return Result.ok(yield* Scheduler)
      })
    )
    if (Result.isError(result))
      throw new MqScheduleException('unavailable', 'Scheduler activation failed', {
        cause: result.error
      })
    this.runningScheduler = result.value
    this.assertStarting()
  }

  /** Called after clients are bound; lazy Worker layers still belong to the same runtime. */
  activateWorkers(): Promise<void> {
    this.activating ??= this.activate()
    return this.activating
  }

  private async activate(): Promise<void> {
    if (this.plans.length === 0) return
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqEngineStateException(this.state, 'start workers')
    for (const plan of this.plans) {
      this.assertStarting()
      const result = await runtime.run(() =>
        Effect.gen(async function* () {
          return Result.ok(yield* plan.token)
        })
      )
      if (Result.isError(result))
        throw new MqJobException('binding', 'Worker activation failed', { cause: result.error })
      this.runningWorkers.push({ name: plan.name, handle: result.value })
    }
    this.assertStarting()
  }

  activateOutboxPublisher(): Promise<void> {
    this.publishing ??= this.activatePublisher()
    return this.publishing
  }

  private async activatePublisher(): Promise<void> {
    if (!this.hasPublisher) return
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqOutboxException('unavailable', 'The outbox runtime is not ready')
    this.assertStarting()
    const resolved = await runtime.run(() =>
      Effect.gen(async function* () {
        return Result.ok(yield* Publisher)
      })
    )
    if (Result.isError(resolved))
      throw new MqOutboxException('unavailable', 'The outbox publisher could not start', {
        cause: resolved.error
      })
    this.runningPublisher = resolved.value
    this.assertStarting()
  }

  outboxPublisher(): OutboxPublisherSnapshot | undefined {
    const publisher = this.runningPublisher
    return publisher === undefined
      ? undefined
      : Object.freeze({
          id: publisher.id,
          state: publisher.state,
          activeCount: publisher.activeCount
        })
  }

  async withOutbox<Value>(
    name: string,
    operation: (store: OutboxStore) => Value | PromiseLike<Value>
  ): Promise<Value> {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqOutboxException('unavailable', 'The outbox runtime is not ready')
    const store = this.readyOutboxes.get(name)
    if (store === undefined)
      throw new MqOutboxException('unavailable', 'This connection has no enabled outbox')
    const result = await runtime.run(async () => ({ value: await operation(store) }))
    return result.value
  }

  async runOperation<Value, Failure>(
    operation: () => JobOperation<Value, Failure, OperationStoreToken, true>
  ): Promise<Result<Value, Failure>> {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined)
      throw new MqJobException('binding', `The MQ engine is ${this.state}`)
    return runtime.run(() =>
      Effect.gen(async function* () {
        return Result.ok(yield* operation())
      })
    )
  }

  workers(): readonly WorkerSnapshot[] {
    return Object.freeze(
      this.runningWorkers.map(({ name, handle }) =>
        Object.freeze({ name, id: handle.id, state: handle.state, activeCount: handle.activeCount })
      )
    )
  }

  async awaitIdle(options: WorkerIdleOptions = {}): Promise<void> {
    if (this.state !== 'ready') throw new MqEngineStateException(this.state, 'wait for workers')
    await Promise.all(
      this.runningWorkers.map(async ({ handle }) => {
        await handle.awaitIdle(options)
      })
    )
  }

  async withStore<Value>(
    name: string,
    operation: (store: JobStoreContract) => Value | PromiseLike<Value>
  ): Promise<Value> {
    if (this.state !== 'ready' || this.runtime === undefined)
      throw new MqEngineStateException(this.state, 'access a store')
    const connection = this.ready.get(name)
    if (connection === undefined) throw new MqConnectionException(name, 'configuration')
    const outcome = await this.runtime.run(async () => ({
      value: await operation(connection.store)
    }))
    return outcome.value
  }

  async probe(name: string): Promise<MqConnectionSnapshot> {
    return this.withStore(name, async (store) => {
      try {
        const result = await store.counts()
        if (Result.isError(result)) throw result.error
      } catch (cause) {
        throw new MqConnectionException(name, 'probe', { cause })
      }
      const snapshot = this.ready.get(name)?.snapshot
      if (snapshot === undefined || this.state !== 'ready')
        throw new MqEngineStateException(this.state, 'finish a probe')
      return snapshot
    })
  }

  close(): Promise<void> {
    if (this.closing !== undefined) return this.closing
    this.stopRequested = true
    this.currentState = 'stopping'
    this.snapshots = Object.freeze([])
    this.closing = this.finishClose()
    return this.closing
  }

  private async finishClose(): Promise<void> {
    try {
      if (this.starting !== undefined) await Promise.allSettled([this.starting])
      if (this.activating !== undefined) await Promise.allSettled([this.activating])
      if (this.publishing !== undefined) await Promise.allSettled([this.publishing])
      if (this.scheduling !== undefined) await Promise.allSettled([this.scheduling])
      await this.cleanup()
    } finally {
      this.currentState = 'closed'
    }
  }

  private cleanup(): Promise<void> {
    this.cleaning ??= this.releaseResources()
    return this.cleaning
  }

  private async releaseResources(): Promise<void> {
    const failures: Error[] = []
    try {
      await this.runtime?.dispose({
        gracePeriod: this.shutdown.gracePeriodMs,
        abortAfterGracePeriod: this.shutdown.abortAfterGracePeriod
      })
    } catch (cause) {
      failures.push(cleanupError(cause))
    } finally {
      this.runtime = undefined
      this.ready.clear()
      this.readyOutboxes.clear()
      this.readySchedules.clear()
      this.readyFlows.clear()
      this.runningScheduler = undefined
      this.runningPublisher = undefined
      this.runningWorkers = []
      this.snapshots = Object.freeze([])
    }
    if (failures.length === 0) failures.push(...this.runtimeCleanupErrors)
    this.runtimeCleanupErrors = []
    for (const resource of this.acquired.toReversed()) {
      try {
        await resource.release?.()
      } catch (cause) {
        failures.push(cleanupError(cause))
      }
    }
    this.acquired = []
    if (failures.length > 0) throw new AggregateError(failures, 'MQ resource cleanup failed')
  }
}
