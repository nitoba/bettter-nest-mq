import { Effect, Layer, Runtime } from 'better-effect'
import { Clock, ClockLive } from 'better-effect/standard-services'
import { assertJobStoreProtocolCompatible } from 'better-effect-mq'
import type { JobOperation, JobStoreContract, WorkerHandle } from 'better-effect-mq'
import { Result } from 'better-result'

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
import type { AcquiredConnection, NamedStore, NamedStoreToken } from './connection-definition.ts'
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
  private runtime: Runtime<NamedStore | EngineWorker | Clock> | undefined
  private acquired: AcquiredConnection[] = []
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

  constructor(connections: MqConnectionMap | undefined, shutdown: MqShutdownOptions = {}) {
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

  start(queues: ReadonlyArray<QueueDefinition>, plans: readonly WorkerPlan[] = []): Promise<void> {
    if (this.stopRequested) return Promise.reject(new MqEngineStateException(this.state, 'start'))
    if (this.starting !== undefined) return this.starting
    this.plans = plans
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
      const workers = Layer.merge(...this.plans.map((plan) => plan.layer))
      this.runtime = await Runtime.make(Layer.merge(ClockLive, stores, workers), {
        onCleanupFailure: (diagnostic) => {
          this.runtimeCleanupErrors.push(
            new Error('MQ runtime cleanup diagnostic', { cause: diagnostic })
          )
        }
      })
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
      this.assertStarting()
      this.ready = ready
      this.snapshots = Object.freeze([...ready.values()].map((value) => value.snapshot))
      this.currentState = 'ready'
    } catch (cause) {
      const primary =
        cause instanceof MqConnectionException || cause instanceof MqEngineStateException
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

  async runOperation<Value, Failure>(
    operation: () => JobOperation<Value, Failure, NamedStoreToken, true>
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
