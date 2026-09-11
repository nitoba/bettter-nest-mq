import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
// Normalize a repeated, explicitly counted integration anchor before applying one-shot patches.
const integration = 'src/integrations/postgres.ts'
let pg = readFileSync(integration, 'utf8')
if (!pg.includes('postgresScheduleLayer')) {
  const anchor = '          outbox\n        )'
  assert.equal(pg.split(anchor).length - 1, 2)
  pg = pg.replaceAll(anchor, '          outbox,\n          options.schedules ?? false\n        )')
  writeFileSync(integration, pg)
  let script = readFileSync('.github/apply-schedules.mjs', 'utf8')
  const rows = script.split('\n').filter((row) => !row.startsWith("  ['          outbox\\n"))
  writeFileSync('.github/apply-schedules.mjs', rows.join('\n'))
}
await import('./apply-schedules.mjs')
const file = 'src/engine/engine-session.ts'
let text = readFileSync(file, 'utf8')
if (!text.includes('readySchedules')) {
  function replace(before, after) {
    assert.equal(text.split(before).length - 1, 1, `Session anchor missing: ${before}`)
    text = text.replace(before, after)
  }
  replace("import { Effect, Layer, Runtime }", `import { JobSchedules } from 'better-effect-mq'
import type { JobScheduleStoreContract, JobSchedulerHandle } from 'better-effect-mq'
import { resolveScheduleOptions } from '../schedules/decorator.ts'
import { MqScheduleException } from '../schedules/errors.ts'
import type { MqScheduleOptions, SchedulerSnapshot } from '../schedules/types.ts'
import { scheduleToken, scheduleAlias, scheduleRegistries, schedulerLayer, Scheduler } from './schedule-plan.ts'
import type { NamedSchedule, OperationSchedule, EngineScheduler, ScheduleRegistry } from './schedule-plan.ts'
import type { CompiledSchedule } from './schedule-compiler.ts'
import { Effect, Layer, Runtime }`)
  replace('NamedStore | OperationStore | EngineWorker | Clock | NamedOutbox | EngineOutboxPublisher', 'NamedStore | OperationStore | EngineWorker | Clock | NamedOutbox | EngineOutboxPublisher | NamedSchedule | OperationSchedule | EngineScheduler')
  replace('  private acquired: AcquiredConnection[] = []', `  private acquired: AcquiredConnection[] = []
  private readonly scheduleOptions: Required<MqScheduleOptions>
  private readonly schedulerEnabled: boolean
  private scheduledDefinitions: readonly CompiledSchedule[] = []
  private readySchedules = new Map<string, JobScheduleStoreContract>()
  private runningScheduler: JobSchedulerHandle | undefined
  private scheduling: Promise<void> | undefined
  private hasScheduler = false
  private schedulerErrors = 0`)
  replace('    outbox: { enabled?: boolean; options?: MqOutboxOptions } = {}', '    outbox: { enabled?: boolean; options?: MqOutboxOptions } = {},\n    schedules: { enabled?: boolean; options?: MqScheduleOptions } = {}')
  replace('    this.outboxOptions = resolveOutboxOptions(outbox.options)', '    this.outboxOptions = resolveOutboxOptions(outbox.options)\n    this.scheduleOptions = resolveScheduleOptions(schedules.options)\n    this.schedulerEnabled = schedules.enabled ?? true')
  replace('start(queues: ReadonlyArray<QueueDefinition>, plans: readonly WorkerPlan[] = []): Promise<void>', 'start(queues: ReadonlyArray<QueueDefinition>, plans: readonly WorkerPlan[] = [], schedules: readonly CompiledSchedule[] = []): Promise<void>')
  replace('    this.plans = plans', '    this.plans = plans\n    this.scheduledDefinitions = schedules')
  replace('      const outboxes = Layer.merge', `      const scheduleBindings = bindings.flatMap((binding) => {
        const factory = binding.resource.schedules
        return factory === undefined ? [] : [{ name: binding.name, token: scheduleToken(binding.name), layer: factory(binding.name) }]
      })
      const scheduleNames = scheduleBindings.map((binding) => binding.name)
      for (const entry of this.scheduledDefinitions) {
        if (!scheduleNames.includes(entry.registered.identity.connection)) throw new MqScheduleException('unavailable', 'Declared schedules require an explicitly enabled schedule store')
      }
      const scheduleStores = Layer.merge(...scheduleBindings.map((binding) => binding.layer))
      const scheduleAliases = Layer.merge(...scheduleNames.map(scheduleAlias))
      const registries = scheduleRegistries(scheduleNames, this.scheduledDefinitions, this.scheduleOptions.group)
      this.hasScheduler = this.schedulerEnabled && scheduleBindings.length > 0
      const scheduler = schedulerLayer(registries, this.scheduleOptions, () => { this.schedulerErrors += 1 })
      const outboxes = Layer.merge`)
  replace('Layer.merge(ClockLive, stores, operations, workers, outboxes, publisher)', 'Layer.merge(ClockLive, stores, operations, workers, outboxes, publisher, scheduleStores, scheduleAliases, scheduler)')
  replace('      const readyOutboxes = new Map<string, OutboxStore>()', `      const readySchedules = new Map<string, JobScheduleStoreContract>()
      for (const binding of scheduleBindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() => Effect.gen(async function* () { return Result.ok(yield* binding.token) }))
        if (Result.isError(resolved)) throw new MqScheduleException('unavailable', 'Schedule store acquisition failed', { cause: resolved.error })
        const descriptor = resolved.value.descriptor
        if (descriptor.extension !== 'better-effect-mq/schedules' || descriptor.extensionVersion !== 1 || descriptor.jobStoreProtocolVersion !== 1) throw new MqScheduleException('unavailable', 'Incompatible schedule store protocol')
        const probe = await runtime.run(async () => ({ value: await resolved.value.listSchedules({ limit: 1 }) }))
        if (Result.isError(probe.value)) throw new MqScheduleException('unavailable', 'Schedule store probe failed', { cause: probe.value.error })
        readySchedules.set(binding.name, resolved.value)
      }
      const readyOutboxes = new Map<string, OutboxStore>()`)
  replace('      this.readyOutboxes = readyOutboxes', '      this.readyOutboxes = readyOutboxes\n      this.readySchedules = readySchedules')
  replace('        cause instanceof MqOutboxException', '        cause instanceof MqScheduleException ||\n        cause instanceof MqOutboxException')
  const anchor = '  /** Called after clients are bound; lazy Worker layers still belong to the same runtime. */'
  replace(anchor, `  scheduleSources(): readonly string[] { return Object.freeze([...this.readySchedules.keys()]) }
  async withSchedules<Value>(name: string, operation: (store: JobScheduleStoreContract) => Value | PromiseLike<Value>): Promise<Value> {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined) throw new MqScheduleException('unavailable', 'The MQ engine is not ready')
    const store = this.readySchedules.get(name)
    if (store === undefined) throw new MqScheduleException('unavailable', 'This connection has no enabled schedule store')
    const result = await runtime.run(async () => ({ value: await operation(store) }))
    return result.value
  }
  async reconcileSchedules(definition: ScheduleRegistry) {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined) throw new MqScheduleException('unavailable', 'The MQ engine is not ready')
    const result = await runtime.run(() => Effect.gen(async function* () { return Result.ok(yield* JobSchedules.reconcile(definition, { removal: 'warn' })) }))
    if (Result.isError(result)) throw new MqScheduleException('operation', 'Schedule reconciliation failed', { cause: result.error })
    return result.value
  }
  scheduler(): SchedulerSnapshot | undefined {
    const handle = this.runningScheduler
    return handle === undefined ? undefined : Object.freeze({ state: handle.state, activeTickCount: handle.activeTickCount, reportedErrors: this.schedulerErrors })
  }
  async sweepSchedules(): Promise<void> {
    if (this.state !== 'ready' || this.runningScheduler === undefined || this.runtime === undefined) throw new MqScheduleException('unavailable', 'No running scheduler is enabled in this application')
    const handle = this.runningScheduler
    const errors = this.schedulerErrors
    await this.runtime.run(async () => { await handle.sweep() })
    if (errors !== this.schedulerErrors) throw new MqScheduleException('operation', 'One or more scheduler operations failed during the sweep')
  }
  activateScheduler(): Promise<void> {
    this.scheduling ??= this.startScheduler()
    return this.scheduling
  }
  private async startScheduler(): Promise<void> {
    if (!this.hasScheduler) return
    const runtime = this.runtime
    if (runtime === undefined || this.state !== 'ready') throw new MqScheduleException('unavailable', 'Cannot start a scheduler before storage is ready')
    this.assertStarting()
    const result = await runtime.run(() => Effect.gen(async function* () { return Result.ok(yield* Scheduler) }))
    if (Result.isError(result)) throw new MqScheduleException('unavailable', 'Scheduler activation failed', { cause: result.error })
    this.runningScheduler = result.value
    this.assertStarting()
  }

${anchor}`)
  replace('      if (this.publishing !== undefined) await Promise.allSettled([this.publishing])', '      if (this.publishing !== undefined) await Promise.allSettled([this.publishing])\n      if (this.scheduling !== undefined) await Promise.allSettled([this.scheduling])')
  replace('      this.readyOutboxes.clear()', '      this.readyOutboxes.clear()\n      this.readySchedules.clear()\n      this.runningScheduler = undefined')
  writeFileSync(file, text)
}
const test = 'tests/integration/schedules.test.ts'
text = readFileSync(test, 'utf8')
if (!text.includes('JSON.parse(\'{"text":1}\')')) writeFileSync(test, text.replace('payload: { text: 1 }', 'payload: JSON.parse(\'{"text":1}\')'))
