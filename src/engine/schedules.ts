import type { Type } from '@nestjs/common'
import { ModuleRef } from '@nestjs/core'
import type { ScheduleRecord } from 'better-effect-mq'
import { Result } from 'better-result'
import { isDeepStrictEqual } from 'node:util'
import { MqScheduleException } from '../schedules/errors.ts'
import { scheduleName } from '../schedules/decorator.ts'
import type {
  MqScheduleOptions,
  ScheduleOptions,
  ScheduleSnapshot,
  ScheduleListOptions,
  ScheduleReport,
  SchedulesMonitor
} from '../schedules/types.ts'
import { JobContract } from '../contracts/job-definition.ts'
import { requireInteger } from '../contracts/policies.ts'
import type { QueueService } from '../contracts/queue-service.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import type { MqRegistry } from '../module/mq.registry.ts'
import type { EngineSession } from './engine-session.ts'
import { compileSchedule, compileSchedules, type CompiledSchedule } from './schedule-compiler.ts'
import { declarationRegistry } from './schedule-plan.ts'
import { controlledStore } from './controlled-store.ts'

function valueOf<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result))
    throw new MqScheduleException('operation', 'Schedule storage operation failed', {
      cause: result.error
    })
  return result.value
}
function snapshot(connection: string, record: ScheduleRecord): ScheduleSnapshot {
  return Object.freeze({
    ...record,
    connection,
    job: Object.freeze({ ...record.job }),
    metadata: Object.freeze({ ...record.metadata }),
    misfire: Object.freeze({ ...record.misfire })
  })
}
function assertIdentity(record: ScheduleRecord, registered: RegisteredJob): void {
  const wanted = registered.identity
  if (
    record.job.queue !== wanted.queue ||
    record.job.name !== wanted.name ||
    record.job.version !== wanted.version
  )
    throw new MqScheduleException(
      'identity',
      'The schedule address belongs to another job contract'
    )
}
function matches(record: ScheduleRecord, entry: CompiledSchedule): boolean {
  const desired = entry.schedule
  return (
    record.cron === desired.cron &&
    record.everyMs === desired.everyMs &&
    record.timeZone === desired.timeZone &&
    record.priority === desired.defaults.priority &&
    record.attemptsMax === desired.defaults.attempts &&
    record.timeoutMs === desired.defaults.timeoutMs &&
    isDeepStrictEqual(record.backoff, desired.defaults.backoff) &&
    isDeepStrictEqual(record.payload, entry.encoded) &&
    isDeepStrictEqual(record.metadata, desired.metadata) &&
    isDeepStrictEqual(record.misfire, desired.misfire) &&
    record.overlap === desired.overlap
  )
}

export class SchedulesCoordinator implements SchedulesMonitor {
  private definitions: readonly CompiledSchedule[] = []
  constructor(
    private readonly session: EngineSession,
    private readonly registry: MqRegistry,
    private readonly moduleRef: ModuleRef,
    private readonly options: Required<MqScheduleOptions>
  ) {}

  async prepare(): Promise<readonly CompiledSchedule[]> {
    this.definitions = await compileSchedules(this.registry.jobs(), this.options)
    return this.definitions
  }
  private registered(queue: Type<QueueService>, property: string): RegisteredJob {
    let instance: QueueService
    try {
      instance = this.moduleRef.get(queue, { strict: false })
    } catch (cause) {
      throw new MqScheduleException('identity', 'The queue Service is not registered', { cause })
    }
    const contract = Object.getOwnPropertyDescriptor(instance, property)?.value
    const registered =
      contract instanceof JobContract
        ? this.registry.jobs().find((job) => job.contract === contract)
        : undefined
    if (registered === undefined)
      throw new MqScheduleException('identity', 'The referenced property is not a registered job')
    return registered
  }
  private group(registered: RegisteredJob, requested?: string): string {
    const group = scheduleName(requested ?? this.options.group, 'schedule.group')
    if (
      group !== this.options.group &&
      !this.definitions.some(
        (entry) =>
          entry.registered.identity.connection === registered.identity.connection &&
          entry.schedule.group === group
      )
    )
      throw new MqScheduleException(
        'definition',
        'Dynamic schedules must use the default or an explicitly declared scheduler group'
      )
    return group
  }
  private async assertStorePolicy(
    connection: string,
    record: Pick<ScheduleRecord, 'queue'>
  ): Promise<void> {
    await this.session.withStore(connection, async (store) => {
      const extension = controlledStore(store)
      if (extension === undefined) return
      const controls = valueOf(await extension.getControls({ queue: record.queue }))
      if (controls?.enabled && controls.perKeyConcurrency !== undefined)
        throw new MqScheduleException(
          'definition',
          'The schedule protocol cannot target a persisted per-key-limited queue without dispatch keys'
        )
    })
  }
  async initialize(): Promise<void> {
    // Inspect persisted records in every served group, including declarations omitted by this release.
    for (const connection of this.session.scheduleSources()) {
      for (const group of new Set([
        this.options.group,
        ...this.definitions
          .filter((entry) => entry.registered.identity.connection === connection)
          .map((entry) => entry.schedule.group)
      ])) {
        const records = await this.session.withSchedules(connection, async (store) =>
          valueOf(await store.listSchedules({ group }))
        )
        for (const record of records) await this.assertStorePolicy(connection, record)
      }
    }
    await this.synchronize(this.options.mode)
  }
  private async preflight(entry: CompiledSchedule, validate: boolean): Promise<void> {
    const connection = entry.registered.identity.connection
    await this.assertStorePolicy(connection, { queue: entry.schedule.job.identity.queue })
    const record = await this.session.withSchedules(connection, async (store) =>
      valueOf(await store.getSchedule({ group: entry.schedule.group, key: entry.schedule.key }))
    )
    if (record !== undefined) assertIdentity(record, entry.registered)
    if (validate && record === undefined)
      throw new MqScheduleException(
        'missing',
        'Deploy the declared schedule before starting a validating replica'
      )
    if (validate && record !== undefined && !matches(record, entry))
      throw new MqScheduleException(
        'drift',
        'A schedule declaration differs from its persisted definition'
      )
  }
  private async synchronize(mode: 'validate' | 'reconcile'): Promise<readonly ScheduleReport[]> {
    for (const entry of this.definitions) await this.preflight(entry, mode === 'validate')
    if (mode === 'validate') return Object.freeze([])
    const reports: ScheduleReport[] = []
    for (const entry of this.definitions) {
      const report = await this.session.reconcileSchedules(declarationRegistry(entry))
      reports.push(
        Object.freeze({
          connection: entry.registered.identity.connection,
          group: entry.schedule.group,
          created: Object.freeze(report.created.map((record) => record.key)),
          updated: Object.freeze(report.updated.map((record) => record.key)),
          unchanged: Object.freeze(report.unchanged.map((record) => record.key))
        })
      )
    }
    return Object.freeze(reports)
  }
  async reconcile(): Promise<readonly ScheduleReport[]> {
    if (this.options.mode !== 'reconcile')
      throw new MqScheduleException(
        'mode',
        'This application validates schedule definitions read-only'
      )
    return this.synchronize('reconcile')
  }
  async get(
    queue: Type<QueueService>,
    property: string,
    key: string,
    group?: string
  ): Promise<ScheduleSnapshot | undefined> {
    const job = this.registered(queue, property)
    const address = { group: this.group(job, group), key: scheduleName(key, 'schedule.key') }
    const record = await this.session.withSchedules(job.identity.connection, async (store) =>
      valueOf(await store.getSchedule(address))
    )
    if (record === undefined) return undefined
    assertIdentity(record, job)
    return snapshot(job.identity.connection, record)
  }
  async list(
    queue: Type<QueueService>,
    property: string,
    options: ScheduleListOptions = {}
  ): Promise<readonly ScheduleSnapshot[]> {
    const job = this.registered(queue, property)
    const group = this.group(job, options.group)
    if (options.limit !== undefined) requireInteger(options.limit, 'schedule.limit', 1)
    const records = await this.session.withSchedules(job.identity.connection, async (store) =>
      valueOf(await store.listSchedules({ group }))
    )
    const matched = records.filter(
      (record) =>
        record.job.queue === job.identity.queue &&
        record.job.name === job.identity.name &&
        record.job.version === job.identity.version &&
        (options.paused === undefined || options.paused === record.paused)
    )
    return Object.freeze(
      matched.slice(0, options.limit).map((record) => snapshot(job.identity.connection, record))
    )
  }
  async upsert<Input>(
    queue: Type<QueueService>,
    property: string,
    options: ScheduleOptions<Input>
  ): Promise<ScheduleSnapshot> {
    if (this.options.mode !== 'reconcile')
      throw new MqScheduleException(
        'mode',
        'Schedule definition writes require explicit reconcile mode'
      )
    const job = this.registered(queue, property)
    const group = this.group(job, options.group)
    const entry = await compileSchedule(job, { ...options, group }, this.options)
    await this.preflight(entry, false)
    const report = await this.session.reconcileSchedules(declarationRegistry(entry))
    const record = [...report.created, ...report.updated, ...report.unchanged][0]
    if (record === undefined)
      throw new MqScheduleException('operation', 'Schedule upsert did not return a record')
    return snapshot(job.identity.connection, record)
  }
  private async mutate(
    queue: Type<QueueService>,
    property: string,
    key: string,
    group: string | undefined,
    operation: 'pause' | 'resume' | 'remove'
  ): Promise<boolean> {
    const job = this.registered(queue, property)
    const address = { group: this.group(job, group), key: scheduleName(key, 'schedule.key') }
    return this.session.withSchedules(job.identity.connection, async (store) => {
      const record = valueOf(await store.getSchedule(address))
      if (record === undefined) {
        if (operation === 'remove') return false
        throw new MqScheduleException('missing', 'The schedule does not exist')
      }
      assertIdentity(record, job)
      if (operation === 'remove') return valueOf(await store.removeSchedule(address))
      valueOf(
        await (operation === 'pause' ? store.pauseSchedule(address) : store.resumeSchedule(address))
      )
      return true
    })
  }
  async pause(
    queue: Type<QueueService>,
    property: string,
    key: string,
    group?: string
  ): Promise<void> {
    await this.mutate(queue, property, key, group, 'pause')
  }
  async resume(
    queue: Type<QueueService>,
    property: string,
    key: string,
    group?: string
  ): Promise<void> {
    await this.mutate(queue, property, key, group, 'resume')
  }
  remove(
    queue: Type<QueueService>,
    property: string,
    key: string,
    group?: string
  ): Promise<boolean> {
    return this.mutate(queue, property, key, group, 'remove')
  }
  scheduler() {
    return this.session.scheduler()
  }
  sweep() {
    return this.session.sweepSchedules()
  }
}
