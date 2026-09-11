import { Logger } from '@nestjs/common'
import { Layer } from 'better-effect'
import { JobScheduler, JobSchedules, JobScheduleStore } from 'better-effect-mq'
import type { JobScheduleStoreToken, JobScheduleStoreInstance, JobSchedulerServiceInstance, JobSchedulesDefinition, JobScheduleDraft } from 'better-effect-mq'
import type { MqScheduleOptions } from '../schedules/types.ts'
import { namedStoreToken } from './connection-definition.ts'
import type { NamedStoreToken, NamedStore } from './connection-definition.ts'
import { operationStoreToken } from './operation-store.ts'
import type { OperationStoreToken } from './operation-store.ts'
import type { CompiledJob } from './job-compiler.ts'
import type { CompiledSchedule } from './schedule-compiler.ts'

export type NamedScheduleToken = JobScheduleStoreToken<NamedStoreToken>
export type NamedSchedule = JobScheduleStoreInstance<NamedStoreToken>
export type OperationSchedule = JobScheduleStoreInstance<OperationStoreToken>
export type ScheduleLayerFactory = (name: string) => Layer<NamedSchedule, NamedStore>
export type ScheduleRegistry = JobSchedulesDefinition<string, readonly JobScheduleDraft<CompiledJob, string>[], readonly OperationStoreToken[]>
export type EngineScheduler = JobSchedulerServiceInstance<'nestjs/scheduler'>
export const Scheduler = JobScheduler.service('nestjs/scheduler')
export function scheduleToken(name: string): NamedScheduleToken { return JobScheduleStore.for(namedStoreToken(name)) }
export function scheduleAlias(name: string): Layer<OperationSchedule, NamedSchedule> {
  return Layer.gen(JobScheduleStore.for(operationStoreToken(name)), async function* () { return yield* scheduleToken(name) })
}
export function scheduleRegistries(names: readonly string[], compiled: readonly CompiledSchedule[], group: string): readonly ScheduleRegistry[] {
  const registries: ScheduleRegistry[] = []
  for (const name of names) {
    const groups = new Set([group, ...compiled.filter((entry) => entry.registered.identity.connection === name).map((entry) => entry.schedule.group)])
    for (const current of groups) registries.push(JobSchedules.define({ group: current, schedules: [], stores: [operationStoreToken(name)] }))
  }
  return registries
}
export function declarationRegistry(entry: CompiledSchedule): ScheduleRegistry {
  // Rebuild an upstream draft, rather than treating a grouped descriptor as a new draft.
  const s = entry.schedule
  const options = { payload: s.payload, metadata: s.metadata, defaults: s.defaults, timeZone: s.timeZone, misfire: s.misfire, overlap: s.overlap }
  const draft = s.job.schedule(s.key, s.cron === undefined ? { ...options, everyMs: s.everyMs! } : { ...options, cron: s.cron })
  return JobSchedules.define({ group: s.group, schedules: [draft], stores: [s.job.store] })
}
export function schedulerLayer(registries: readonly ScheduleRegistry[], options: Required<MqScheduleOptions>, onError: () => void) {
  const logger = new Logger('BetterNestMqScheduler')
  return Scheduler.layer(() => ({
    registries, sweepIntervalMs: options.sweepIntervalMs, batchSize: options.batchSize,
    maxStoreRetries: options.maxStoreRetries, retryDelayMs: options.retryDelayMs,
    startupReconcile: false,
    onError: () => { onError(); logger.warn('A scheduler operation failed; inspect schedule state and connection health') }
  }))
}
