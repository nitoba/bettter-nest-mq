import 'reflect-metadata'
import { isDeepStrictEqual } from 'node:util'
import { parseCron, makeEveryMs, isValidTimeZone, maxScheduleIdentityLength } from 'better-effect-mq'
import { requireInteger, requireName, copyRetry } from '../contracts/policies.ts'
import { QueueService } from '../contracts/queue-service.ts'
import type { JobJsonValue } from '../jobs/types.ts'
import { MqScheduleException } from './errors.ts'
import type { MqScheduleOptions, ScheduleOptions } from './types.ts'

const SCHEDULES = Symbol('mq.schedules')
export function scheduleName(value: string, field: string): string {
  requireName(value, field)
  if (value.length > maxScheduleIdentityLength || value.includes('\0')) throw new MqScheduleException('definition', `${field} exceeds the schedule identity limit or contains NUL`)
  return value
}
function freezeJson(value: JobJsonValue): void {
  if (value instanceof Object) {
    for (const child of Object.values(value)) freezeJson(child)
    Object.freeze(value)
  }
}
export function copySchedule<Input>(options: ScheduleOptions<Input>): ScheduleOptions<Input> {
  try {
    scheduleName(options.key, 'schedule.key')
    if (options.group !== undefined) scheduleName(options.group, 'schedule.group')
    if ((options.cron === undefined) === (options.everyMs === undefined)) throw new Error('Exactly one of cron or everyMs is required')
    if (options.cron !== undefined) parseCron(options.cron)
    else makeEveryMs(options.everyMs)
    if (options.timeZone !== undefined && !isValidTimeZone(options.timeZone)) throw new Error('A valid IANA timezone is required')
    if (options.priority !== undefined) requireInteger(options.priority, 'schedule.priority')
    if (options.timeoutMs !== undefined) requireInteger(options.timeoutMs, 'schedule.timeoutMs', 1)
    if (options.overlap !== undefined && options.overlap !== 'skip' && options.overlap !== 'allow') throw new Error('Invalid overlap policy')
    if (options.misfire !== undefined) {
      if (options.misfire.strategy === 'catch-up') requireInteger(options.misfire.maxOccurrences, 'misfire.maxOccurrences', 1)
      else if (options.misfire.strategy !== 'skip' && options.misfire.strategy !== 'run-once') throw new Error('Invalid misfire policy')
    }
    const text = JSON.stringify(options.payload)
    if (text === undefined) throw new Error('Schedule payload must be JSON input')
    const payload = JSON.parse(text)
    if (!isDeepStrictEqual(payload, options.payload)) throw new Error('Schedule input must have a lossless JSON representation')
    freezeJson(payload)
    let result: ScheduleOptions<Input> = { ...options, payload }
    if (options.retry !== undefined) result = { ...result, retry: copyRetry(options.retry) }
    if (options.metadata !== undefined) result = { ...result, metadata: Object.freeze({ ...options.metadata }) }
    if (options.misfire !== undefined) result = { ...result, misfire: Object.freeze({ ...options.misfire }) }
    return Object.freeze(result)
  } catch (cause) {
    if (cause instanceof MqScheduleException) throw cause
    throw new MqScheduleException('definition', 'Invalid schedule declaration', { cause })
  }
}

export function Schedule<Input extends JobJsonValue>(options: ScheduleOptions<Input>): PropertyDecorator {
  const copied = copySchedule(options)
  return (target, key) => {
    if (!(target instanceof QueueService) || key !== String(key)) throw new MqScheduleException('definition', '@Schedule requires a string-named QueueService job property')
    const own: ReadonlyMap<string, readonly ScheduleOptions[]> = Reflect.getOwnMetadata(SCHEDULES, target) ?? new Map()
    const previous = own.get(String(key)) ?? []
    if (previous.some((entry) => entry.key === copied.key && entry.group === copied.group)) throw new MqScheduleException('definition', 'Duplicate schedule decorator address')
    const next = new Map(own)
    next.set(String(key), Object.freeze([...previous, copied]))
    Reflect.defineMetadata(SCHEDULES, next, target)
  }
}
export function scheduleMetadata(queue: QueueService, property: string): readonly ScheduleOptions[] {
  let prototype = Object.getPrototypeOf(queue)
  while (prototype !== null) {
    const map: ReadonlyMap<string, readonly ScheduleOptions[]> | undefined = Reflect.getOwnMetadata(SCHEDULES, prototype)
    const schedules = map?.get(property)
    if (schedules !== undefined) return schedules
    prototype = Object.getPrototypeOf(prototype)
  }
  return Object.freeze([])
}
export function assertScheduleProperties(queue: QueueService, properties: ReadonlySet<string>): void {
  let prototype = Object.getPrototypeOf(queue)
  while (prototype !== null) {
    const map: ReadonlyMap<string, readonly ScheduleOptions[]> | undefined = Reflect.getOwnMetadata(SCHEDULES, prototype)
    for (const property of map?.keys() ?? []) {
      if (!properties.has(property)) throw new MqScheduleException('definition', `@Schedule on ${property} has no registered @Job`)
    }
    prototype = Object.getPrototypeOf(prototype)
  }
}
export function resolveScheduleOptions(options: MqScheduleOptions = {}): Readonly<Required<MqScheduleOptions>> {
  const resolved = {
    mode: options.mode ?? 'validate', group: options.group ?? 'nestjs/schedules',
    sweepIntervalMs: options.sweepIntervalMs ?? 1_000, batchSize: options.batchSize ?? 100,
    maxStoreRetries: options.maxStoreRetries ?? 3, retryDelayMs: options.retryDelayMs ?? 25
  }
  if (resolved.mode !== 'validate' && resolved.mode !== 'reconcile') throw new MqScheduleException('definition', 'schedules.mode must be validate or reconcile')
  scheduleName(resolved.group, 'schedules.group')
  requireInteger(resolved.sweepIntervalMs, 'schedules.sweepIntervalMs', 1)
  requireInteger(resolved.batchSize, 'schedules.batchSize', 1)
  requireInteger(resolved.maxStoreRetries, 'schedules.maxStoreRetries')
  requireInteger(resolved.retryDelayMs, 'schedules.retryDelayMs')
  return Object.freeze(resolved)
}
