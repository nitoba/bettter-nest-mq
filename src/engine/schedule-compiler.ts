import { JobSchedules } from 'better-effect-mq'
import type { JobSchedule, JobScheduleDraft, JobScheduleOptions } from 'better-effect-mq'
import { MqScheduleException } from '../schedules/errors.ts'
import { copySchedule, resolveScheduleOptions } from '../schedules/decorator.ts'
import type { MqScheduleOptions, ScheduleOptions } from '../schedules/types.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { validateSchema, encodeSchema } from '../contracts/schema.ts'
import type { JobJsonValue } from '../jobs/types.ts'
import { compileJob, compileBackoff, type CompiledJob } from './job-compiler.ts'

export type CompiledScheduleRecord = JobSchedule<CompiledJob, string, string>
export interface CompiledSchedule {
  readonly draft: JobScheduleDraft<CompiledJob, string>
  readonly registered: RegisteredJob
  readonly schedule: CompiledScheduleRecord
  readonly encoded: JobJsonValue
}
export async function compileSchedule<Input>(
  registered: RegisteredJob,
  supplied: ScheduleOptions<Input>,
  config: MqScheduleOptions = {}
): Promise<CompiledSchedule> {
  const options = copySchedule(supplied)
  const group = options.group ?? resolveScheduleOptions(config).group
  const contract = registered.contract
  const payload = await validateSchema(contract.schemas.payload, options.payload)
  const encoded: JobJsonValue = JSON.parse(await encodeSchema(contract.schemas.payload, payload))
  if (
    registered.controls?.perKeyConcurrency !== undefined ||
    contract.getDispatchKey(payload) !== undefined
  )
    throw new MqScheduleException(
      'definition',
      'The pinned scheduler cannot persist dispatch keys; schedule a coordinator job for per-key work'
    )
  if (registered.policy.delayMs !== 0)
    throw new MqScheduleException(
      'definition',
      'A scheduled job cannot inherit a relative enqueue delay; its cadence defines occurrence time'
    )
  const retry = options.retry ?? registered.policy.retry
  const job = compileJob(registered)
  const common: Omit<JobScheduleOptions<CompiledJob>, 'cron' | 'everyMs'> = {
    payload,
    metadata: options.metadata ?? {},
    attempts: retry.attempts,
    backoff: compileBackoff(retry),
    priority: options.priority ?? registered.policy.priority,
    misfire: options.misfire ?? { strategy: 'run-once' },
    overlap: options.overlap ?? 'allow',
    timeZone: options.timeZone ?? 'UTC'
  }
  const timeoutMs = options.timeoutMs ?? registered.policy.timeoutMs
  let cadence: JobScheduleOptions<CompiledJob> =
    options.cron === undefined
      ? { ...common, everyMs: options.everyMs }
      : { ...common, cron: options.cron }
  if (timeoutMs !== undefined) cadence = { ...cadence, timeoutMs }
  try {
    const draft = JobSchedules.schedule(job, options.key, cadence)
    const registry = JobSchedules.define({ group, schedules: [draft] })
    const schedule = registry.schedules[0]
    if (schedule === undefined)
      throw new MqScheduleException('definition', 'Schedule registry contains no declaration')
    return { registered, schedule, encoded, draft }
  } catch (cause) {
    if (cause instanceof MqScheduleException) throw cause
    throw new MqScheduleException('definition', 'Schedule compilation failed', { cause })
  }
}
export async function compileSchedules(
  jobs: readonly RegisteredJob[],
  options: MqScheduleOptions = {}
): Promise<readonly CompiledSchedule[]> {
  const compiled: CompiledSchedule[] = []
  const keys = new Set<string>()
  for (const registered of jobs) {
    for (const definition of registered.schedules ?? []) {
      const item = await compileSchedule(registered, definition, options)
      const key = JSON.stringify([
        registered.identity.connection,
        item.schedule.group,
        item.schedule.key
      ])
      if (keys.has(key))
        throw new MqScheduleException(
          'definition',
          'Schedule group/key must be unique within a connection'
        )
      keys.add(key)
      compiled.push(item)
    }
  }
  return Object.freeze(compiled)
}
