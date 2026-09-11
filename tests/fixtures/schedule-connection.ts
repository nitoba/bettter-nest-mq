import { Layer } from 'better-effect'
import { MemoryJobStore, MemoryJobScheduleStore } from 'better-effect-mq'
import { defineConnection } from '../../src/engine/connection-definition.ts'
import { scheduleToken } from '../../src/engine/schedule-plan.ts'

/** Explicit real upstream reference stores, never a production fallback. */
export function scheduleConnection(enabled = true) {
  const jobs = MemoryJobStore.make()
  const schedules = MemoryJobScheduleStore.make({ jobStore: jobs })
  const trace = { acquired: 0, released: 0 }
  const connection = defineConnection({ adapter: 'memory', ownership: 'borrowed', boundary: jobs, scope: 'schedule-tests' }, (token) => {
    const layer = Layer.scoped(token, () => { trace.acquired += 1; return jobs }, () => { trace.released += 1 })
    return enabled ? { layer, schedules: (name: string) => Layer.succeed(scheduleToken(name), schedules) } : { layer }
  })
  return { connection, jobs, schedules, trace }
}
