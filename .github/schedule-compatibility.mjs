import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
function replace(path, before, after) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(after)) return
  assert.equal(text.split(before).length - 1, 1, `Missing compatibility anchor: ${path}: ${before}`)
  writeFileSync(path, text.replace(before, after))
}
replace('src/engine/engine-session.ts', 'JobSchedules.reconcile(definition,', 'JobSchedules.reconcile<ScheduleRegistry>(definition,')
replace('src/engine/schedule-plan.ts', 's.job.schedule(', 'JobSchedules.schedule(s.job, ')
replace('src/engine/schedules.ts', "import type { ScheduleRecord } from 'better-effect-mq'", "import type { ScheduleRecord } from 'better-effect-mq'\nimport { makeQueueName } from 'better-effect-mq'")
replace('src/engine/schedules.ts', "record: Pick<ScheduleRecord, 'queue'>", 'record: { readonly queue: string }')
replace('src/engine/schedules.ts', 'extension.getControls({ queue: record.queue })', 'extension.getControls({ queue: valueOf(makeQueueName(record.queue)) })')
const pg = 'src/integrations/postgres.ts'
const text = readFileSync(pg, 'utf8')
writeFileSync(pg, text.replaceAll('          options.schedules ?? false\n', '          schedules\n'))
