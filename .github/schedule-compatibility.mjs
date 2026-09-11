import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
function replace(path, before, after) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(after)) return
  assert.equal(text.split(before).length - 1, 1, `Missing compatibility anchor: ${path}: ${before}`)
  writeFileSync(path, text.replace(before, after))
}
replace(
  'src/engine/engine-session.ts',
  'JobSchedules.reconcile<ScheduleRegistry>',
  'JobSchedules.reconcile<string, ScheduleDrafts, readonly OperationStoreToken[]>'
)
replace(
  'src/engine/engine-session.ts',
  '  ScheduleRegistry\n',
  '  ScheduleRegistry,\n  ScheduleDrafts\n'
)
replace(
  'src/engine/schedule-compiler.ts',
  'JobSchedule, JobScheduleOptions',
  'JobSchedule, JobScheduleDraft, JobScheduleOptions'
)
replace(
  'src/engine/schedule-compiler.ts',
  'export interface CompiledSchedule {',
  'export interface CompiledSchedule {\n  readonly draft: JobScheduleDraft<CompiledJob, string>'
)
replace(
  'src/engine/schedule-compiler.ts',
  'return { registered, schedule, encoded }',
  'return { registered, schedule, encoded, draft }'
)
replace(
  'src/schedules/decorator.ts',
  "requireInteger(resolved.maxStoreRetries, 'schedules.maxStoreRetries')",
  "requireInteger(resolved.maxStoreRetries, 'schedules.maxStoreRetries', 1)"
)
