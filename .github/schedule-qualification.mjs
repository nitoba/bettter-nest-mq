import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
function replace(path, before, after) {
  const text = readFileSync(path, 'utf8')
  if (text.includes(after)) return
  assert.equal(text.split(before).length - 1, 1, `Expected one test anchor in ${path}: ${before}`)
  writeFileSync(path, text.replace(before, after))
}
replace(
  'tests/integration/schedules.test.ts',
  'missing opt-in schedule resource fails bootstrap and releases already acquired resources',
  'missing opt-in schedule resource fails before scoped store acquisition'
)
replace(
  'tests/integration/schedules.test.ts',
  'expect(fixture.trace.released).toBe(1)',
  'expect(fixture.trace).toEqual({ acquired: 0, released: 0 })'
)
replace(
  'tests/integration/schedules.test.ts',
  '    const set = await fixture.schedules.upsertSchedule({',
  `    // An unchanged upsert intentionally preserves the cursor. Seed a fresh overdue record
    // in this isolated reference fixture instead of expecting upsert to reset runtime state.
    const removed = await fixture.schedules.removeSchedule({ group: 'nestjs/schedules', key: 'regular' })
    if (Result.isError(removed)) throw removed.error
    const set = await fixture.schedules.upsertSchedule({`
)
replace('scripts/test-package.ts', "      'outbox'\n", "      'outbox',\n      'schedules'\n")
