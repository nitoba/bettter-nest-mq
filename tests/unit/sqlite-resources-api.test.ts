import { expect, test } from 'bun:test'
import { sqlite } from '../../src/integrations/sqlite-bun.ts'

for (const feature of ['schedules', 'events']) {
  test(`SQLite ${feature} is an explicit inert resource option`, () => {
    const connection = sqlite({ path: './jobs.db', [feature]: true })
    expect(connection.adapter).toBe('sqlite')
    expect(connection.ownership).toBe('owned')
  })
}
