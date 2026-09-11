import { expect, test } from 'bun:test'
import * as mq from '../../src/index.ts'

test('exposes durable schedule declarations and Nest administration without engine types', () => {
  for (const name of ['Schedule', 'MqSchedulesService', 'MqScheduleException']) expect(Object.keys(mq)).toContain(name)
  expect(Object.keys(mq)).not.toContain('JobScheduler')
})
