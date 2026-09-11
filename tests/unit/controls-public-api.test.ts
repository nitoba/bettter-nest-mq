import { expect, test } from 'bun:test'
import * as mq from '../../src/index.ts'

test('exposes persistent queue controls without exporting engine internals', () => {
  expect(Object.keys(mq)).toContain('QueueControls')
  expect(Object.keys(mq)).toContain('MqQueueControlsService')
  expect(Object.keys(mq)).toContain('QueueControlsException')
  expect(Object.keys(mq)).not.toContain('ControlledJobStoreContract')
})
