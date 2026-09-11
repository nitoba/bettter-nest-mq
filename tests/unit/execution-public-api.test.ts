import { expect, test } from 'bun:test'
import * as mq from '../../src/index.ts'

test('exposes real producer and decorated worker primitives without exposing the engine', () => {
  for (const name of [
    'Worker',
    'Process',
    'JobData',
    'JobContext',
    'MqWorkersService',
    'MqJobException',
    'JobWaitTimeoutException'
  ]) {
    expect(Object.keys(mq)).toContain(name)
  }
  expect(Object.keys(mq)).not.toContain('Runtime')
})
