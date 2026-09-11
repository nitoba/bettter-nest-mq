import { expect, test } from 'bun:test'
import * as mq from '../../src/index.ts'

test('exposes durable Nest flow phases and typed plans without engine tokens', () => {
  for (const name of [
    'Flow',
    'FanOut',
    'Collect',
    'FlowData',
    'FlowChildren',
    'flowJob',
    'flowChildren',
    'MqFlowsService',
    'MqFlowException'
  ])
    expect(Object.keys(mq)).toContain(name)
  expect(Object.keys(mq)).not.toContain('FlowStore')
})
