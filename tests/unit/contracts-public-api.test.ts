import { expect, test } from 'bun:test'

import * as mq from '../../src/index.ts'

test('exposes the approved typed contract and Nest registration primitives', () => {
  for (const name of [
    'QueueService',
    'Queue',
    'Job',
    'Retry',
    'JobTimeout',
    'MqRegistry',
    'defineCodec',
    'JobFailureException'
  ]) {
    expect(Object.keys(mq)).toContain(name)
  }
})
