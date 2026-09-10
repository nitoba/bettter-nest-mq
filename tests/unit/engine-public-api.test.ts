import { expect, test } from 'bun:test'

import * as mq from '../../src/index.ts'

test('exposes connection readiness and lifecycle failures without exporting an Effect runtime', () => {
  for (const name of ['MqConnectionsService', 'MqConnectionException', 'MqEngineStateException']) {
    expect(Object.keys(mq)).toContain(name)
  }
  expect(Object.keys(mq)).not.toContain('MqEngineHost')
})
