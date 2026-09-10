import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Layer } from 'better-effect'
import { MemoryJobStore } from 'better-effect-mq'

import { MqEngineStateException } from '../../src/index.ts'
import { defineConnection } from '../../src/engine/connection-definition.ts'
import { EngineSession } from '../../src/engine/engine-session.ts'

// The barriers prove close races an acquisition already executing inside the real Runtime,
// rather than merely cancelling before the descriptor factory has been entered.
test('rolls back an acquisition that finishes after close was requested', async () => {
  const entered = Promise.withResolvers<void>()
  const gate = Promise.withResolvers<void>()
  const releases: string[] = []
  const connection = defineConnection({
    adapter: 'memory', ownership: 'owned', boundary: entered, scope: 'racing-acquisition'
  }, (token) => ({
    layer: Layer.scoped(token, async () => {
      entered.resolve()
      await gate.promise
      return MemoryJobStore.make()
    }, () => { releases.push('layer') }),
    release: async () => { releases.push('resource') }
  }))
  const session = new EngineSession({ primary: connection }, { gracePeriodMs: 0 })
  const starting = session.start([])
  const rejected = assert.rejects(starting, MqEngineStateException)
  await entered.promise
  const closing = session.close()
  expect(session.state).toBe('stopping')
  expect(session.connections()).toEqual([])
  gate.resolve()
  await Promise.all([rejected, closing])
  expect(session.state).toBe('closed')
  expect(releases).toEqual(['layer', 'resource'])
})
