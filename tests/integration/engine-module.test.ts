import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'

import { Job, MqConnectionException, MqConnectionsService, MqModule, MqRegistry, Queue, QueueService } from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

@Queue({ name: 'connected', connection: 'primary' })
class ConnectedQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}

test('Nest starts the real engine after contract validation and releases it on close', async () => {
  const fixture = memoryConnection()
  const connections = { primary: fixture.connection }
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ connections }), MqModule.forFeature([ConnectedQueue])] }).compile()
  expect(fixture.trace.acquisitions).toBe(0)
  expect(Object.isFrozen(connections)).toBe(false)
  try {
    await app.init()
    expect(app.get(MqRegistry).jobs()).toHaveLength(1)
    expect(app.get(MqConnectionsService).state).toBe('ready')
    expect(await app.get(MqConnectionsService).probe('primary')).toMatchObject({ name: 'primary' })
  } finally {
    await app.close()
  }
  expect(fixture.trace.resourceReleases).toBe(1)
  expect(app.get(MqConnectionsService).state).toBe('closed')
})

test('contract-only Nest applications remain independent of storage', async () => {
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({}), MqModule.forFeature([ConnectedQueue])] }).compile()
  try {
    await app.init()
    expect(app.get(MqConnectionsService).state).toBe('disabled')
    expect(app.get(MqRegistry).jobs()).toHaveLength(1)
  } finally {
    await app.close()
  }
})

test('missing configured connections fail bootstrap without waiting for shutdown hooks to clean up', async () => {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({ connections: { wrong: fixture.connection } }), MqModule.forFeature([ConnectedQueue])] }).compile()
  await assert.rejects(app.init(), MqConnectionException)
  expect(fixture.trace.acquisitions).toBe(0)
  expect(app.get(MqConnectionsService).connections()).toEqual([])
  await assert.rejects(app.close(), MqConnectionException)
})
