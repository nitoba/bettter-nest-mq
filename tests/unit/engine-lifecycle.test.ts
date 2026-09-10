import { describe, expect, test } from 'bun:test'
import assert from 'node:assert/strict'

import { MqConnectionException, MqEngineStateException } from '../../src/index.ts'
import { EngineSession } from '../../src/engine/engine-session.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

const shutdown = { gracePeriodMs: 0, abortAfterGracePeriod: true }

describe('one real engine session and its resources', () => {
  test('preserves contract-only mode without creating stores', async () => {
    const session = new EngineSession(undefined, shutdown)
    expect(session.state).toBe('idle')
    await session.start([])
    expect(session.state).toBe('disabled')
    expect(session.connections()).toEqual([])
    await assert.rejects(session.probe('default'), MqEngineStateException)
    await session.close()
    expect(session.state).toBe('closed')
  })

  test('definitions are inert, all named stores become ready, and cleanup happens once in order', async () => {
    const first = memoryConnection()
    const second = memoryConnection()
    const session = new EngineSession({ first: first.connection, second: second.connection }, shutdown)
    expect(first.trace.acquisitions).toBe(0)
    expect(session.connections()).toEqual([])
    await Promise.all([session.start([]), session.start([])])
    expect(session.state).toBe('ready')
    expect(first.trace.acquisitions).toBe(1)
    expect(second.trace.acquisitions).toBe(1)
    const connections = session.connections()
    expect(connections.map((value) => value.name)).toEqual(['first', 'second'])
    expect(Object.isFrozen(connections)).toBe(true)
    expect(Object.isFrozen(connections[0]?.capabilities)).toBe(true)
    expect(await session.probe('first')).toMatchObject({ name: 'first', adapter: 'memory', ownership: 'owned' })
    const firstStore = await session.withStore('first', (store) => store)
    const secondStore = await session.withStore('second', (store) => store)
    expect(firstStore).not.toBe(secondStore)
    expect(await session.withStore('first', (store) => store)).toBe(firstStore)
    await Promise.all([session.close(), session.close()])
    expect(first.trace.events).toEqual(['acquire', 'layer-release', 'resource-release'])
    expect(second.trace.events).toEqual(['acquire', 'layer-release', 'resource-release'])
    expect(session.connections()).toEqual([])
    await assert.rejects(session.start([]), MqEngineStateException)
    await assert.rejects(session.probe('first'), MqEngineStateException)
  })

  test('checks queue connection references before acquiring anything', async () => {
    const fixture = memoryConnection()
    const session = new EngineSession({ available: fixture.connection }, shutdown)
    await assert.rejects(session.start([{ name: 'reports', connection: 'missing', jobs: [] }]), MqConnectionException)
    expect(fixture.trace.acquisitions).toBe(0)
    expect(fixture.trace.resourceReleases).toBe(0)
    expect(session.state).toBe('failed')
    await session.close()
  })

  test('rejects repeated physical descriptors under different names before acquisition', async () => {
    const fixture = memoryConnection()
    const session = new EngineSession({ first: fixture.connection, second: fixture.connection }, shutdown)
    await assert.rejects(session.start([]), MqConnectionException)
    expect(fixture.trace.acquisitions).toBe(0)
    await session.close()
  })

  test('rolls back all resource handles after a store acquisition fails', async () => {
    const first = memoryConnection()
    const cause = new Error('acquisition failed')
    const second = memoryConnection({ acquisitionFailure: cause })
    const session = new EngineSession({ first: first.connection, second: second.connection }, shutdown)
    await assert.rejects(session.start([]), MqConnectionException)
    expect(session.state).toBe('failed')
    expect(session.connections()).toEqual([])
    expect(first.trace.layerReleases).toBe(1)
    expect(first.trace.resourceReleases).toBe(1)
    expect(second.trace.resourceReleases).toBe(1)
    await session.close()
    expect(first.trace.resourceReleases).toBe(1)
  })

  test('unsupported capabilities fail closed and release the acquired store', async () => {
    const fixture = memoryConnection({ requirements: ['durableChangeFeed'] })
    const session = new EngineSession({ main: fixture.connection }, shutdown)
    await assert.rejects(session.start([]), MqConnectionException)
    expect(session.connections()).toEqual([])
    expect(fixture.trace.layerReleases).toBe(1)
    expect(fixture.trace.resourceReleases).toBe(1)
    await session.close()
  })

  test('does not become ready if close races a pending acquisition', async () => {
    const gate = Promise.withResolvers<void>()
    const fixture = memoryConnection({ beforeAcquire: gate.promise })
    const session = new EngineSession({ main: fixture.connection }, shutdown)
    const starting = session.start([])
    const rejected = assert.rejects(starting, MqEngineStateException)
    const closing = session.close()
    expect(session.state).toBe('stopping')
    gate.resolve()
    await Promise.all([rejected, closing])
    expect(session.state).toBe('closed')
    expect(session.connections()).toEqual([])
    expect(fixture.trace.resourceReleases).toBeLessThanOrEqual(1)
  })

  test('drains admitted operations before releasing adapter resources', async () => {
    const fixture = memoryConnection()
    const session = new EngineSession({ main: fixture.connection }, { gracePeriodMs: 1_000, abortAfterGracePeriod: false })
    await session.start([])
    const entered = Promise.withResolvers<void>()
    const gate = Promise.withResolvers<void>()
    const operation = session.withStore('main', async () => {
      entered.resolve()
      await gate.promise
      return 'completed'
    })
    await entered.promise
    const closing = session.close()
    expect(fixture.trace.resourceReleases).toBe(0)
    await assert.rejects(session.probe('main'), MqEngineStateException)
    gate.resolve()
    expect(await operation).toBe('completed')
    await closing
    expect(fixture.trace.resourceReleases).toBe(1)
  })

  test('continues cleanup after failures and never releases the same resource twice', async () => {
    const first = memoryConnection({ resourceFailure: new Error('first cleanup') })
    const second = memoryConnection({ releaseFailure: new Error('second layer cleanup') })
    const session = new EngineSession({ first: first.connection, second: second.connection }, shutdown)
    await session.start([])
    await assert.rejects(session.close(), AggregateError)
    expect(session.state).toBe('closed')
    expect(first.trace.resourceReleases).toBe(1)
    expect(second.trace.resourceReleases).toBe(1)
    await assert.rejects(session.close(), AggregateError)
    expect(first.trace.resourceReleases).toBe(1)
  })

  test('separate application sessions never share store instances', async () => {
    const fixture = memoryConnection()
    const first = new EngineSession({ main: fixture.connection }, shutdown)
    const second = new EngineSession({ main: fixture.connection }, shutdown)
    await Promise.all([first.start([]), second.start([])])
    try {
      expect(await first.withStore('main', (store) => store)).not.toBe(await second.withStore('main', (store) => store))
      await first.close()
      expect(await second.probe('main')).toMatchObject({ name: 'main' })
    } finally {
      await first.close()
      await second.close()
    }
  })

  test('unknown connections are rejected without inventing a store', async () => {
    const fixture = memoryConnection()
    const session = new EngineSession({ main: fixture.connection }, shutdown)
    await session.start([])
    try {
      await assert.rejects(session.probe('missing'), MqConnectionException)
      expect(fixture.trace.acquisitions).toBe(1)
    } finally {
      await session.close()
    }
  })
})
