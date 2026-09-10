import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Pool } from 'pg'

import { MqConfiguration, MqConnectionException } from '../../src/index.ts'
import { postgres, type PostgresConnectionOptions } from '../../src/integrations/postgres.ts'

const invalidConfigurations: ReadonlyArray<PostgresConnectionOptions> = [
  { connectionString: 'not a URL' },
  { connectionString: 'https://example.com/database' },
  { connectionString: 'postgresql://localhost/database', max: 1 },
  { connectionString: 'postgresql://localhost/database', connectionTimeoutMs: 0 },
  { connectionString: 'postgresql://localhost/database', idleTimeoutMs: -1 },
  { connectionString: 'postgresql://localhost/database', schema: 'invalid;schema' }
]

test.each(invalidConfigurations)(
  'normalizes PostgreSQL configuration failures into facade errors: %p',
  (options) => {
    assert.throws(
      () => postgres(options),
      (error) => {
        assert.ok(error instanceof MqConnectionException)
        expect(error.phase).toBe('configuration')
        expect(error.message).not.toContain(options.connectionString ?? 'postgresql://')
        return true
      }
    )
  }
)

test('borrowed pool declarations do not connect, freeze or install pool-level error listeners', async () => {
  const pool = new Pool({
    connectionString: 'postgresql://unused:unused@localhost:1/unused',
    max: 2
  })
  const errorListeners = pool.listenerCount('error')
  try {
    const connection = postgres({ pool })
    const connections = { primary: connection }
    const configuration = new MqConfiguration({ connections })
    expect(configuration.options.connections?.primary).toBe(connection)
    expect(configuration.options.connections).not.toBe(connections)
    expect(Object.isFrozen(configuration.options.connections)).toBe(true)
    expect(Object.isFrozen(connections)).toBe(false)
    expect(Object.isFrozen(pool)).toBe(false)
    expect(pool.totalCount).toBe(0)
    expect(pool.listenerCount('error')).toBe(errorListeners)
    expect(JSON.stringify(connection)).toBe('{"adapter":"postgres","ownership":"borrowed"}')
  } finally {
    await pool.end()
  }
})
