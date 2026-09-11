import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { MqConfiguration, MqConnectionsService, MqModule, type MqConnection } from 'better-nest-mq'
import { migratePostgres, postgres, validatePostgres } from 'better-nest-mq/postgres'

const unusedPool = new Pool({ connectionString: 'postgresql://unused:unused@localhost:1/unused' })
try {
  const borrowed: MqConnection = postgres({ pool: unusedPool, namespace: 'packed' })
  const owned: MqConnection = postgres({ connectionString: 'postgresql://unused:unused@localhost:1/unused', max: 2 })
  assert.equal(borrowed.ownership, 'borrowed')
  assert.equal(owned.ownership, 'owned')
  assert.equal(unusedPool.totalCount, 0)
  assert.equal(new MqConfiguration({ connections: { primary: borrowed } }).options.connections?.primary, borrowed)
} finally { await unusedPool.end() }

export function invalidOwnership(pool: Pool): void {
  // @ts-expect-error A connection cannot be both borrowed and owned.
  postgres({ pool, connectionString: 'postgresql://localhost/database' })
}

async function assertDisconnected(admin: Pool, tag: string): Promise<void> {
  // Observe remote backend teardown rather than assuming socket close and a query on a
  // different session are synchronous. The same bounded check is used by the ownership suite.
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const result = await admin.query<{ count: number }>(
      'SELECT count(*)::integer AS count FROM pg_stat_activity WHERE application_name = $1', [tag]
    )
    if (result.rows[0]?.count === 0) return
    await setTimeout(20)
  }
  const remaining = await admin.query<{ pid: number; state: string }>(
    'SELECT pid, state FROM pg_stat_activity WHERE application_name = $1', [tag]
  )
  assert.deepEqual(remaining.rows, [], 'Owned PostgreSQL backends remained open after application shutdown')
}

async function verifyLiveDatabase(connectionString: string): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '')
  const schema = `mq_packed_${suffix}`
  const tag = `mq_packed_${suffix}`
  const url = new URL(connectionString)
  url.searchParams.set('application_name', tag)
  const admin = new Pool({ connectionString })
  try {
    const migration = await migratePostgres({ pool: admin, schema })
    assert.equal((await validatePostgres({ pool: admin, schema })).version, migration.version)

    @Module({ imports: [MqModule.forRoot({ connections: {
      primary: postgres({ connectionString: url.href, schema, namespace: 'packed' })
    } })] })
    class ApplicationModule {}

    const app = await NestFactory.createApplicationContext(ApplicationModule, { logger: false, abortOnError: false })
    try {
      const monitor = app.get(MqConnectionsService)
      assert.equal(monitor.state, 'ready')
      assert.equal((await monitor.probe('primary')).adapter, 'postgres')
      const terminated = await admin.query<{ terminated: boolean }>(
        `SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity
         WHERE application_name = $1 AND state = 'idle' AND query NOT LIKE 'LISTEN %'`, [tag]
      )
      assert.ok(terminated.rows.some((row) => row.terminated), 'The fault test must disconnect an actual idle pool client')
      await setTimeout(50)
      assert.equal((await monitor.probe('primary')).name, 'primary')
    } finally { await app.close() }
    await assertDisconnected(admin, tag)
    console.log('Packed PostgreSQL consumer: actual migrations, Nest readiness, idle-client recovery and owned cleanup passed')
  } finally {
    try { await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`) }
    finally { await admin.end() }
  }
}

const liveConnection = process.env.MQ_TEST_DATABASE_URL
if (liveConnection !== undefined) await verifyLiveDatabase(liveConnection)
console.log('External consumer: optional PostgreSQL types, exports and inert declarations passed')
