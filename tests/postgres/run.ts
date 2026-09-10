import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import { Pool } from 'pg'
import { Test } from '@nestjs/testing'
import { makeJobId, makeJobName, makeQueueName } from 'better-effect-mq'
import { Result } from 'better-result'

import { MqConnectionException, MqConnectionsService, MqModule } from '../../src/index.ts'
import { migratePostgres, postgres, validatePostgres } from '../../src/integrations/postgres.ts'
import { EngineSession } from '../../src/engine/engine-session.ts'
import { MqEngineHost } from '../../src/engine/mq-engine.host.ts'

const connectionString = process.env.MQ_TEST_DATABASE_URL
assert.ok(connectionString, 'MQ_TEST_DATABASE_URL must point to a dedicated PostgreSQL test database')
const suffix = randomUUID().replaceAll('-', '')
const schema = `mq_test_${suffix}`
const missingSchema = `mq_missing_${suffix}`
const admin = new Pool({ connectionString, max: 5 })
const borrowed = new Pool({ connectionString, max: 10 })
const shutdown = { gracePeriodMs: 0, abortAfterGracePeriod: true }

function taggedConnection(tag: string): string {
  assert.ok(connectionString)
  const url = new URL(connectionString)
  url.searchParams.set('application_name', tag)
  return url.href
}

async function countConnections(tag: string): Promise<number> {
  const result = await admin.query<{ count: number }>(
    'SELECT count(*)::integer AS count FROM pg_stat_activity WHERE application_name = $1', [tag]
  )
  return result.rows[0]?.count ?? 0
}

async function assertDisconnected(tag: string): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (await countConnections(tag) === 0) return
    await setTimeout(20)
  }
  assert.equal(await countConnections(tag), 0, `Owned pool ${tag} left PostgreSQL connections open`)
}

try {
  const declaration = postgres({ pool: borrowed, schema, namespace: 'alpha' })
  assert.equal(borrowed.totalCount, 0, 'The factory must not connect')
  assert.equal(Object.isFrozen(borrowed), false)
  assert.equal(JSON.stringify(declaration).includes('connectionString'), false)
  const migration = await migratePostgres({ pool: admin, schema })
  assert.ok(migration.version > 0)
  assert.ok(migration.applied.length > 0)
  assert.equal(Object.isFrozen(migration.applied), true)
  assert.deepEqual((await migratePostgres({ pool: admin, schema })).applied, [])
  assert.equal((await validatePostgres({ pool: admin, schema })).version, migration.version)
  console.log('PASS explicit migrations and inert PostgreSQL declarations')

  const app = await Test.createTestingModule({ imports: [MqModule.forRoot({
    connections: {
      alpha: declaration,
      beta: postgres({ pool: borrowed, schema, namespace: 'beta' })
    }, shutdown
  })] }).compile()
  let persistedId: string | undefined
  try {
    await app.init()
    const monitor = app.get(MqConnectionsService)
    assert.equal(monitor.state, 'ready')
    assert.equal(monitor.connections().length, 2)
    assert.equal((await monitor.probe('alpha')).ownership, 'borrowed')
    assert.equal(JSON.stringify(monitor.connections()).includes('postgres://'), false)
    const session = app.get(MqEngineHost).session
    const queue = makeQueueName('durable')
    const name = makeJobName('task')
    if (Result.isError(queue)) throw queue.error
    if (Result.isError(name)) throw name.error
    const now = Date.now()
    const saved = await session.withStore('alpha', (store) => store.enqueue({
      job: { queue: queue.value, name: name.value, version: 1 },
      payload: { message: 'survives restart' }, now, runAt: now, attemptsMax: 3
    }))
    if (Result.isError(saved)) throw saved.error
    persistedId = saved.value.job.id
    const alpha = await session.withStore('alpha', (store) => store.counts())
    if (Result.isError(alpha)) throw alpha.error
    assert.equal(alpha.value.total, 1)
    const beta = await session.withStore('beta', (store) => store.counts())
    if (Result.isError(beta)) throw beta.error
    assert.equal(beta.value.total, 0)
  } finally {
    await app.close()
  }
  assert.ok(persistedId)
  const jobId = makeJobId(persistedId)
  if (Result.isError(jobId)) throw jobId.error
  assert.equal((await borrowed.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
  console.log('PASS real Nest startup, namespace isolation, durable writes and borrowed pool ownership')

  const ownedTag = `mq_owned_${suffix}`
  const owned = new EngineSession({ alpha: postgres({
    connectionString: taggedConnection(ownedTag), schema, namespace: 'alpha'
  }) }, shutdown)
  try {
    await owned.start([])
    assert.ok(await countConnections(ownedTag) > 0)
    const count = await owned.withStore('alpha', (store) => store.counts())
    if (Result.isError(count)) throw count.error
    assert.equal(count.value.total, 1, 'The same named connection must reopen the same durable storage')
    const stored = await owned.withStore('alpha', (store) => store.getJob({ jobId: jobId.value }))
    if (Result.isError(stored)) throw stored.error
    assert.deepEqual(stored.value?.payload, { message: 'survives restart' })
    assert.equal((await owned.probe('alpha')).ownership, 'owned')
  } finally {
    await Promise.all([owned.close(), owned.close()])
  }
  await assertDisconnected(ownedTag)
  console.log('PASS persistence across contexts and exactly-once owned pool cleanup')

  // The upstream adapter hashes the named JobStore token into its storage namespace.
  const renamed = new EngineSession({ renamed: postgres({ pool: borrowed, schema, namespace: 'alpha' }) }, shutdown)
  try {
    await renamed.start([])
    const count = await renamed.withStore('renamed', (store) => store.counts())
    if (Result.isError(count)) throw count.error
    assert.equal(count.value.total, 0, 'Renaming a connection deliberately changes its durable address')
  } finally {
    await renamed.close()
  }
  console.log('PASS named connection isolation follows the upstream persistence protocol')

  const goodTag = `mq_rollback_good_${suffix}`
  const badTag = `mq_rollback_bad_${suffix}`
  const failing = new EngineSession({
    good: postgres({ connectionString: taggedConnection(goodTag), schema, namespace: 'rollback' }),
    bad: postgres({ connectionString: taggedConnection(badTag), schema: missingSchema })
  }, shutdown)
  await assert.rejects(failing.start([]), MqConnectionException)
  assert.equal(failing.state, 'failed')
  assert.deepEqual(failing.connections(), [])
  await assertDisconnected(goodTag)
  await assertDisconnected(badTag)
  await failing.close()
  const missing = await admin.query<{ present: string | null }>('SELECT to_regclass($1) AS present', [`${missingSchema}.better_effect_mq_jobs`])
  assert.equal(missing.rows[0]?.present, null, 'Startup must never apply migrations')
  console.log('PASS failed schema validation, complete acquisition rollback and no automatic migrations')

  const invalidBorrowed = new EngineSession({ primary: postgres({ pool: borrowed, schema: missingSchema }) }, shutdown)
  await assert.rejects(invalidBorrowed.start([]), MqConnectionException)
  await invalidBorrowed.close()
  assert.equal((await borrowed.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
  console.log('PASS borrowed pools survive failed startup')
} finally {
  await borrowed.end()
  await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
  await admin.query(`DROP SCHEMA IF EXISTS "${missingSchema}" CASCADE`)
  await admin.end()
}
console.log('PostgreSQL integration passed against a real server')
