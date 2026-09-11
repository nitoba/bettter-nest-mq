import { postgresFlowLayer } from './postgres-flow-resource.ts'
import { Logger } from '@nestjs/common'
import type { Pool } from 'pg'
import {
  PostgresJobStore,
  PostgresMigrator,
  validateNamespace,
  validatePool,
  validateSchema
} from 'better-effect-mq-postgres'
import type { MqCapability, MqConnection } from '../connections/connection.ts'
import { MqConnectionException } from '../connections/errors.ts'
import { requireInteger } from '../contracts/policies.ts'
import { defineConnection } from '../engine/connection-definition.ts'
import type { AcquiredConnection } from '../engine/connection-definition.ts'
import { postgresJsonPool } from './postgres-json-pool.ts'
import { postgresScheduleLayer } from './postgres-schedule-resource.ts'
import { postgresOutboxLayer } from './postgres-outbox-resource.ts'

interface PostgresCommonOptions {
  readonly schema?: string
  readonly namespace?: string
  readonly validateSchema?: boolean
  readonly requireCapabilities?: ReadonlyArray<MqCapability>
  /** Enable a transactional outbox sharing this connection's native pool. */
  readonly outbox?: boolean
  readonly schedules?: boolean
  readonly flows?: boolean
}
export type PostgresConnectionOptions = PostgresCommonOptions &
  (
    | {
        readonly pool: Pool
        readonly connectionString?: never
        readonly max?: never
        readonly connectionTimeoutMs?: never
        readonly idleTimeoutMs?: never
      }
    | {
        readonly pool?: never
        readonly connectionString: string
        readonly max?: number
        readonly connectionTimeoutMs?: number
        readonly idleTimeoutMs?: number
      }
  )

export function postgres(options: PostgresConnectionOptions): MqConnection {
  try {
    return createPostgresConnection(options)
  } catch (cause) {
    if (cause instanceof MqConnectionException) throw cause
    throw new MqConnectionException('<postgres>', 'configuration', { cause })
  }
}

function withOutbox(
  resource: AcquiredConnection,
  pool: Pool,
  schema: string,
  namespace: string,
  enabled: boolean,
  schedules: boolean,
  flows: boolean
): AcquiredConnection {
  let result = resource
  if (enabled)
    result = { ...result, outbox: (name) => postgresOutboxLayer(name, pool, schema, namespace) }
  if (schedules)
    result = {
      ...result,
      schedules: (name) => postgresScheduleLayer(name, pool, schema, namespace)
    }
  if (flows)
    result = { ...result, flows: (name) => postgresFlowLayer(name, pool, schema, namespace) }
  return result
}

function createPostgresConnection(options: PostgresConnectionOptions): MqConnection {
  const schema = validateSchema(options.schema ?? 'public')
  const namespace = validateNamespace(options.namespace ?? 'default')
  const validate = options.validateSchema ?? true
  const outbox = options.outbox ?? false
  const flows = options.flows ?? false
  if (flows !== true && flows !== false)
    throw new MqConnectionException('<postgres>', 'configuration')
  const schedules = options.schedules ?? false
  if (schedules !== true && schedules !== false)
    throw new MqConnectionException('<postgres>', 'configuration')
  if (outbox !== true && outbox !== false)
    throw new MqConnectionException('<postgres>', 'configuration')
  const scope = JSON.stringify([schema, namespace])
  const requirements = [...(options.requireCapabilities ?? [])]
  if (options.pool !== undefined) {
    if (options.connectionString !== undefined)
      throw new MqConnectionException('<postgres>', 'configuration')
    const pool = options.pool
    validatePool(pool)
    return defineConnection(
      { adapter: 'postgres', ownership: 'borrowed', boundary: pool, scope, requirements },
      (token) =>
        withOutbox(
          {
            layer: PostgresJobStore.layerFor(token, {
              pool: postgresJsonPool(pool),
              schema,
              namespace,
              validateSchema: validate
            })
          },
          pool,
          schema,
          namespace,
          outbox,
          schedules,
          flows
        )
    )
  }
  const connectionString = options.connectionString
  const url = new URL(connectionString)
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:')
    throw new Error('Expected a PostgreSQL URL')
  const max = options.max ?? 10
  const connectionTimeoutMillis = options.connectionTimeoutMs ?? 10_000
  const idleTimeoutMillis = options.idleTimeoutMs ?? 10_000
  requireInteger(max, 'postgres.max', 2)
  requireInteger(connectionTimeoutMillis, 'postgres.connectionTimeoutMs', 1)
  requireInteger(idleTimeoutMillis, 'postgres.idleTimeoutMs')
  return defineConnection(
    { adapter: 'postgres', ownership: 'owned', boundary: connectionString, scope, requirements },
    async (token) => {
      const { Pool: PoolConstructor } = await import('pg')
      const pool = new PoolConstructor({
        connectionString,
        max,
        connectionTimeoutMillis,
        idleTimeoutMillis
      })
      const logger = new Logger('BetterNestMqPostgres')
      const onIdleError = (): void => {
        logger.warn('An idle PostgreSQL client disconnected; the pool will replace it on demand')
      }
      pool.on('error', onIdleError)
      const release = async (): Promise<void> => {
        await pool.end()
        pool.removeListener('error', onIdleError)
      }
      try {
        return withOutbox(
          {
            layer: PostgresJobStore.layerFor(token, {
              pool: postgresJsonPool(pool),
              schema,
              namespace,
              validateSchema: validate
            }),
            release
          },
          pool,
          schema,
          namespace,
          outbox,
          schedules,
          flows
        )
      } catch (cause) {
        try {
          await release()
        } catch (cleanupCause) {
          throw new AggregateError([cause, cleanupCause], 'PostgreSQL setup and cleanup failed', {
            cause
          })
        }
        throw cause
      }
    }
  )
}

export interface PostgresMigrationOptions {
  readonly pool: Pool
  readonly schema?: string
}
export interface PostgresMigrationReport {
  readonly schema: string
  readonly version: number
  readonly applied: ReadonlyArray<number>
}
export interface PostgresSchemaReport {
  readonly schema: string
  readonly version: number
}

export async function migratePostgres(
  options: PostgresMigrationOptions
): Promise<PostgresMigrationReport> {
  try {
    const schema = validateSchema(options.schema ?? 'public')
    const result = await PostgresMigrator.run(options.pool, { schema })
    return Object.freeze({
      schema: result.schema,
      version: result.version,
      applied: Object.freeze([...result.applied])
    })
  } catch (cause) {
    throw new MqConnectionException('<postgres-migrations>', 'migrate', { cause })
  }
}
export async function validatePostgres(
  options: PostgresMigrationOptions
): Promise<PostgresSchemaReport> {
  try {
    const schema = validateSchema(options.schema ?? 'public')
    const result = await PostgresMigrator.validate(options.pool, { schema })
    return Object.freeze({ schema: result.schema, version: result.version })
  } catch (cause) {
    throw new MqConnectionException('<postgres-migrations>', 'probe', { cause })
  }
}

export { postgresOutbox } from './postgres-outbox.ts'
export type {
  PostgresOutboxClient,
  PostgresOutboxTransaction,
  PostgresOutboxCallback,
  PostgresOutboxParameter
} from './postgres-outbox.types.ts'
