import type { Pool } from 'pg'
import {
  JobDefinitionError,
  JobStoreFailure,
  makeJobId,
  validateAttemptRecord,
  validateJobRecordV2
} from 'better-effect-mq'
import type {
  AttemptRecordV2,
  CountsRequest,
  JobCountsV2,
  JobId,
  JobRecordV2,
  JsonValue
} from 'better-effect-mq'
import { Result } from 'better-result'
import type { FlowJobReads, FlowReadError } from '../engine/flow-read-store.ts'
import { MqFlowException } from '../flows/errors.ts'
import { postgresFlowJsonPool } from './postgres-json-pool.ts'
import { postgresFlowNamespace } from './postgres-flow-resource.ts'

type DatabaseNumber = number | string | null
export type FlowJobRow = {
  readonly id: string
  readonly name: string
  readonly queue: string
  readonly state: string
  readonly dispatch_key: string | null
  readonly idempotency_key: string | null
  readonly lease_owner: string | null
  readonly lease_token: string | null
  readonly payload: JsonValue
  readonly metadata: JsonValue
  readonly result: JsonValue
  readonly result_present: boolean
  readonly failure: JsonValue
  readonly parent: JsonValue
  readonly flow: JsonValue
  readonly backoff: JsonValue
} & {
  readonly [
    Key in
      | 'version'
      | 'priority'
      | 'run_at_ms'
      | 'sequence'
      | 'attempts_max'
      | 'attempts_made'
      | 'attempt_sequence'
      | 'delivery_count'
      | 'stalled_count'
      | 'timeout_ms'
      | 'created_at_ms'
      | 'updated_at_ms'
      | 'processed_at_ms'
      | 'finished_at_ms'
      | 'lease_expires_at_ms'
      | 'cancellation_requested_at_ms'
  ]: DatabaseNumber
}

type FlowAttemptRow = {
  readonly outcome: string
  readonly result: JsonValue
  readonly result_present: boolean
  readonly failure: JsonValue
} & {
  readonly [
    Key in
      | 'attempt'
      | 'attempt_sequence'
      | 'delivery'
      | 'started_at_ms'
      | 'finished_at_ms'
      | 'retry_at_ms'
      | 'retry_delay_ms'
  ]: DatabaseNumber
}

function numeric(value: DatabaseNumber): number {
  return value === null ? Number.NaN : Number(value)
}
function optionalNumeric(value: DatabaseNumber): number | undefined {
  return value === null ? undefined : numeric(value)
}

/** Decode only the persisted representation. The upstream v2 validator owns all state invariants. */
export function decodeFlowJobRow(row: FlowJobRow): Result<JobRecordV2, JobDefinitionError> {
  return validateJobRecordV2({
    id: row.id,
    name: row.name,
    queue: row.queue,
    version: numeric(row.version),
    state: row.state,
    dispatchKey: row.dispatch_key ?? undefined,
    payload: row.payload,
    metadata: row.metadata,
    priority: numeric(row.priority),
    runAt: numeric(row.run_at_ms),
    orderingSequence: numeric(row.sequence),
    attemptsMax: numeric(row.attempts_max),
    attemptsMade: numeric(row.attempts_made),
    attemptSequence: numeric(row.attempt_sequence),
    deliveryCount: numeric(row.delivery_count),
    stalledCount: numeric(row.stalled_count),
    backoff: row.backoff ?? undefined,
    timeoutMs: optionalNumeric(row.timeout_ms),
    idempotencyKey: row.idempotency_key ?? undefined,
    createdAt: numeric(row.created_at_ms),
    updatedAt: numeric(row.updated_at_ms),
    processedAt: optionalNumeric(row.processed_at_ms),
    finishedAt: optionalNumeric(row.finished_at_ms),
    leaseOwner: row.lease_owner ?? undefined,
    leaseToken: row.lease_token ?? undefined,
    leaseExpiresAt: optionalNumeric(row.lease_expires_at_ms),
    cancellationRequestedAt: optionalNumeric(row.cancellation_requested_at_ms),
    result: row.result_present ? row.result : undefined,
    failure: row.failure ?? undefined,
    parent: row.parent ?? undefined,
    flow: row.flow ?? undefined
  })
}
function decodeAttempt(row: FlowAttemptRow): Result<AttemptRecordV2, JobDefinitionError> {
  const fannedOut = row.outcome === 'fanned-out'
  if (
    fannedOut &&
    (row.result_present ||
      row.failure !== null ||
      row.retry_at_ms !== null ||
      row.retry_delay_ms !== null)
  ) {
    return Result.err(
      new JobDefinitionError({
        field: 'attempt',
        message: 'Fan-out ledger entries cannot contain a terminal result or retry outcome'
      })
    )
  }
  // Validate shared ledger fields through the released-attempt shape, then restore the
  // additive v2 outcome. Neither the stored value nor the returned outcome is rewritten.
  const checked = validateAttemptRecord({
    attempt: numeric(row.attempt),
    attemptSequence: optionalNumeric(row.attempt_sequence),
    delivery: numeric(row.delivery),
    startedAt: optionalNumeric(row.started_at_ms),
    finishedAt: optionalNumeric(row.finished_at_ms),
    outcome: fannedOut ? 'released' : row.outcome,
    result: row.result_present ? row.result : undefined,
    failure: row.failure ?? undefined,
    retryAt: optionalNumeric(row.retry_at_ms),
    retryDelayMs: optionalNumeric(row.retry_delay_ms)
  })
  if (Result.isError(checked)) return checked
  return Result.ok(
    fannedOut ? Object.freeze({ ...checked.value, outcome: 'fanned-out' }) : checked.value
  )
}
function storeFailure<Cause>(operation: string, cause: Cause): JobStoreFailure {
  const error = new JobStoreFailure({
    operation,
    message: 'PostgreSQL flow read failed',
    retryable: false
  })
  Object.defineProperty(error, 'cause', { value: cause })
  return error
}

/** Read companion for the pinned JobStore: no write SQL, no new connection and no changed namespace. */
export function postgresFlowReads(
  name: string,
  pool: Pool,
  schema: string,
  namespace: string
): FlowJobReads {
  const client = postgresFlowJsonPool(pool)
  const scoped = postgresFlowNamespace(name, namespace)
  const jobs = `"${schema}"."better_effect_mq_jobs"`
  const attempts = `"${schema}"."better_effect_mq_attempts"`
  const children = `"${schema}"."better_effect_mq_flow_children"`
  return {
    async getJob(id) {
      try {
        const rows = await client.query<FlowJobRow>(
          `SELECT j.*, j.result IS NOT NULL AS result_present FROM ${jobs} AS j WHERE namespace=$1 AND id=$2`,
          [scoped, id]
        )
        const row = rows.rows[0]
        return row === undefined ? Result.ok(undefined) : decodeFlowJobRow(row)
      } catch (cause) {
        return Result.err(storeFailure('getJobV2', cause))
      }
    },
    async getAttempts(id) {
      try {
        const rows = await client.query<FlowAttemptRow>(
          `SELECT a.*, a.result IS NOT NULL AS result_present FROM ${attempts} AS a WHERE namespace=$1 AND job_id=$2 ORDER BY attempt_sequence`,
          [scoped, id]
        )
        const records: AttemptRecordV2[] = []
        for (const row of rows.rows) {
          const value = decodeAttempt(row)
          if (Result.isError(value)) return value
          records.push(value.value)
        }
        return Result.ok(Object.freeze(records))
      } catch (cause) {
        return Result.err(storeFailure('getAttemptsV2', cause))
      }
    },
    async counts(request: CountsRequest = {}): Promise<Result<JobCountsV2, FlowReadError>> {
      try {
        const rows = await client.query<{ state: string; count: string }>(
          `SELECT state, count(*)::text AS count FROM ${jobs} WHERE namespace=$1 AND ($2::text IS NULL OR queue=$2) AND ($3::text IS NULL OR name=$3) GROUP BY state`,
          [scoped, request.queue ?? null, request.name ?? null]
        )
        const counts = {
          waiting: 0,
          delayed: 0,
          active: 0,
          completed: 0,
          failed: 0,
          cancelled: 0,
          waitingChildren: 0,
          total: 0
        }
        for (const row of rows.rows) {
          const count = Number(row.count)
          if (!Number.isSafeInteger(count) || count < 0) throw new Error('Invalid stored count')
          switch (row.state) {
            case 'waiting':
              counts.waiting = count
              break
            case 'delayed':
              counts.delayed = count
              break
            case 'active':
              counts.active = count
              break
            case 'completed':
              counts.completed = count
              break
            case 'failed':
              counts.failed = count
              break
            case 'cancelled':
              counts.cancelled = count
              break
            case 'waiting-children':
              counts.waitingChildren = count
              break
            default:
              return Result.err(
                new JobDefinitionError({
                  field: 'state',
                  message: 'Unrecognized persisted job state'
                })
              )
          }
          counts.total += count
          if (!Number.isSafeInteger(counts.total))
            throw new Error('Job count exceeds the supported integer range')
        }
        return Result.ok(Object.freeze(counts))
      } catch (cause) {
        return Result.err(storeFailure('countsV2', cause))
      }
    },
    async recoveryIds(flowNames) {
      if (flowNames.length === 0) return []
      const rows = await client.query<{ id: string }>(
        `SELECT j.id FROM ${jobs} AS j WHERE j.namespace=$1 AND j.flow->>'flowName'=ANY($2::text[]) AND (j.state IN ('waiting','active','waiting-children') OR EXISTS (SELECT 1 FROM ${children} AS c WHERE c.namespace=j.namespace AND c.flow_id=j.id AND c.status='cancelled' AND NOT c.cascaded)) ORDER BY j.id LIMIT 10001`,
        [scoped, [...flowNames]]
      )
      if (rows.rows.length > 10_000)
        throw new MqFlowException(
          'unavailable',
          'More than 10000 recoverable flow parents require a paginated deployment recovery strategy'
        )
      const ids: JobId[] = []
      for (const row of rows.rows) {
        const id = makeJobId(row.id)
        if (Result.isError(id))
          throw new MqFlowException('operation', 'Invalid persisted parent id', { cause: id.error })
        ids.push(id.value)
      }
      return Object.freeze(ids)
    }
  }
}
