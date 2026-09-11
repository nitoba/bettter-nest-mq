import { assertRetryReference } from './retry-reference.ts'
import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { validatePreparedEnqueue } from 'better-effect-mq'
import { makeOutboxId, makeOutboxRecord } from 'better-effect-mq-outbox'
import type { OutboxRecord, OutboxRecordInput } from 'better-effect-mq-outbox'
import { Result } from 'better-result'
import { validateDispatchKey } from '../controls/decorator.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { encodeSchema, validateSchema } from '../contracts/schema.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { OutboxEntry, OutboxSnapshot } from '../outbox/types.ts'

export async function compileOutboxRecord(
  source: string,
  entry: OutboxEntry,
  jobs: readonly RegisteredJob[]
): Promise<OutboxRecord> {
  const id = makeOutboxId(entry.id)
  if (Result.isError(id))
    throw new MqOutboxException('prepare', 'Invalid outbox id', { cause: id.error })
  const stable = `outbox-${createHash('sha256')
    .update(JSON.stringify([source, entry.id, entry.job.connection]))
    .digest('hex')}`
  const prepared = validatePreparedEnqueue({
    ...entry.job.request,
    id: entry.job.request.id ?? stable
  })
  if (Result.isError(prepared))
    throw new MqOutboxException('prepare', 'Invalid prepared job', { cause: prepared.error })
  const request = prepared.value
  const job = jobs.find(
    ({ identity }) =>
      identity.connection === entry.job.connection &&
      identity.queue === request.identity.queue &&
      identity.name === request.identity.name &&
      identity.version === request.identity.version
  )
  if (job === undefined)
    throw new MqOutboxException(
      'prepare',
      'The outbox destination job must be registered in this application'
    )
  try {
    assertRetryReference(job, request.metadata)
  } catch (cause) {
    throw new MqOutboxException(
      'prepare',
      'Prepared retry policy differs from the registered contract',
      { cause }
    )
  }
  const payload = await validateSchema(job.contract.schemas.payload, request.payload)
  const encoded = JSON.parse(await encodeSchema(job.contract.schemas.payload, payload))
  if (!isDeepStrictEqual(encoded, request.payload))
    throw new MqOutboxException(
      'prepare',
      'Prepared payload is not the canonical encoded contract value'
    )
  const derived = job.contract.getDispatchKey(payload)
  if (request.dispatchKey !== undefined) validateDispatchKey(request.dispatchKey)
  if (derived !== undefined && derived !== request.dispatchKey)
    throw new MqOutboxException('prepare', 'Prepared dispatch key differs from the job contract')
  if (job.controls?.perKeyConcurrency !== undefined && request.dispatchKey === undefined)
    throw new MqOutboxException('prepare', 'This destination queue requires a dispatch key')
  let input: OutboxRecordInput = {
    id: id.value,
    target: entry.job.connection,
    request,
    attemptsMax: entry.attempts ?? 10,
    nowMs: Date.now()
  }
  if (entry.runAtMs !== undefined) input = { ...input, runAtMs: entry.runAtMs }
  const result = makeOutboxRecord(input)
  if (Result.isError(result))
    throw new MqOutboxException('prepare', 'Invalid outbox publication policy', {
      cause: result.error
    })
  return result.value
}

/** The pinned upstream digest omits target and dispatchKey. Check the complete immutable
 * request after its locked append so a duplicate can never silently change its destination. */
export function assertSameOutboxContent(existing: OutboxRecord, incoming: OutboxRecord): void {
  if (
    existing.target !== incoming.target ||
    existing.attemptsMax !== incoming.attemptsMax ||
    !isDeepStrictEqual(existing.request, incoming.request)
  )
    throw new MqOutboxException(
      'conflict',
      'This outbox id already belongs to another request or destination'
    )
}

export function outboxSnapshot(source: string, record: OutboxRecord): OutboxSnapshot {
  return Object.freeze({
    source,
    id: record.id,
    target: record.target,
    state: record.state,
    request: record.request,
    attemptsMax: record.attemptsMax,
    attemptsMade: record.attemptsMade,
    runAtMs: record.runAtMs,
    createdAtMs: record.createdAtMs,
    updatedAtMs: record.updatedAtMs,
    publishedAtMs: record.publishedAtMs,
    failure: record.failure
  })
}
