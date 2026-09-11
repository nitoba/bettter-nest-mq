import { Codec, JobDecodeFailure, JobEncodeFailure, Queue } from 'better-effect-mq'
import type {
  JobDefaultsInput,
  JobDefinition as EngineJobDefinition,
  PersistedBackoff
} from 'better-effect-mq'
import { Result } from 'better-result'

import { ContractDefinitionException, JobFailureException } from '../contracts/errors.ts'
import { copyRetry } from '../contracts/policies.ts'
import type { RetryOptions } from '../contracts/policies.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { decodeSchema, encodeSchema, validateSchema } from '../contracts/schema.ts'
import type { SchemaOutput, ValueSchema } from '../contracts/schema.ts'
import { operationStoreToken } from './operation-store.ts'
import type { OperationStoreToken } from './operation-store.ts'

export type ContractValue = SchemaOutput<ValueSchema>
export type DomainFailure = JobFailureException<ContractValue>
export type CompiledJob = EngineJobDefinition<
  string,
  string,
  number,
  Codec<ContractValue>,
  Codec<ContractValue>,
  Codec<DomainFailure>,
  OperationStoreToken
>

/** One schema bridge for every persisted boundary; codec errors remain engine encode/decode failures. */
function schemaCodec(schema: ValueSchema): Codec<ContractValue> {
  return Codec.make<ContractValue>({
    encode: async (value) => {
      try {
        return Result.ok(JSON.parse(await encodeSchema(schema, value)))
      } catch {
        return Result.err(
          new JobEncodeFailure({
            message: 'The value cannot be encoded by its declared job schema'
          })
        )
      }
    },
    decode: async (value) => {
      try {
        return Result.ok(await validateSchema(schema, value))
      } catch {
        return Result.err(
          new JobDecodeFailure({
            message: 'Persisted data does not satisfy its declared job schema'
          })
        )
      }
    }
  })
}

function failureCodec(schema: ValueSchema | undefined): Codec<DomainFailure> {
  return Codec.make<DomainFailure>({
    encode: async (failure) => {
      if (schema === undefined || !(failure instanceof JobFailureException)) {
        return Result.err(
          new JobEncodeFailure({ message: 'No valid domain-failure contract was declared' })
        )
      }
      try {
        return Result.ok(JSON.parse(await encodeSchema(schema, failure.failure)))
      } catch {
        return Result.err(
          new JobEncodeFailure({
            message: 'The domain failure does not satisfy its declared schema'
          })
        )
      }
    },
    decode: async (value) => {
      if (schema === undefined)
        return Result.err(
          new JobDecodeFailure({ message: 'No domain-failure contract was declared' })
        )
      try {
        return Result.ok(new JobFailureException(await validateSchema(schema, value)))
      } catch {
        return Result.err(
          new JobDecodeFailure({ message: 'Persisted domain failure does not satisfy its schema' })
        )
      }
    }
  })
}

/** Facade syntax is translated to the engine's serializable policy, never a serialized function. */
export function compileBackoff(options: RetryOptions): PersistedBackoff {
  const backoff = copyRetry(options).backoff
  if (backoff === undefined) return { type: 'constant', delayMs: 0 }
  if (backoff.type === 'custom')
    throw new ContractDefinitionException(
      'Named custom retry providers are not supported by the execution bridge yet'
    )
  let compiled: PersistedBackoff
  switch (backoff.type) {
    case 'fixed':
      compiled = { type: 'constant', delayMs: backoff.delayMs }
      break
    case 'linear':
      compiled = {
        type: 'linear',
        delayMs: backoff.initialDelayMs,
        incrementMs: backoff.incrementMs
      }
      break
    case 'exponential':
      compiled = { type: 'exponential', delayMs: backoff.initialDelayMs, factor: backoff.factor }
      break
  }
  if (backoff.maxDelayMs !== undefined) compiled = { ...compiled, maxDelayMs: backoff.maxDelayMs }
  if (backoff.jitter !== undefined) compiled = { ...compiled, jitter: backoff.jitter }
  return compiled
}

export function compileJob(registered: RegisteredJob): CompiledJob {
  const { contract, identity, policy } = registered
  if (policy.timeoutMs === 0)
    throw new ContractDefinitionException('Executable job timeouts must be greater than zero')
  let defaults: JobDefaultsInput = {
    attempts: policy.retry.attempts,
    backoff: compileBackoff(policy.retry),
    priority: policy.priority
  }
  if (policy.timeoutMs !== undefined) defaults = { ...defaults, timeoutMs: policy.timeoutMs }
  return Queue.define(identity.queue).job(identity.name, {
    version: identity.version,
    payload: schemaCodec(contract.schemas.payload),
    result: schemaCodec(contract.schemas.result),
    failure: failureCodec(contract.schemas.failure),
    defaults,
    store: operationStoreToken(identity.connection),
    idempotencyKey: (payload) => contract.getIdempotencyKey(payload),
    retryable: (failure) => contract.canRetry(failure.failure)
  })
}

/** Validate a thrown domain failure before it enters the engine's typed-error channel. */
export async function validateDomainFailure(
  job: RegisteredJob,
  failure: DomainFailure
): Promise<DomainFailure> {
  const schema = job.contract.schemas.failure
  if (schema === undefined)
    throw new ContractDefinitionException('JobFailureException requires a declared failure schema')
  const value = await decodeSchema(schema, await encodeSchema(schema, failure.failure))
  return new JobFailureException(value, { cause: failure })
}
