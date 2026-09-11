import { jobClient } from '../jobs/binding.ts'
import type { JobAttempt, JobEnqueueItem, JobEnqueueOptions, JobExecuteOptions, JobScheduleOptions, JobSnapshot, JobWaitOptions, PreparedJob } from '../jobs/types.ts'
import { ContractDefinitionException } from './errors.ts'
import { decodeSchema, encodeSchema, validateSchema } from './schema.ts'
import type { SchemaInput, SchemaOutput, ValueSchema } from './schema.ts'

export interface JobSchemas {
  readonly payload: ValueSchema
  readonly result: ValueSchema
  readonly failure: ValueSchema | undefined
}

/** Erased discovery view. The compiler validates values with these schemas before predicates run. */
export abstract class JobContract {
  abstract readonly schemas: JobSchemas
  abstract getIdempotencyKey(payload: SchemaOutput<ValueSchema>): string | undefined
  abstract canRetry(failure: SchemaOutput<ValueSchema>): boolean
}

export type InputOf<Job extends JobContract> = SchemaInput<Job['schemas']['payload']>
export type PayloadOf<Job extends JobContract> = SchemaOutput<Job['schemas']['payload']>
export type ResultOf<Job extends JobContract> = SchemaOutput<Job['schemas']['result']>
type FailureOutput<Schema> = Schema extends ValueSchema ? SchemaOutput<Schema> : never
export type FailureOf<Job extends JobContract> = FailureOutput<Job['schemas']['failure']>

export interface JobOptions<Payload extends ValueSchema, Result extends ValueSchema, Failure extends ValueSchema | undefined = undefined> {
  readonly payload: Payload
  readonly result: Result
  readonly failure?: Failure
  readonly idempotencyKey?: (payload: SchemaOutput<Payload>) => string
  readonly retryable?: (failure: FailureOutput<Failure>) => boolean
}

/** Inert until registered. Producer methods use only the owning application's live binding. */
export class JobDefinition<Payload extends ValueSchema, Result extends ValueSchema, Failure extends ValueSchema | undefined = undefined> extends JobContract {
  override readonly schemas: Readonly<{ payload: Payload; result: Result; failure: Failure | undefined }>
  private readonly keyFactory: JobOptions<Payload, Result, Failure>['idempotencyKey']
  private readonly retryClassifier: JobOptions<Payload, Result, Failure>['retryable']

  constructor(options: JobOptions<Payload, Result, Failure>) {
    super()
    if (options.failure === undefined && options.retryable !== undefined) throw new ContractDefinitionException('A retryable predicate requires a declared failure schema')
    this.schemas = Object.freeze({ payload: options.payload, result: options.result, failure: options.failure })
    this.keyFactory = options.idempotencyKey
    this.retryClassifier = options.retryable
  }

  parsePayload(input: SchemaInput<Payload>): Promise<SchemaOutput<Payload>> { return validateSchema(this.schemas.payload, input) }
  encodePayload(value: SchemaOutput<Payload>): Promise<string> { return encodeSchema(this.schemas.payload, value) }
  decodePayload(text: string): Promise<SchemaOutput<Payload>> { return decodeSchema(this.schemas.payload, text) }
  parseResult(input: SchemaInput<Result>): Promise<SchemaOutput<Result>> { return validateSchema(this.schemas.result, input) }
  encodeResult(value: SchemaOutput<Result>): Promise<string> { return encodeSchema(this.schemas.result, value) }
  decodeResult(text: string): Promise<SchemaOutput<Result>> { return decodeSchema(this.schemas.result, text) }

  async encodeFailure(value: FailureOutput<Failure>): Promise<string> {
    const schema = this.schemas.failure
    if (schema === undefined) throw new ContractDefinitionException('This job has no declared failure schema')
    return encodeSchema(schema, value)
  }

  async decodeFailure(text: string): Promise<FailureOutput<Failure>> {
    const schema = this.schemas.failure
    if (schema === undefined) throw new ContractDefinitionException('This job has no declared failure schema')
    const value = await decodeSchema(schema, text)
    // SAFETY: schema is this descriptor's declared Failure schema; absence was rejected above.
    return value as FailureOutput<Failure>
  }

  override getIdempotencyKey(payload: SchemaOutput<Payload>): string | undefined {
    const key = this.keyFactory?.(payload)
    if (key !== undefined && (key.length === 0 || key.trim() !== key)) throw new ContractDefinitionException('The idempotency key must be non-empty without surrounding whitespace')
    return key
  }

  override canRetry(failure: FailureOutput<Failure>): boolean { return this.retryClassifier?.(failure) ?? false }

  async enqueue(input: SchemaInput<Payload>, options?: JobEnqueueOptions): Promise<string> {
    const client = jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this)
    return client.enqueue(await this.encodePayload(await this.parsePayload(input)), options)
  }

  async enqueueDecoded(value: SchemaOutput<Payload>, options?: JobEnqueueOptions): Promise<string> {
    const client = jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this)
    return client.enqueue(await this.encodePayload(value), options)
  }

  async enqueueMany(items: ReadonlyArray<JobEnqueueItem<SchemaInput<Payload>>>): Promise<readonly string[]> {
    const client = jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this)
    const encoded = await Promise.all(items.map(async (item): Promise<JobEnqueueItem<string>> => {
      const payload = await this.encodePayload(await this.parsePayload(item.payload))
      return item.options === undefined ? { payload } : { payload, options: item.options }
    }))
    return client.enqueueMany(encoded)
  }

  async prepare(input: SchemaInput<Payload>, options?: JobEnqueueOptions): Promise<PreparedJob> {
    const client = jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this)
    return client.prepare(await this.encodePayload(await this.parsePayload(input)), options)
  }

  async poll(id: string): Promise<JobSnapshot<SchemaOutput<Result>, FailureOutput<Failure>> | undefined> {
    return jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this).poll(id)
  }

  async attempts(id: string): Promise<readonly JobAttempt<SchemaOutput<Result>, FailureOutput<Failure>>[]> {
    return jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this).attempts(id)
  }

  async awaitResult(id: string, options?: JobWaitOptions): Promise<SchemaOutput<Result>> {
    return jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this).awaitResult(id, options)
  }

  async execute(input: SchemaInput<Payload>, options: JobExecuteOptions = {}): Promise<SchemaOutput<Result>> {
    const id = await this.enqueue(input, options.enqueue)
    return this.awaitResult(id, options.wait)
  }

  async cancel(id: string): Promise<void> { await jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this).cancel(id) }
  async retry(id: string, options?: JobScheduleOptions): Promise<void> { await jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this).retry(id, options) }
  async promote(id: string): Promise<void> { await jobClient<SchemaOutput<Result>, FailureOutput<Failure>>(this).promote(id) }
}
