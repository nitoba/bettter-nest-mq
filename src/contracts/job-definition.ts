import { ContractDefinitionException } from './errors.ts'
import { decodeSchema, encodeSchema, validateSchema } from './schema.ts'
import type { SchemaInput, SchemaOutput, ValueSchema } from './schema.ts'

export interface JobSchemas {
  readonly payload: ValueSchema
  readonly result: ValueSchema
  readonly failure: ValueSchema | undefined
}

/** Erased descriptor view used by discovery; business code retains the concrete JobDefinition. */
export abstract class JobContract {
  abstract readonly schemas: JobSchemas
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

/** Inert typed validation/encoding contract. No store, runtime or producer is created here. */
export class JobDefinition<Payload extends ValueSchema, Result extends ValueSchema, Failure extends ValueSchema | undefined = undefined> extends JobContract {
  override readonly schemas: Readonly<{ payload: Payload; result: Result; failure: Failure | undefined }>
  private readonly keyFactory: JobOptions<Payload, Result, Failure>['idempotencyKey']
  private readonly retryClassifier: JobOptions<Payload, Result, Failure>['retryable']

  constructor(options: JobOptions<Payload, Result, Failure>) {
    super()
    if (options.failure === undefined && options.retryable !== undefined) {
      throw new ContractDefinitionException('A retryable predicate requires a declared failure schema')
    }
    this.schemas = Object.freeze({ payload: options.payload, result: options.result, failure: options.failure })
    this.keyFactory = options.idempotencyKey
    this.retryClassifier = options.retryable
  }

  parsePayload(input: SchemaInput<Payload>): Promise<SchemaOutput<Payload>> {
    return validateSchema(this.schemas.payload, input)
  }

  encodePayload(value: SchemaOutput<Payload>): Promise<string> {
    return encodeSchema(this.schemas.payload, value)
  }

  decodePayload(text: string): Promise<SchemaOutput<Payload>> {
    return decodeSchema(this.schemas.payload, text)
  }

  parseResult(input: SchemaInput<Result>): Promise<SchemaOutput<Result>> {
    return validateSchema(this.schemas.result, input)
  }

  encodeResult(value: SchemaOutput<Result>): Promise<string> {
    return encodeSchema(this.schemas.result, value)
  }

  decodeResult(text: string): Promise<SchemaOutput<Result>> {
    return decodeSchema(this.schemas.result, text)
  }

  async encodeFailure(value: FailureOutput<Failure>): Promise<string> {
    const schema = this.schemas.failure
    if (schema === undefined) throw new ContractDefinitionException('This job has no declared failure schema')
    return encodeSchema(schema, value)
  }

  async decodeFailure(text: string): Promise<FailureOutput<Failure>> {
    const schema = this.schemas.failure
    if (schema === undefined) throw new ContractDefinitionException('This job has no declared failure schema')
    const value = await decodeSchema(schema, text)
    // SAFETY: schema is the declared Failure schema, and undefined was rejected before decoding.
    return value as FailureOutput<Failure>
  }

  getIdempotencyKey(payload: SchemaOutput<Payload>): string | undefined {
    const key = this.keyFactory?.(payload)
    if (key !== undefined && (key.length === 0 || key.trim() !== key)) {
      throw new ContractDefinitionException('The idempotency key must be non-empty without surrounding whitespace')
    }
    return key
  }

  canRetry(failure: FailureOutput<Failure>): boolean {
    return this.retryClassifier?.(failure) ?? false
  }
}
