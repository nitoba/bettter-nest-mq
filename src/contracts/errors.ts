import type { StandardSchemaV1 } from '@standard-schema/spec'

export class ContractDefinitionException extends Error {
  readonly code = 'MQ_CONTRACT_DEFINITION'

  constructor(message: string) {
    super(message)
    this.name = 'ContractDefinitionException'
  }
}

export class SchemaValidationException extends Error {
  readonly code = 'MQ_SCHEMA_VALIDATION'
  readonly issues: ReadonlyArray<StandardSchemaV1.Issue>

  constructor(issues: ReadonlyArray<StandardSchemaV1.Issue>) {
    super('The value does not satisfy the job schema')
    this.name = 'SchemaValidationException'
    this.issues = Object.freeze(
      issues.map((issue) =>
        Object.freeze({
          message: issue.message,
          path: issue.path === undefined ? undefined : Object.freeze([...issue.path])
        })
      )
    )
  }
}

export class SchemaDefectException extends Error {
  readonly code = 'MQ_SCHEMA_DEFECT'

  constructor(
    readonly phase: 'validate' | 'encode',
    options: ErrorOptions
  ) {
    super(`The schema implementation threw during ${phase}`, options)
    this.name = 'SchemaDefectException'
  }
}

export class SchemaEncodingException extends Error {
  readonly code = 'MQ_SCHEMA_ENCODING'

  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'SchemaEncodingException'
  }
}

/** A serializable domain failure is validated by the job's failure schema at persistence time. */
export class JobFailureException<Failure> extends Error {
  readonly code = 'MQ_JOB_FAILURE'

  constructor(
    readonly failure: Failure,
    options?: ErrorOptions
  ) {
    super('The job reported a domain failure', options)
    this.name = 'JobFailureException'
  }
}
