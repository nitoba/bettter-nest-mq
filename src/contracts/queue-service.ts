import { JobDefinition } from './job-definition.ts'
import type { JobOptions } from './job-definition.ts'
import type { ValueSchema } from './schema.ts'

/** Base for injectable queue contracts. Constructing a queue performs no I/O. */
export abstract class QueueService {
  protected job<Payload extends ValueSchema, Result extends ValueSchema, Failure extends ValueSchema | undefined = undefined>(
    options: JobOptions<Payload, Result, Failure>
  ): JobDefinition<Payload, Result, Failure> {
    return new JobDefinition(options)
  }
}
