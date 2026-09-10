import 'reflect-metadata'

export { MqModule } from './module/mq.module.ts'
export { MqConfiguration } from './module/mq.configuration.ts'
export { MqRegistry } from './module/mq.registry.ts'
export { MqConnectionsService } from './connections/mq-connections.service.ts'
export { MqConnectionException, MqEngineStateException } from './connections/errors.ts'
export type { MqConnectionPhase } from './connections/errors.ts'
export type {
  MqConnection,
  MqConnectionMap,
  MqConnectionSnapshot,
  MqConnectionOwnership,
  MqStoreCapabilities,
  MqCapability,
  MqEngineState
} from './connections/connection.ts'
export type {
  MqModuleOptions,
  MqOptionsFactory,
  MqResolvedOptions,
  MqShutdownOptions
} from './module/mq-module.options.ts'
export { QueueService } from './contracts/queue-service.ts'
export { JobContract, JobDefinition } from './contracts/job-definition.ts'
export type {
  JobOptions,
  JobSchemas,
  InputOf,
  PayloadOf,
  ResultOf,
  FailureOf
} from './contracts/job-definition.ts'
export { Queue, Job, Retry, JobTimeout } from './contracts/decorators.ts'
export type { QueueOptions, JobDecoratorOptions } from './contracts/decorators.ts'
export { getQueueDefinition } from './contracts/queue-definition.ts'
export type { JobIdentity, RegisteredJob, QueueDefinition } from './contracts/queue-definition.ts'
export { resolveJobPolicy } from './contracts/policies.ts'
export type {
  JobPolicy,
  ResolvedJobPolicy,
  RetryOptions,
  RetryBackoff
} from './contracts/policies.ts'
export { defineCodec, validateSchema, encodeSchema, decodeSchema } from './contracts/schema.ts'
export type { ValueSchema, SchemaCodec, SchemaInput, SchemaOutput } from './contracts/schema.ts'
export {
  ContractDefinitionException,
  JobFailureException,
  SchemaValidationException,
  SchemaDefectException,
  SchemaEncodingException
} from './contracts/errors.ts'
