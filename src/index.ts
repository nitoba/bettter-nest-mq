import 'reflect-metadata'
export { Schedule } from './schedules/decorator.ts'
export { MqSchedulesService } from './schedules/service.ts'
export { MqScheduleException } from './schedules/errors.ts'
export type { SchedulePhase } from './schedules/errors.ts'
export type {
  ScheduleOptions,
  ScheduleMisfire,
  ScheduleCadence,
  MqScheduleOptions,
  ScheduleSnapshot,
  ScheduleReport,
  ScheduleListOptions,
  SchedulerSnapshot
} from './schedules/types.ts'

export { MqOutboxService } from './outbox/service.ts'
export { MqOutboxException } from './outbox/errors.ts'
export type { OutboxPhase } from './outbox/errors.ts'
export type {
  OutboxEntry,
  OutboxState,
  OutboxSnapshot,
  OutboxAppendResult,
  OutboxCounts,
  OutboxListOptions,
  OutboxPublisherSnapshot,
  OutboxFailure,
  MqOutboxOptions
} from './outbox/types.ts'

export { QueueControls } from './controls/decorator.ts'
export { MqQueueControlsService } from './controls/service.ts'
export { QueueControlsException } from './controls/errors.ts'
export type { QueueControlsPhase } from './controls/errors.ts'
export type {
  QueueControlsOptions,
  MqControlsOptions,
  QueueControlsSnapshot,
  QueueControlsReport
} from './controls/types.ts'

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
  MqShutdownOptions,
  MqExecutionOptions
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
export { Worker, Process, JobData, JobContext } from './workers/decorators.ts'
export { MqWorkersService } from './workers/mq-workers.service.ts'
export type {
  WorkerOptions,
  ProcessOptions,
  JobExecutionContext,
  WorkerSnapshot,
  WorkerIdleOptions
} from './workers/types.ts'
export {
  MqJobException,
  JobWaitTimeoutException,
  JobWaitAbortedException,
  JobCancelledException
} from './jobs/errors.ts'
export type { JobOperationName } from './jobs/errors.ts'
export type {
  JobEnqueueOptions,
  JobEnqueueItem,
  JobScheduleOptions,
  JobWaitOptions,
  JobExecuteOptions,
  JobJsonValue,
  JobState,
  JobFailureKind,
  JobAttemptOutcome,
  JobFailureView,
  JobSnapshot,
  JobAttempt,
  PreparedJob,
  PreparedBackoff
} from './jobs/types.ts'

export { Flow, FanOut, Collect, FlowData, FlowChildren } from './flows/decorators.ts'
export { flowJob, flowChildren } from './flows/references.ts'
export type { FlowJobReference, FlowChildPlan } from './flows/references.ts'
export { MqFlowsService } from './flows/service.ts'
export { MqFlowException } from './flows/errors.ts'
export type { FlowPhase } from './flows/errors.ts'
export type {
  FlowOptions,
  FlowChildOptions,
  FlowChildInput,
  FlowManifest,
  FlowCounts,
  FlowChildResult,
  FlowPageOptions,
  FlowChildPage,
  FlowResultsReader,
  FlowSnapshot
} from './flows/types.ts'
