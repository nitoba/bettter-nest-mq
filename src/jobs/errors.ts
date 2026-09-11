export type JobOperationName = 'binding' | 'enqueue' | 'enqueueMany' | 'prepare' | 'poll' | 'attempts' | 'awaitResult' | 'cancel' | 'retry' | 'promote'

export class MqJobException extends Error {
  readonly code = 'MQ_JOB_OPERATION'
  constructor(readonly operation: JobOperationName, message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = 'MqJobException'
  }
}

export class JobWaitTimeoutException extends Error {
  readonly code = 'MQ_JOB_WAIT_TIMEOUT'
  constructor(readonly jobId: string, readonly timeoutMs: number) {
    super(`Waiting for job ${JSON.stringify(jobId)} exceeded ${timeoutMs}ms; the job was not cancelled`)
    this.name = 'JobWaitTimeoutException'
  }
}

export class JobWaitAbortedException extends Error {
  readonly code = 'MQ_JOB_WAIT_ABORTED'
  constructor(readonly jobId: string, options?: ErrorOptions) {
    super(`Waiting for job ${JSON.stringify(jobId)} was aborted; the job was not cancelled`, options)
    this.name = 'JobWaitAbortedException'
  }
}

export class JobCancelledException extends Error {
  readonly code = 'MQ_JOB_CANCELLED'
  constructor(readonly jobId: string, options?: ErrorOptions) {
    super(`Job ${JSON.stringify(jobId)} was cancelled`, options)
    this.name = 'JobCancelledException'
  }
}
