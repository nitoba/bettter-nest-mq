import type { Type } from '@nestjs/common'
import type { SchemaOutput, ValueSchema } from '../contracts/schema.ts'
import type { FlowResultsReader } from '../flows/types.ts'
import type { JobExecutionContext } from '../workers/types.ts'

/** An MQ invocation, never an HTTP request or a Nest HTTP execution context. */
export interface MqExecutionContext<Payload = SchemaOutput<ValueSchema>> {
  readonly type: 'mq'
  readonly phase: 'process' | 'fanOut' | 'collect'
  readonly worker: Type
  readonly method: string
  readonly job: JobExecutionContext
  readonly payload: Payload
  readonly children: FlowResultsReader | undefined
}

export interface MqGuard<Payload = SchemaOutput<ValueSchema>> {
  canActivate(context: MqExecutionContext<Payload>): boolean | Promise<boolean>
}

export interface MqPipe<Payload = SchemaOutput<ValueSchema>> {
  transform(payload: Payload, context: MqExecutionContext<Payload>): Payload | Promise<Payload>
}

/** A single-use continuation, valid only while its interceptor is active. */
export type MqNext<Value = SchemaOutput<ValueSchema>> = () => Promise<Value>

export interface MqInterceptor<Value = SchemaOutput<ValueSchema>> {
  intercept(context: MqExecutionContext, next: MqNext<Value>): Value | Promise<Value>
}

export interface MqExceptionFilter<
  Failure = SchemaOutput<ValueSchema>,
  Value = SchemaOutput<ValueSchema>
> {
  supports(cause: Failure, context: MqExecutionContext): boolean | Promise<boolean>
  catch(cause: Failure, context: MqExecutionContext): Value | Promise<Value>
}

export class MqGuardRejectedException extends Error {
  constructor() {
    super('An MQ guard rejected this invocation')
    this.name = 'MqGuardRejectedException'
  }
}
