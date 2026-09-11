import type { JobContract } from '../contracts/job-definition.ts'
import type { SchemaOutput, ValueSchema } from '../contracts/schema.ts'
import { MqJobException } from './errors.ts'
import type { JobAttempt, JobEnqueueItem, JobEnqueueOptions, JobScheduleOptions, JobSnapshot, JobWaitOptions, PreparedJob } from './types.ts'

export interface JobClient<Success, Failure> {
  enqueue(payloadJson: string, options?: JobEnqueueOptions): Promise<string>
  enqueueMany(items: ReadonlyArray<JobEnqueueItem<string>>): Promise<readonly string[]>
  prepare(payloadJson: string, options?: JobEnqueueOptions): Promise<PreparedJob>
  poll(id: string): Promise<JobSnapshot<Success, Failure> | undefined>
  attempts(id: string): Promise<readonly JobAttempt<Success, Failure>[]>
  awaitResult(id: string, options?: JobWaitOptions): Promise<Success>
  cancel(id: string): Promise<void>
  retry(id: string, options?: JobScheduleOptions): Promise<void>
  promote(id: string): Promise<void>
}

type ContractValue = SchemaOutput<ValueSchema>
interface Binding { readonly owner: object; readonly client: JobClient<ContractValue, ContractValue> }
// Entries belong to concrete Nest instances and are removed on shutdown; no runtime is global.
const bindings = new WeakMap<JobContract, Binding>()

export function bindJobClient(contract: JobContract, owner: object, client: JobClient<ContractValue, ContractValue>): void {
  if (bindings.has(contract)) throw new MqJobException('binding', 'A job contract instance is already bound to an application')
  bindings.set(contract, { owner, client })
}

export function unbindJobClient(contract: JobContract, owner: object): void {
  if (bindings.get(contract)?.owner === owner) bindings.delete(contract)
}

export function jobClient<Success, Failure>(contract: JobContract): JobClient<Success, Failure> {
  const binding = bindings.get(contract)
  if (binding === undefined) throw new MqJobException('binding', 'This job is not bound to a ready MQ application')
  // SAFETY: the host compiles each binding using this exact descriptor's result/failure schemas.
  return binding.client as JobClient<Success, Failure>
}
