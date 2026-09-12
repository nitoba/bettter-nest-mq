import 'reflect-metadata'
import { ContractDefinitionException } from '../contracts/errors.ts'
import { requireInteger, requireName } from '../contracts/policies.ts'
import type { SchemaOutput, ValueSchema } from '../contracts/schema.ts'

export interface RetryPolicyOptions {
  readonly name: string
  readonly version: number
}
export interface RetryPolicyContext extends RetryPolicyOptions {
  readonly attempt: number
  readonly attemptsMax: number
}
export type RetryPolicyDecision = boolean | { readonly retry: boolean; readonly delayMs?: number }

/** Native decisions are synchronous. Use static Nest dependencies, not request-scoped I/O. */
export interface MqRetryPolicy<Failure = SchemaOutput<ValueSchema>> {
  decide(failure: Failure, context: RetryPolicyContext): RetryPolicyDecision
}

const POLICY = Symbol('mq.retry-policy')

export function RetryPolicy(options: RetryPolicyOptions): ClassDecorator {
  requireName(options.name, 'retry.policy.name')
  requireInteger(options.version, 'retry.policy.version', 1)
  if (options.name.includes('\0') || options.name.length > 256)
    throw new ContractDefinitionException('Retry policy names must fit within 256 characters')
  const snapshot = Object.freeze({ name: options.name, version: options.version })
  return (target) => {
    if (Reflect.hasOwnMetadata(POLICY, target))
      throw new ContractDefinitionException('Duplicate @RetryPolicy decorator')
    Reflect.defineMetadata(POLICY, snapshot, target)
  }
}

export function retryPolicyMetadata<Target extends object>(
  target: Target
): RetryPolicyOptions | undefined {
  return Reflect.getOwnMetadata(POLICY, target)
}
