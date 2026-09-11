import {
  UseMqGuards,
  type MqExecutionContext,
  type MqPipe,
  type MqRetryPolicy,
  type RetryPolicyContext
} from '../../src/index.ts'

export class SynchronousRetry implements MqRetryPolicy<{ busy: boolean }> {
  decide(failure: { busy: boolean }, context: RetryPolicyContext) {
    const version: number = context.version
    return { retry: failure.busy, delayMs: version * 10 }
  }
}
export class InvalidRetry implements MqRetryPolicy<string> {
  // @ts-expect-error Native retry decisions cannot be asynchronous.
  async decide() {
    return true
  }
}
export class InvalidPipe implements MqPipe<number> {
  // @ts-expect-error A decoded-number pipe cannot return an unvalidated string.
  transform() {
    return 'not a number'
  }
}
class InvalidGuard {
  canActivate() {
    return 'yes'
  }
}
// @ts-expect-error MQ guards must return boolean decisions.
UseMqGuards(InvalidGuard)
export function contextTypes(context: MqExecutionContext<Date>): Date {
  // @ts-expect-error A facade invocation must never expose native ownership tokens.
  void context.job.leaseToken
  return context.payload
}
