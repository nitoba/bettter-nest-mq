import { ContractDefinitionException } from '../contracts/errors.ts'
import type { MqExecutionContext, MqInterceptor, MqNext } from '../enhancers/types.ts'
import type { ContractValue } from './job-compiler.ts'

/** Single-use ownership and draining prevent middleware from escaping the native attempt Scope. */
export async function invokeMqInterceptor(
  interceptor: MqInterceptor,
  context: MqExecutionContext,
  dispatch: MqNext
): Promise<ContractValue> {
  let open = true
  let admitted: Promise<ContractValue> | undefined
  let drained: Promise<void> | undefined
  let violation: ContractDefinitionException | undefined
  const next: MqNext = () => {
    if (!open || admitted !== undefined) {
      violation = new ContractDefinitionException(
        'MQ next is single-use and only valid inside its active interceptor'
      )
      const rejected = Promise.reject<ContractValue>(violation)
      void rejected.catch(() => undefined)
      return rejected
    }
    admitted = Promise.resolve().then(dispatch)
    drained = admitted.then(
      () => undefined,
      () => undefined
    )
    return admitted
  }
  let value: ContractValue
  try {
    value = await interceptor.intercept(context, next)
  } finally {
    open = false
    await drained
  }
  if (violation !== undefined) throw violation
  return value
}
