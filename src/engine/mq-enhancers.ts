import { invokeMqInterceptor } from './mq-interceptor.ts'
import type { Type } from '@nestjs/common'
import type { ContextId, DiscoveryService, ModuleRef } from '@nestjs/core'
import { ContractDefinitionException } from '../contracts/errors.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { decodeSchema, encodeSchema } from '../contracts/schema.ts'
import { mqEnhancerMetadata } from '../enhancers/decorators.ts'
import { MqGuardRejectedException } from '../enhancers/types.ts'
import type {
  MqExecutionContext,
  MqGuard,
  MqPipe,
  MqInterceptor,
  MqExceptionFilter
} from '../enhancers/types.ts'
import type { FlowResultsReader } from '../flows/types.ts'
import type { JobExecutionContext } from '../workers/types.ts'
import type { ContractValue } from './job-compiler.ts'
import { methodDescriptor } from './provider-method.ts'

type Providers = ReturnType<DiscoveryService['getProviders']>

function verifyProviders<Provider>(
  providers: Providers,
  types: readonly Type<Provider>[],
  methods: readonly string[]
): void {
  for (const type of types) {
    const matches = providers.filter((wrapper) => wrapper.token === type && !wrapper.isAlias)
    const wrapper = matches[0]
    if (matches.length !== 1 || wrapper?.metatype?.prototype === undefined)
      throw new ContractDefinitionException(
        `MQ enhancer ${type.name} requires one unambiguous class provider`
      )
    for (const method of methods) {
      const descriptor = methodDescriptor(wrapper.metatype.prototype, method)
      if (descriptor === undefined || !(descriptor.value instanceof Function))
        throw new ContractDefinitionException(
          `MQ enhancer ${type.name} must implement ${method} as a method`
        )
    }
  }
}

/** Compiled before resource acquisition; invocation-scoped providers use the worker's ContextId. */
export class MqPipeline {
  private readonly guards: readonly Type<MqGuard>[]
  private readonly pipes: readonly Type<MqPipe>[]
  private readonly interceptors: readonly Type<MqInterceptor>[]
  private readonly filters: readonly Type<MqExceptionFilter>[]

  constructor(
    discovery: DiscoveryService,
    private readonly moduleRef: ModuleRef,
    private readonly target: Type,
    handler: (...args: never[]) => ContractValue,
    private readonly method: string,
    private readonly registered: RegisteredJob,
    private readonly phase: MqExecutionContext['phase']
  ) {
    const owner = mqEnhancerMetadata(target)
    const local = mqEnhancerMetadata(handler)
    this.guards = [...owner.guards, ...local.guards]
    this.pipes = [...owner.pipes, ...local.pipes]
    this.interceptors = [...owner.interceptors, ...local.interceptors]
    this.filters = [...local.filters, ...owner.filters]
    const providers = discovery.getProviders()
    verifyProviders(providers, this.guards, ['canActivate'])
    verifyProviders(providers, this.pipes, ['transform'])
    verifyProviders(providers, this.interceptors, ['intercept'])
    verifyProviders(providers, this.filters, ['supports', 'catch'])
  }

  async run(
    payload: ContractValue,
    job: JobExecutionContext,
    children: FlowResultsReader | undefined,
    contextId: ContextId,
    invoke: (value: ContractValue) => Promise<ContractValue>
  ): Promise<ContractValue> {
    let context: MqExecutionContext = Object.freeze({
      type: 'mq',
      phase: this.phase,
      worker: this.target,
      method: this.method,
      payload,
      job,
      children
    })
    const resolve = <Provider>(type: Type<Provider>): Promise<Provider> =>
      this.moduleRef.resolve(type, contextId, { strict: false })
    job.signal.throwIfAborted()
    try {
      for (const type of this.guards) {
        job.signal.throwIfAborted()
        const guard = await resolve(type)
        job.signal.throwIfAborted()
        const accepted = await guard.canActivate(context)
        if (accepted === false) throw new MqGuardRejectedException()
        if (accepted !== true)
          throw new ContractDefinitionException('MQ guards must return a boolean')
      }
      for (const type of this.pipes) {
        job.signal.throwIfAborted()
        const pipe = await resolve(type)
        job.signal.throwIfAborted()
        const transformed = await pipe.transform(context.payload, context)
        const schema = this.registered.contract.schemas.payload
        const decoded = await decodeSchema(schema, await encodeSchema(schema, transformed))
        context = Object.freeze({ ...context, payload: decoded })
      }
      const dispatch = async (index: number): Promise<ContractValue> => {
        job.signal.throwIfAborted()
        const type = this.interceptors[index]
        if (type === undefined) return await invoke(context.payload)
        const interceptor = await resolve(type)
        job.signal.throwIfAborted()
        return await invokeMqInterceptor(interceptor, context, () => dispatch(index + 1))
      }
      return await dispatch(0)
    } catch (cause) {
      job.signal.throwIfAborted()
      for (const type of this.filters) {
        const filter = await resolve(type)
        job.signal.throwIfAborted()
        const accepted = await filter.supports(cause, context)
        job.signal.throwIfAborted()
        if (accepted === true) return await filter.catch(cause, context)
        if (accepted !== false)
          throw new ContractDefinitionException('MQ filter supports must return a boolean')
      }
      throw cause
    }
  }
}
