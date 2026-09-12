import 'reflect-metadata'
import type { Type } from '@nestjs/common'
import { ContractDefinitionException } from '../contracts/errors.ts'
import type { MqGuard, MqPipe, MqInterceptor, MqExceptionFilter } from './types.ts'

const GUARDS = Symbol('mq.guards')
const PIPES = Symbol('mq.pipes')
const INTERCEPTORS = Symbol('mq.interceptors')
const FILTERS = Symbol('mq.filters')

function decorate<Provider>(
  key: symbol,
  providers: readonly Type<Provider>[]
): ClassDecorator & MethodDecorator {
  for (const provider of providers)
    if (!(provider instanceof Function) || provider.prototype === undefined)
      throw new ContractDefinitionException('MQ enhancers must be registered provider classes')
  const snapshot = Object.freeze([...providers])
  return <Target extends object>(
    target: Target,
    property?: string | symbol,
    descriptor?: PropertyDescriptor
  ) => {
    const owner = property === undefined ? target : descriptor?.value
    if (!(owner instanceof Function))
      throw new ContractDefinitionException('MQ enhancer decorators require a class or method')
    const previous: readonly Type<Provider>[] = Reflect.getOwnMetadata(key, owner) ?? []
    // Legacy decorators are applied bottom-up: prepend to retain source declaration order.
    Reflect.defineMetadata(key, Object.freeze([...snapshot, ...previous]), owner)
  }
}

export function UseMqGuards(
  ...providers: readonly Type<MqGuard>[]
): ClassDecorator & MethodDecorator {
  return decorate(GUARDS, providers)
}
export function UseMqPipes(
  ...providers: readonly Type<MqPipe>[]
): ClassDecorator & MethodDecorator {
  return decorate(PIPES, providers)
}
export function UseMqInterceptors(
  ...providers: readonly Type<MqInterceptor>[]
): ClassDecorator & MethodDecorator {
  return decorate(INTERCEPTORS, providers)
}
export function UseMqFilters(
  ...providers: readonly Type<MqExceptionFilter>[]
): ClassDecorator & MethodDecorator {
  return decorate(FILTERS, providers)
}

function inherited<Provider, Target extends object>(
  target: Target,
  key: symbol
): readonly Type<Provider>[] {
  const providers: Type<Provider>[] = []
  let current = target
  while (current !== null) {
    const own: readonly Type<Provider>[] = Reflect.getOwnMetadata(key, current) ?? []
    providers.unshift(...own)
    current = Object.getPrototypeOf(current)
  }
  return Object.freeze(providers)
}

/** Class chains compose base-first; method metadata follows the actual implementation function. */
export function mqEnhancerMetadata<Target extends object>(target: Target) {
  return {
    guards: inherited<MqGuard, Target>(target, GUARDS),
    pipes: inherited<MqPipe, Target>(target, PIPES),
    interceptors: inherited<MqInterceptor, Target>(target, INTERCEPTORS),
    filters: inherited<MqExceptionFilter, Target>(target, FILTERS)
  }
}
