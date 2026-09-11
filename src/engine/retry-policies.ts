import { methodDescriptor } from './provider-method.ts'
import { Scope } from '@nestjs/common'
import type { DiscoveryService, ModuleRef } from '@nestjs/core'
import { Retry } from 'better-effect-mq'
import type { RetryPolicy as EngineRetryPolicy } from 'better-effect-mq'
import { ContractDefinitionException } from '../contracts/errors.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { retryPolicyMetadata } from '../retries/policy.ts'
import type { MqRetryPolicy } from '../retries/policy.ts'
import type { DomainFailure } from './job-compiler.ts'
import { retryReference } from './retry-reference.ts'

type ProviderWrapper = ReturnType<DiscoveryService['getProviders']>[number]

export class RetryPolicies {
  private readonly providers = new Map<string, ProviderWrapper>()
  private readonly resolved = new Map<string, MqRetryPolicy>()

  constructor(
    discovery: DiscoveryService,
    private readonly moduleRef: ModuleRef
  ) {
    for (const wrapper of discovery.getProviders()) {
      if (wrapper.isAlias || wrapper.metatype?.prototype === undefined) continue
      const options = retryPolicyMetadata(wrapper.metatype)
      if (options === undefined) continue
      const key = JSON.stringify([options.name, options.version])
      if (this.providers.has(key))
        throw new ContractDefinitionException(`Duplicate retry policy ${key}`)
      this.providers.set(key, wrapper)
    }
  }

  require(job: RegisteredJob): MqRetryPolicy | undefined {
    const key = retryReference(job)
    if (key === undefined) return undefined
    const cached = this.resolved.get(key)
    if (cached !== undefined) return cached
    const wrapper = this.providers.get(key)
    if (wrapper === undefined)
      throw new ContractDefinitionException(`Missing registered retry policy ${key}`)
    if (
      wrapper.scope === Scope.REQUEST ||
      wrapper.scope === Scope.TRANSIENT ||
      !wrapper.isDependencyTreeStatic()
    )
      throw new ContractDefinitionException(
        'Retry policies require singleton/static dependencies because the native decision is synchronous'
      )
    const instance: MqRetryPolicy = this.moduleRef.get(wrapper.token, { strict: false })
    if (
      instance === undefined ||
      instance === null ||
      !(methodDescriptor(instance, 'decide')?.value instanceof Function)
    )
      throw new ContractDefinitionException(
        'A retry policy must implement decide(failure, context)'
      )
    this.resolved.set(key, instance)
    return instance
  }

  compile(job: RegisteredJob): EngineRetryPolicy | undefined {
    const backoff = job.policy.retry.backoff
    if (backoff?.type !== 'custom') return undefined
    return Retry.custom<DomainFailure>({
      decide: (failure, context) => {
        const provider = this.require(job)
        if (provider === undefined)
          throw new ContractDefinitionException('Missing custom retry provider')
        return provider.decide(
          failure.failure,
          Object.freeze({ ...context, name: backoff.policy, version: backoff.version })
        )
      }
    })
  }
}
