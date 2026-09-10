import { Inject, Injectable, Scope, type OnApplicationBootstrap, type OnModuleDestroy } from '@nestjs/common'
import { DiscoveryService } from '@nestjs/core'

import { ContractDefinitionException } from '../contracts/errors.ts'
import { getQueueDefinition } from '../contracts/queue-definition.ts'
import type { QueueDefinition, RegisteredJob } from '../contracts/queue-definition.ts'
import { QueueService } from '../contracts/queue-service.ts'
import { MqConfiguration } from './mq.configuration.ts'

/** Validated snapshot local to one Nest application context; no global mutable registry. */
@Injectable()
export class MqRegistry implements OnApplicationBootstrap, OnModuleDestroy {
  private queueSnapshot: ReadonlyArray<QueueDefinition> = Object.freeze([])
  private jobSnapshot: ReadonlyArray<RegisteredJob> = Object.freeze([])
  private byIdentity = new Map<string, RegisteredJob>()
  private initialized = false

  constructor(
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Inject(MqConfiguration) private readonly configuration: MqConfiguration
  ) {}

  queues(): ReadonlyArray<QueueDefinition> {
    return this.queueSnapshot
  }

  jobs(): ReadonlyArray<RegisteredJob> {
    return this.jobSnapshot
  }

  get(identity: string): RegisteredJob | undefined {
    return this.byIdentity.get(identity)
  }

  onApplicationBootstrap(): void {
    this.initialize()
  }

  /** Idempotent prerequisite for the host; avoids relying on concurrent Nest hook ordering. */
  initialize(): void {
    if (this.initialized) return
    const queues: QueueDefinition[] = []
    const jobs = new Map<string, RegisteredJob>()
    const instances = new Set<QueueService>()
    const queueKeys = new Set<string>()
    for (const wrapper of this.discovery.getProviders()) {
      const instance = wrapper.instance
      const isQueueClass = wrapper.metatype?.prototype instanceof QueueService
      if (!isQueueClass && !(instance instanceof QueueService)) continue
      if (wrapper.scope === Scope.REQUEST || wrapper.scope === Scope.TRANSIENT || !wrapper.isDependencyTreeStatic()) {
        throw new ContractDefinitionException('QueueService contracts require singleton providers with static dependency trees')
      }
      if (!(instance instanceof QueueService)) throw new ContractDefinitionException('QueueService provider was not initialized')
      if (instances.has(instance)) continue
      instances.add(instance)
      const definition = getQueueDefinition(instance, this.configuration.options.defaults)
      const queueKey = JSON.stringify([definition.connection, definition.name])
      if (queueKeys.has(queueKey)) throw new ContractDefinitionException(`Duplicate queue identity ${queueKey}`)
      queueKeys.add(queueKey)
      for (const job of definition.jobs) {
        if (jobs.has(job.identity.key)) throw new ContractDefinitionException(`Duplicate job identity ${job.identity.key}`)
        jobs.set(job.identity.key, job)
      }
      queues.push(definition)
    }
    this.queueSnapshot = Object.freeze(queues)
    this.jobSnapshot = Object.freeze([...jobs.values()])
    this.byIdentity = jobs
    this.initialized = true
  }

  onModuleDestroy(): void {
    this.queueSnapshot = Object.freeze([])
    this.jobSnapshot = Object.freeze([])
    this.byIdentity = new Map()
  }
}
