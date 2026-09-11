import {
  Inject,
  Injectable,
  type OnApplicationBootstrap,
  type OnModuleDestroy
} from '@nestjs/common'
import { DiscoveryService, ModuleRef } from '@nestjs/core'

import { ContractDefinitionException } from '../contracts/errors.ts'
import { JobContract } from '../contracts/job-definition.ts'
import type { RegisteredJob } from '../contracts/queue-definition.ts'
import { bindJobClient, unbindJobClient } from '../jobs/binding.ts'
import { MqConfiguration } from '../module/mq.configuration.ts'
import { MqRegistry } from '../module/mq.registry.ts'
import { EngineSession } from './engine-session.ts'
import { compileJob, type CompiledJob } from './job-compiler.ts'
import { createJobClient } from './job-client.ts'
import { discoverWorkers } from './worker-discovery.ts'

@Injectable()
export class MqEngineHost implements OnApplicationBootstrap, OnModuleDestroy {
  readonly session: EngineSession
  private readonly bound: JobContract[] = []

  constructor(
    @Inject(MqConfiguration) private readonly configuration: MqConfiguration,
    @Inject(MqRegistry) private readonly registry: MqRegistry,
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Inject(ModuleRef) private readonly moduleRef: ModuleRef
  ) {
    this.session = new EngineSession(
      configuration.options.connections,
      configuration.options.shutdown
    )
  }

  async onApplicationBootstrap(): Promise<void> {
    this.registry.initialize()
    const entries = new Map<JobContract, { registered: RegisteredJob; compiled: CompiledJob }>()
    if (this.configuration.options.connections !== undefined) {
      for (const registered of this.registry.jobs()) {
        if (entries.has(registered.contract))
          throw new ContractDefinitionException(
            'A JobDefinition instance cannot represent multiple durable identities'
          )
        entries.set(registered.contract, { registered, compiled: compileJob(registered) })
      }
    }
    const plans =
      this.configuration.options.execution.workers &&
      this.configuration.options.connections !== undefined
        ? discoverWorkers(
            this.discovery,
            this.moduleRef,
            entries,
            this.configuration.options.shutdown
          )
        : []
    try {
      await this.session.start(this.registry.queues(), plans)
      if (this.session.state === 'ready') {
        for (const [contract, { registered, compiled }] of entries) {
          bindJobClient(contract, this, createJobClient(this.session, registered, compiled))
          this.bound.push(contract)
        }
        await this.session.activateWorkers()
      }
    } catch (cause) {
      this.detach()
      try {
        await this.session.close()
      } catch (cleanupCause) {
        throw new AggregateError([cause, cleanupCause], 'MQ activation and cleanup failed', {
          cause
        })
      }
      throw cause
    }
  }

  private detach(): void {
    for (const contract of this.bound) unbindJobClient(contract, this)
    this.bound.length = 0
  }

  async onModuleDestroy(): Promise<void> {
    this.detach()
    await this.session.close()
  }
}
