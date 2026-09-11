import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

function patch(path, marker, replacements) {
  let text = readFileSync(path, 'utf8')
  if (text.includes(marker)) return
  for (const [before, after] of replacements) {
    assert.equal(text.split(before).length - 1, 1, `Expected one anchor in ${path}: ${before}`)
    text = text.replace(before, after)
  }
  writeFileSync(path, text)
}
patch('src/integrations/postgres-json-pool.ts', 'export function postgresJsonClient', [
  [
    'function jsonClient(client: PoolClient)',
    'export function postgresJsonClient(client: PoolClient)'
  ],
  ['return jsonClient(await pool.connect())', 'return postgresJsonClient(await pool.connect())'],
  [
    'jsonClient(client).query<Row>(text, values)',
    'postgresJsonClient(client).query<Row>(text, values)'
  ]
])
patch('src/engine/connection-definition.ts', 'OutboxLayerFactory', [
  [
    'import { JobStore }',
    "import type { OutboxLayerFactory } from './outbox-plan.ts'\nimport { JobStore }"
  ],
  [
    'export interface AcquiredConnection {',
    'export interface AcquiredConnection {\n  readonly outbox?: OutboxLayerFactory'
  ]
])
patch('src/module/mq.configuration.ts', 'resolveOutboxOptions', [
  [
    'import { resolveJobPolicy }',
    "import { resolveOutboxOptions } from '../outbox/options.ts'\nimport { resolveJobPolicy }"
  ],
  [
    '      defaults: resolveJobPolicy(options.defaults ?? {}),',
    '      defaults: resolveJobPolicy(options.defaults ?? {}),\n      outbox: resolveOutboxOptions(options.outbox),'
  ],
  [
    'execution: Object.freeze({ workers: options.execution?.workers ?? true })',
    'execution: Object.freeze({ workers: options.execution?.workers ?? true, outboxPublisher: options.execution?.outboxPublisher ?? true })'
  ]
])
patch('src/engine/mq-engine.host.ts', 'OutboxCoordinator', [
  [
    'import { QueueControlsCoordinator }',
    "import { OutboxCoordinator } from './outbox-coordinator.ts'\nimport { QueueControlsCoordinator }"
  ],
  [
    '  readonly controls: QueueControlsCoordinator',
    '  readonly controls: QueueControlsCoordinator\n  readonly outbox: OutboxCoordinator'
  ],
  [
    '      configuration.options.shutdown\n    )',
    '      configuration.options.shutdown,\n      { enabled: configuration.options.execution.outboxPublisher, options: configuration.options.outbox }\n    )\n    this.outbox = new OutboxCoordinator(this.session, registry)'
  ],
  [
    '        await this.session.activateWorkers()',
    '        await this.session.activateWorkers()\n        await this.session.activateOutboxPublisher()'
  ]
])
patch('src/module/mq-module.definition.ts', 'bindOutboxService', [
  [
    'import { MqEngineHost }',
    "import { MqOutboxService } from '../outbox/service.ts'\nimport { bindOutboxService } from '../engine/outbox-coordinator.ts'\nimport { MqEngineHost }"
  ],
  [
    '      MqEngineHost,',
    '      MqEngineHost,\n      { provide: MqOutboxService, inject: [MqEngineHost], useFactory: (host: MqEngineHost): MqOutboxService => bindOutboxService(new MqOutboxService(host.outbox), host.outbox) },'
  ],
  [
    '    exports: [\n      ...(definition.exports ?? []),',
    '    exports: [\n      ...(definition.exports ?? []),\n      MqOutboxService,'
  ]
])
patch('src/index.ts', "from './outbox/types.ts'", [
  [
    "import 'reflect-metadata'",
    `import 'reflect-metadata'

export { MqOutboxService } from './outbox/service.ts'
export { MqOutboxException } from './outbox/errors.ts'
export type { OutboxPhase } from './outbox/errors.ts'
export type { OutboxEntry, OutboxState, OutboxSnapshot, OutboxAppendResult, OutboxCounts, OutboxListOptions, OutboxPublisherSnapshot, OutboxFailure, MqOutboxOptions } from './outbox/types.ts'`
  ]
])
patch('src/engine/engine-session.ts', 'activateOutboxPublisher', [
  [
    "import { Result } from 'better-result'",
    `import { Result } from 'better-result'
import type { OutboxStore, OutboxPublisherHandle } from 'better-effect-mq-outbox'
import { MqOutboxException } from '../outbox/errors.ts'
import { resolveOutboxOptions } from '../outbox/options.ts'
import type { MqOutboxOptions, OutboxPublisherSnapshot } from '../outbox/types.ts'
import { outboxToken, outboxPublisherLayer, Publisher } from './outbox-plan.ts'
import type { NamedOutbox, EngineOutboxPublisher } from './outbox-plan.ts'`
  ],
  [
    'Runtime<NamedStore | OperationStore | EngineWorker | Clock>',
    'Runtime<NamedStore | OperationStore | EngineWorker | Clock | NamedOutbox | EngineOutboxPublisher>'
  ],
  [
    '  private acquired: AcquiredConnection[] = []',
    `  private acquired: AcquiredConnection[] = []
  private readonly outboxOptions: Readonly<Required<MqOutboxOptions>>
  private readonly publisherEnabled: boolean
  private readyOutboxes = new Map<string, OutboxStore>()
  private runningPublisher: OutboxPublisherHandle | undefined
  private publishing: Promise<void> | undefined
  private hasPublisher = false`
  ],
  [
    '  constructor(connections: MqConnectionMap | undefined, shutdown: MqShutdownOptions = {}) {',
    `  constructor(connections: MqConnectionMap | undefined, shutdown: MqShutdownOptions = {}, outbox: { enabled?: boolean; options?: MqOutboxOptions } = {}) {
    this.outboxOptions = resolveOutboxOptions(outbox.options)
    this.publisherEnabled = outbox.enabled ?? true`
  ],
  [
    '      const workers = Layer.merge(...this.plans.map((plan) => plan.layer))',
    `      const workers = Layer.merge(...this.plans.map((plan) => plan.layer))
      const outboxBindings = bindings.flatMap((binding) => {
        const factory = binding.resource.outbox
        return factory === undefined ? [] : [{ name: binding.name, token: outboxToken(binding.name), layer: factory(binding.name) }]
      })
      const outboxes = Layer.merge(...outboxBindings.map((binding) => binding.layer))
      this.hasPublisher = this.publisherEnabled && outboxBindings.length > 0
      const publisher = this.hasPublisher ? outboxPublisherLayer(outboxBindings.map((binding) => binding.name), bindings.map((binding) => binding.name), this.outboxOptions) : Layer.empty`
  ],
  [
    'Layer.merge(ClockLive, stores, operations, workers)',
    'Layer.merge(ClockLive, stores, operations, workers, outboxes, publisher)'
  ],
  [
    '      this.assertStarting()\n      this.ready = ready',
    `      const readyOutboxes = new Map<string, OutboxStore>()
      for (const binding of outboxBindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() => Effect.gen(async function* () { return Result.ok(yield* binding.token) }))
        if (Result.isError(resolved)) throw new MqOutboxException('unavailable', 'Outbox store acquisition failed', { cause: resolved.error })
        if (resolved.value.descriptor.protocolVersion !== 1) throw new MqOutboxException('configuration', 'Unsupported outbox protocol')
        const counts = await runtime.run(async () => ({ value: await resolved.value.counts() }))
        if (Result.isError(counts.value)) throw new MqOutboxException('unavailable', 'Outbox storage probe failed', { cause: counts.value.error })
        readyOutboxes.set(binding.name, resolved.value)
      }
      this.assertStarting()
      this.readyOutboxes = readyOutboxes
      this.ready = ready`
  ],
  [
    'cause instanceof MqConnectionException || cause instanceof MqEngineStateException',
    'cause instanceof MqConnectionException || cause instanceof MqEngineStateException || cause instanceof MqOutboxException'
  ],
  [
    '  async runOperation<Value, Failure>(',
    `  activateOutboxPublisher(): Promise<void> {
    this.publishing ??= this.activatePublisher()
    return this.publishing
  }

  private async activatePublisher(): Promise<void> {
    if (!this.hasPublisher) return
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined) throw new MqOutboxException('unavailable', 'The outbox runtime is not ready')
    this.assertStarting()
    const resolved = await runtime.run(() => Effect.gen(async function* () { return Result.ok(yield* Publisher) }))
    if (Result.isError(resolved)) throw new MqOutboxException('unavailable', 'The outbox publisher could not start', { cause: resolved.error })
    this.runningPublisher = resolved.value
    this.assertStarting()
  }

  outboxPublisher(): OutboxPublisherSnapshot | undefined {
    const publisher = this.runningPublisher
    return publisher === undefined ? undefined : Object.freeze({ id: publisher.id, state: publisher.state, activeCount: publisher.activeCount })
  }

  async withOutbox<Value>(name: string, operation: (store: OutboxStore) => Value | PromiseLike<Value>): Promise<Value> {
    const runtime = this.runtime
    if (this.state !== 'ready' || runtime === undefined) throw new MqOutboxException('unavailable', 'The outbox runtime is not ready')
    const store = this.readyOutboxes.get(name)
    if (store === undefined) throw new MqOutboxException('unavailable', 'This connection has no enabled outbox')
    const result = await runtime.run(async () => ({ value: await operation(store) }))
    return result.value
  }

  async runOperation<Value, Failure>(`
  ],
  [
    '      if (this.activating !== undefined) await Promise.allSettled([this.activating])',
    '      if (this.activating !== undefined) await Promise.allSettled([this.activating])\n      if (this.publishing !== undefined) await Promise.allSettled([this.publishing])'
  ],
  [
    '      this.ready.clear()',
    '      this.ready.clear()\n      this.readyOutboxes.clear()\n      this.runningPublisher = undefined'
  ]
])
