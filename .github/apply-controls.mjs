// Temporary, checked patch application in the remote development environment. Removed before merge.
import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'

function patch(path, marker, replacements) {
  let source = readFileSync(path, 'utf8')
  if (source.includes(marker)) return
  for (const [before, after] of replacements) {
    assert.equal(
      source.split(before).length - 1,
      1,
      `Expected one patch anchor in ${path}: ${before}`
    )
    source = source.replace(before, after)
  }
  writeFileSync(path, source)
}

patch('src/contracts/job-definition.ts', 'private readonly dispatchKeyFactory', [
  [
    'import { jobClient }',
    "import { validateDispatchKey } from '../controls/decorator.ts'\nimport { jobClient }"
  ],
  [
    '  abstract canRetry(failure:',
    '  abstract getDispatchKey(payload: SchemaOutput<ValueSchema>): string | undefined\n  abstract canRetry(failure:'
  ],
  [
    '  readonly idempotencyKey?:',
    '  readonly dispatchKey?: (payload: SchemaOutput<Payload>) => string | undefined\n  readonly idempotencyKey?:'
  ],
  [
    '  private readonly keyFactory:',
    "  private readonly dispatchKeyFactory: JobOptions<Payload, Result, Failure>['dispatchKey']\n  private readonly keyFactory:"
  ],
  [
    '    this.keyFactory = options.idempotencyKey',
    '    this.dispatchKeyFactory = options.dispatchKey\n    this.keyFactory = options.idempotencyKey'
  ],
  [
    '  override canRetry(failure:',
    `  override getDispatchKey(payload: SchemaOutput<Payload>): string | undefined {
    const key = this.dispatchKeyFactory?.(payload)
    return key === undefined ? undefined : validateDispatchKey(key)
  }

  private publicationOptions(payload: SchemaOutput<Payload>, options: JobEnqueueOptions | undefined): JobEnqueueOptions | undefined {
    const derived = this.getDispatchKey(payload)
    const supplied = options?.dispatchKey
    if (supplied !== undefined) validateDispatchKey(supplied)
    if (derived !== undefined && supplied !== undefined && derived !== supplied) throw new ContractDefinitionException('An explicit dispatchKey cannot override the key derived by the job contract')
    return derived === undefined ? options : { ...options, dispatchKey: derived }
  }

  override canRetry(failure:`
  ],
  [
    '    return client.enqueue(await this.encodePayload(await this.parsePayload(input)), options)',
    '    const value = await this.parsePayload(input)\n    return client.enqueue(await this.encodePayload(value), this.publicationOptions(value, options))'
  ],
  [
    '    return client.enqueue(await this.encodePayload(value), options)',
    '    return client.enqueue(await this.encodePayload(value), this.publicationOptions(value, options))'
  ],
  [
    '        const payload = await this.encodePayload(await this.parsePayload(item.payload))\n        return item.options === undefined ? { payload } : { payload, options: item.options }',
    '        const value = await this.parsePayload(item.payload)\n        const payload = await this.encodePayload(value)\n        const options = this.publicationOptions(value, item.options)\n        return options === undefined ? { payload } : { payload, options }'
  ],
  [
    '    return client.prepare(await this.encodePayload(await this.parsePayload(input)), options)',
    '    const value = await this.parsePayload(input)\n    return client.prepare(await this.encodePayload(value), this.publicationOptions(value, options))'
  ]
])

patch('src/contracts/queue-definition.ts', 'queueControlsMetadata', [
  [
    'import { getJobMetadata',
    "import { queueControlsMetadata } from '../controls/decorator.ts'\nimport type { QueueControlsOptions } from '../controls/types.ts'\nimport { getJobMetadata"
  ],
  [
    'export interface RegisteredJob {',
    'export interface RegisteredJob {\n  readonly controls?: QueueControlsOptions | undefined'
  ],
  [
    'export interface QueueDefinition {',
    'export interface QueueDefinition {\n  readonly controls?: QueueControlsOptions | undefined'
  ],
  [
    '  const properties = getJobMetadata(queue)',
    '  const controls = queueControlsMetadata(queue.constructor)\n  const properties = getJobMetadata(queue)'
  ],
  ['        property,', '        controls,\n        property,'],
  ['    jobs: Object.freeze(jobs)', '    controls,\n    jobs: Object.freeze(jobs)']
])

patch('src/engine/job-client.ts', 'registered.controls?.perKeyConcurrency', [
  [
    'import { JobFailureException }',
    "import { validateDispatchKey } from '../controls/decorator.ts'\nimport { ContractDefinitionException, JobFailureException }"
  ],
  [
    '  const { policy } = registered',
    `  if (options.dispatchKey !== undefined) validateDispatchKey(options.dispatchKey)
  if (registered.controls?.perKeyConcurrency !== undefined && options.dispatchKey === undefined) throw new ContractDefinitionException('A per-key-limited queue requires a dispatchKey on every new job')
  const { policy } = registered`
  ]
])

patch('src/engine/job-compiler.ts', 'operationStoreToken', [
  [
    "import { namedStoreToken } from './connection-definition.ts'",
    "import { operationStoreToken } from './operation-store.ts'"
  ],
  [
    "import type { NamedStoreToken } from './connection-definition.ts'",
    "import type { OperationStoreToken } from './operation-store.ts'"
  ],
  ['  NamedStoreToken\n>', '  OperationStoreToken\n>'],
  [
    '    store: namedStoreToken(identity.connection)',
    '    store: operationStoreToken(identity.connection)'
  ]
])

patch('src/engine/worker-plan.ts', 'OperationStore', [
  [
    "import type { NamedStore } from './connection-definition.ts'",
    "import type { OperationStore } from './operation-store.ts'"
  ],
  ['Layer<EngineWorker, NamedStore>', 'Layer<EngineWorker, OperationStore>']
])

patch('src/engine/engine-session.ts', 'operationStoreLayer', [
  [
    "import type { AcquiredConnection, NamedStore, NamedStoreToken } from './connection-definition.ts'",
    "import type { AcquiredConnection, NamedStore } from './connection-definition.ts'\nimport { operationStoreLayer } from './operation-store.ts'\nimport type { OperationStore, OperationStoreToken } from './operation-store.ts'"
  ],
  [
    'Runtime<NamedStore | EngineWorker | Clock>',
    'Runtime<NamedStore | OperationStore | EngineWorker | Clock>'
  ],
  [
    '      const workers = Layer.merge',
    '      const operations = Layer.merge(...bindings.map((binding) => operationStoreLayer(binding.name, queues)))\n      const workers = Layer.merge'
  ],
  [
    'Layer.merge(ClockLive, stores, workers)',
    'Layer.merge(ClockLive, stores, operations, workers)'
  ],
  [
    'JobOperation<Value, Failure, NamedStoreToken, true>',
    'JobOperation<Value, Failure, OperationStoreToken, true>'
  ]
])

patch('src/module/mq.configuration.ts', 'resolveControlsOptions', [
  [
    'import { resolveJobPolicy }',
    "import { resolveControlsOptions } from '../controls/decorator.ts'\nimport { resolveJobPolicy }"
  ],
  [
    '      defaults: resolveJobPolicy(options.defaults ?? {}),',
    '      defaults: resolveJobPolicy(options.defaults ?? {}),\n      controls: resolveControlsOptions(options.controls),'
  ]
])

patch('src/engine/mq-engine.host.ts', 'QueueControlsCoordinator', [
  [
    'import { EngineSession }',
    "import { QueueControlsCoordinator } from './queue-controls.ts'\nimport { EngineSession }"
  ],
  [
    '  readonly session: EngineSession',
    '  readonly session: EngineSession\n  readonly controls: QueueControlsCoordinator'
  ],
  [
    '      configuration.options.shutdown\n    )',
    '      configuration.options.shutdown\n    )\n    this.controls = new QueueControlsCoordinator(this.session, registry, configuration.options.controls)'
  ],
  [
    "      if (this.session.state === 'ready') {",
    "      if (this.session.state === 'ready') {\n        await this.controls.initialize()"
  ]
])

patch('src/module/mq-module.definition.ts', 'QUEUE_CONTROLS_MONITOR', [
  [
    'import { MqEngineHost }',
    "import { MqQueueControlsService, QUEUE_CONTROLS_MONITOR } from '../controls/service.ts'\nimport type { QueueControlsMonitor } from '../controls/types.ts'\nimport { MqEngineHost }"
  ],
  [
    '      MqConnectionsService,',
    `      {
        provide: QUEUE_CONTROLS_MONITOR,
        inject: [MqEngineHost],
        useFactory: (host: MqEngineHost): QueueControlsMonitor => host.controls
      },
      MqQueueControlsService,
      MqConnectionsService,`
  ],
  [
    '      MqRegistry,\n      MqConnectionsService,\n      MqWorkersService\n    ]',
    '      MqRegistry,\n      MqQueueControlsService,\n      MqConnectionsService,\n      MqWorkersService\n    ]'
  ]
])

patch('src/index.ts', "from './controls/types.ts'", [
  [
    "import 'reflect-metadata'",
    `import 'reflect-metadata'

export { QueueControls } from './controls/decorator.ts'
export { MqQueueControlsService } from './controls/service.ts'
export { QueueControlsException } from './controls/errors.ts'
export type { QueueControlsPhase } from './controls/errors.ts'
export type { QueueControlsOptions, MqControlsOptions, QueueControlsSnapshot, QueueControlsReport } from './controls/types.ts'`
  ]
])
