import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
function patch(path, marker, replacements) {
  let text = readFileSync(path, 'utf8')
  if (text.includes(marker)) return
  for (const [before, after] of replacements) {
    assert.equal(
      text.split(before).length - 1,
      1,
      `Expected one patch anchor in ${path}: ${before}`
    )
    text = text.replace(before, after)
  }
  writeFileSync(path, text)
}
patch('src/contracts/queue-definition.ts', 'assertScheduleProperties', [
  [
    'import { getJobMetadata',
    "import { assertScheduleProperties, scheduleMetadata } from '../schedules/decorator.ts'\nimport type { ScheduleOptions } from '../schedules/types.ts'\nimport { getJobMetadata"
  ],
  [
    'export interface RegisteredJob {',
    'export interface RegisteredJob {\n  readonly schedules?: readonly ScheduleOptions[]'
  ],
  [
    '  const properties = getJobMetadata(queue)',
    '  const properties = getJobMetadata(queue)\n  assertScheduleProperties(queue, new Set(properties.keys()))'
  ],
  ['        property,', '        property,\n        schedules: scheduleMetadata(queue, property),']
])
patch('src/engine/connection-definition.ts', 'ScheduleLayerFactory', [
  [
    'import type { OutboxLayerFactory }',
    "import type { ScheduleLayerFactory } from './schedule-plan.ts'\nimport type { OutboxLayerFactory }"
  ],
  [
    'export interface AcquiredConnection {',
    'export interface AcquiredConnection {\n  readonly schedules?: ScheduleLayerFactory'
  ]
])
patch('src/integrations/postgres.ts', 'postgresScheduleLayer', [
  [
    'import { postgresOutboxLayer }',
    "import { postgresScheduleLayer } from './postgres-schedule-resource.ts'\nimport { postgresOutboxLayer }"
  ],
  ['  readonly outbox?: boolean', '  readonly outbox?: boolean\n  readonly schedules?: boolean'],
  [
    '  enabled: boolean\n): AcquiredConnection',
    '  enabled: boolean,\n  schedules: boolean\n): AcquiredConnection'
  ],
  [
    '  if (!enabled) return resource\n  return { ...resource, outbox: (name) => postgresOutboxLayer(name, pool, schema, namespace) }',
    `  let result = resource
  if (enabled) result = { ...result, outbox: (name) => postgresOutboxLayer(name, pool, schema, namespace) }
  if (schedules) result = { ...result, schedules: (name) => postgresScheduleLayer(name, pool, schema, namespace) }
  return result`
  ],
  [
    '  const outbox = options.outbox ?? false',
    "  const outbox = options.outbox ?? false\n  const schedules = options.schedules ?? false\n  if (schedules !== true && schedules !== false) throw new MqConnectionException('<postgres>', 'configuration')"
  ]
])
patch('src/module/mq-module.options.ts', 'MqScheduleOptions', [
  [
    'import type { MqConnectionMap }',
    "import type { MqScheduleOptions } from '../schedules/types.ts'\nimport type { MqConnectionMap }"
  ],
  [
    'export interface MqExecutionOptions {',
    'export interface MqExecutionOptions {\n  readonly scheduler?: boolean'
  ],
  [
    'export interface MqModuleOptions {',
    'export interface MqModuleOptions {\n  readonly schedules?: MqScheduleOptions'
  ],
  [
    'export interface MqResolvedOptions {',
    'export interface MqResolvedOptions {\n  readonly schedules: Readonly<Required<MqScheduleOptions>>'
  ]
])
patch('src/module/mq.configuration.ts', 'resolveScheduleOptions', [
  [
    'import { resolveJobPolicy }',
    "import { resolveScheduleOptions } from '../schedules/decorator.ts'\nimport { resolveJobPolicy }"
  ],
  [
    '      defaults: resolveJobPolicy(options.defaults ?? {}),',
    '      defaults: resolveJobPolicy(options.defaults ?? {}),\n      schedules: resolveScheduleOptions(options.schedules),'
  ],
  [
    '        workers: options.execution?.workers ?? true,',
    '        workers: options.execution?.workers ?? true,\n        scheduler: options.execution?.scheduler ?? true,'
  ]
])
patch('src/engine/mq-engine.host.ts', 'SchedulesCoordinator', [
  [
    'import { EngineSession }',
    "import { SchedulesCoordinator } from './schedules.ts'\nimport { EngineSession }"
  ],
  [
    '  readonly session: EngineSession',
    '  readonly session: EngineSession\n  readonly schedules: SchedulesCoordinator'
  ],
  [
    '        options: configuration.options.outbox\n      }',
    '        options: configuration.options.outbox\n      },\n      { enabled: configuration.options.execution.scheduler, options: configuration.options.schedules }'
  ],
  [
    '    this.outbox = new OutboxCoordinator',
    '    this.schedules = new SchedulesCoordinator(this.session, registry, moduleRef, configuration.options.schedules)\n    this.outbox = new OutboxCoordinator'
  ],
  [
    '    try {\n      await this.session.start(this.registry.queues(), plans)',
    '    const schedules = await this.schedules.prepare()\n    try {\n      await this.session.start(this.registry.queues(), plans, schedules)'
  ],
  [
    '        await this.controls.initialize()',
    '        await this.controls.initialize()\n        await this.schedules.initialize()'
  ],
  [
    '        await this.session.activateOutboxPublisher()',
    '        await this.session.activateOutboxPublisher()\n        await this.session.activateScheduler()'
  ]
])
patch('src/index.ts', "from './schedules/service.ts'", [
  [
    "import 'reflect-metadata'",
    `import 'reflect-metadata'
export { Schedule } from './schedules/decorator.ts'
export { MqSchedulesService } from './schedules/service.ts'
export { MqScheduleException } from './schedules/errors.ts'
export type { SchedulePhase } from './schedules/errors.ts'
export type { ScheduleOptions, ScheduleMisfire, ScheduleCadence, MqScheduleOptions, ScheduleSnapshot, ScheduleReport, ScheduleListOptions, SchedulerSnapshot } from './schedules/types.ts'`
  ]
])
