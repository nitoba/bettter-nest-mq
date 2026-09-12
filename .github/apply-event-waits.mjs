import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
// Checked transport for large existing files; no source is written until every anchor matches.
const staged = new Map()
function patch(path, replacements) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after, count = 1] of replacements) {
    assert.equal(text.split(before).length - 1, count, `Unexpected anchor count: ${path}: ${before}`)
    text = text.replaceAll(before, after)
  }
  staged.set(path, text)
}
patch('src/engine/connection-definition.ts', [
  ["import type { FlowJobReads }", "import type { EventLayerFactory } from './event-plan.ts'\nimport type { FlowJobReads }"],
  ['export interface AcquiredConnection {', 'export interface AcquiredConnection {\n  readonly events?: EventLayerFactory']
])
patch('src/integrations/postgres.ts', [
  ["import { postgresFlowReads }", "import { postgresEventLayer } from './postgres-event-resource.ts'\nimport { postgresFlowReads }"],
  ['  readonly flows?: boolean', '  readonly flows?: boolean\n  /** Enable an event reader without changing the namespace event activation policy. */\n  readonly events?: boolean'],
  ['  flows: boolean\n): AcquiredConnection', '  flows: boolean,\n  events: boolean\n): AcquiredConnection'],
  ['  return result\n}', '  if (events) result = { ...result, events: (name) => postgresEventLayer(name, pool, schema, namespace) }\n  return result\n}'],
  ['  const flows = options.flows ?? false', "  const events = options.events ?? false\n  if (events !== true && events !== false) throw new MqConnectionException('<postgres>', 'configuration')\n  const flows = options.flows ?? false"],
  ['          flows\n        )', '          flows,\n          events\n        )', 2]
])
patch('src/jobs/types.ts', [[
  `export interface JobWaitOptions {
  readonly strategy?: 'polling'
  readonly pollIntervalMs?: number
  readonly timeoutMs?: number
  readonly signal?: AbortSignal
}`,
  `export type JobWaitOptions = {
  readonly timeoutMs?: number
  readonly signal?: AbortSignal
} & (
  | { readonly strategy?: 'polling'; readonly pollIntervalMs?: number; readonly pollFallbackMs?: never }
  | { readonly strategy: 'events'; readonly pollFallbackMs?: number; readonly pollIntervalMs?: never }
)`
]])
patch('src/engine/engine-session.ts', [
  ["import { flowReadStore,", "import { eventToken, eventAlias } from './event-plan.ts'\nimport type { NamedEvents, OperationEvents } from './event-plan.ts'\nimport { flowReadStore,"],
  ['        | EngineScheduler\n', '        | EngineScheduler\n        | NamedEvents\n        | OperationEvents\n'],
  ['  private acquired: AcquiredConnection[] = []', '  private acquired: AcquiredConnection[] = []\n  private readyEvents = new Set<string>()'],
  ['      const workers = Layer.merge(...this.plans.map((plan) => plan.layer))', `      const workers = Layer.merge(...this.plans.map((plan) => plan.layer))
      const eventBindings = bindings.flatMap((binding) => {
        const factory = binding.resource.events
        return factory === undefined ? [] : [{ name: binding.name, token: eventToken(binding.name), layer: factory(binding.name) }]
      })
      const eventStores = Layer.merge(...eventBindings.map((binding) => binding.layer))
      const eventAliases = Layer.merge(...eventBindings.map((binding) => eventAlias(binding.name)))`],
  ['          scheduleAliases,\n          scheduler\n', '          scheduleAliases,\n          scheduler,\n          eventStores,\n          eventAliases\n'],
  ['      const readyFlows = new Map<string, FlowStoreV2>()', `      const readyEvents = new Set<string>()
      for (const binding of eventBindings) {
        this.assertStarting()
        acquiring = binding.name
        const resolved = await runtime.run(() => Effect.gen(async function* () { return Result.ok(yield* binding.token) }))
        if (Result.isError(resolved)) throw new MqConnectionException(binding.name, 'acquire', { cause: resolved.error })
        const descriptor = resolved.value.descriptor
        if (descriptor.extension !== 'better-effect-mq/events' || descriptor.extensionVersion !== 1 || descriptor.jobStoreProtocolVersion !== 1) {
          throw new MqConnectionException(binding.name, 'protocol', { cause: new Error('Unsupported event-store protocol') })
        }
        const probe = await runtime.run(async () => ({ value: await resolved.value.tailCursor() }))
        if (Result.isError(probe.value)) throw new MqConnectionException(binding.name, 'probe', { cause: probe.value.error })
        readyEvents.add(binding.name)
      }
      const readyFlows = new Map<string, FlowStoreV2>()`],
  ['      this.readyOutboxes = readyOutboxes', '      this.readyEvents = readyEvents\n      this.readyOutboxes = readyOutboxes'],
  ['  async runOperation<Value, Failure>(', `  assertEvents(name: string): void {
    if (this.state !== 'ready' || !this.readyEvents.has(name)) {
      throw new MqJobException('awaitResult', 'Event waits require an explicitly enabled, ready event reader on the job connection')
    }
  }

  async runOperation<Value, Failure>(`],
  ['JobOperation<Value, Failure, OperationStoreToken, true>', 'JobOperation<Value, Failure, OperationStoreToken, true, OperationEvents>'],
  ['      this.ready.clear()', '      this.ready.clear()\n      this.readyEvents.clear()']
])
patch('src/engine/job-client.ts', [
  ["import { publicationRetry,", "import { validateWaitOptions } from './wait-options.ts'\nimport { operationEventToken } from './event-plan.ts'\nimport type { JobAwaitOptions } from 'better-effect-mq'\nimport type { OperationStoreToken } from './operation-store.ts'\nimport { publicationRetry,"],
  ['  session: EngineSession,\n  job: CompiledJob,', '  session: EngineSession,\n  connection: string,\n  job: CompiledJob,'],
  [`  if (options.strategy !== undefined && options.strategy !== 'polling')
    throw new MqJobException('awaitResult', 'Only polling waits are supported by this integration')
  if (options.timeoutMs !== undefined) requireInteger(options.timeoutMs, 'wait.timeoutMs')
  if (options.pollIntervalMs !== undefined)
    requireInteger(options.pollIntervalMs, 'wait.pollIntervalMs', 1)
  if (options.timeoutMs !== undefined && options.timeoutMs > 2_147_483_647)
    throw new RangeError('wait.timeoutMs exceeds the supported timer range')`,
   `  validateWaitOptions(options)
  if (options.strategy === 'events') session.assertEvents(connection)`],
  [`    const result = await session.runOperation(() =>
      job.awaitResult(id, { signal, pollIntervalMs: options.pollIntervalMs ?? 100 })
    )`,
   `    const wait: JobAwaitOptions<OperationStoreToken> = options.strategy === 'events'
      ? { strategy: 'events', eventStore: operationEventToken(connection), pollFallbackMs: options.pollFallbackMs ?? 5000, signal }
      : { signal, pollIntervalMs: options.pollIntervalMs ?? 100 }
    const result = await session.runOperation(() => job.awaitResult(id, wait))`],
  ['return boundedWait(session, job, id, options)', 'return boundedWait(session, registered.identity.connection, job, id, options)']
])
for (const [path, text] of staged) writeFileSync(path, text)
