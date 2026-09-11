import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
function patch(path, replacements) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of replacements) {
    assert.equal(text.split(before).length - 1, 1, `Expected exactly one anchor in ${path}: ${before}`)
    text = text.replace(before, after)
  }
  writeFileSync(path, text)
}
patch('src/engine/connection-definition.ts', [
  ["import type { FlowLayerFactory }", "import type { FlowJobReads } from './flow-read-store.ts'\nimport type { FlowLayerFactory }"],
  ['export interface AcquiredConnection {', 'export interface AcquiredConnection {\n  readonly flowReads?: (name: string) => FlowJobReads']
])
patch('src/integrations/postgres.ts', [
  ["import { postgresFlowLayer }", "import { postgresFlowReads } from './postgres-flow-reads.ts'\nimport { postgresFlowLayer }"],
  ['result = { ...result, flows: (name) => postgresFlowLayer(name, pool, schema, namespace) }', 'result = { ...result, flows: (name) => postgresFlowLayer(name, pool, schema, namespace), flowReads: (name) => postgresFlowReads(name, pool, schema, namespace) }']
])
patch('src/engine/engine-session.ts', [
  ["import { flowToken, flowAlias }", "import { flowReadStore, type FlowJobReads } from './flow-read-store.ts'\nimport { flowToken, flowAlias }"],
  ['  private readyFlows = new Map<string, FlowStoreV2>()', '  private readyFlows = new Map<string, FlowStoreV2>()\n  private flowReaders = new Map<string, FlowJobReads>()'],
  ['        bindings.push({ name, connection, definition, token, resource })', '        const reads = resource.flowReads?.(name)\n        if (reads !== undefined) this.flowReaders.set(name, reads)\n        bindings.push({ name, connection, definition, token, resource, reads })'],
  ['operationStoreLayer(binding.name, queues)', 'operationStoreLayer(binding.name, queues, binding.reads, () => this.readyFlows.get(binding.name))'],
  ['        const store = resolved.value', '        const store = flowReadStore(resolved.value, binding.reads, () => this.readyFlows.get(binding.name))'],
  ['    for (const plan of this.plans) {\n      this.assertStarting()', '    for (const plan of this.plans) {\n      this.assertStarting()\n      await plan.recover(this.flowReaders)\n    }\n    for (const plan of this.plans) {\n      this.assertStarting()'],
  ['      this.readyFlows.clear()', '      this.readyFlows.clear()\n      this.flowReaders.clear()']
])
patch('src/engine/worker-plan.ts', [
  ["import type { OperationStore } from './operation-store.ts'", "import { operationStoreToken } from './operation-store.ts'\nimport type { OperationStore } from './operation-store.ts'\nimport type { FlowJobReads } from './flow-read-store.ts'"],
  ['export interface WorkerPlan {', 'export interface WorkerPlan {\n  recover(readers: ReadonlyMap<string, FlowJobReads>): Promise<void>'],
  ['  const { name: _name, ...settings } = options', '  let recoveryIds: readonly string[] = []\n  const { name: _name, ...settings } = options'],
  ['    name: options.name,\n    token,', `    name: options.name,
    token,
    async recover(readers) {
      const ids = new Set<string>()
      for (const [connection, reader] of readers) {
        const route = operationStoreToken(connection).serviceTag
        const definitions = flows.map((entry) => 'fanOut' in entry ? entry.flow : entry)
        const names = definitions.filter((entry) => entry.parent.store.serviceTag === route).map((entry) => entry.name)
        for (const id of await reader.recoveryIds(names)) ids.add(id)
      }
      recoveryIds = Object.freeze([...ids])
    },`],
  ['      flows,\n      shutdown', '      flows,\n      flowSweepFlowIds: recoveryIds,\n      shutdown']
])
patch('src/jobs/types.ts', [
  ["export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'cancelled'", "export type JobState = 'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'cancelled' | 'waiting-children'"],
  ["  | 'released'\n", "  | 'released'\n  | 'fanned-out'\n"]
])
