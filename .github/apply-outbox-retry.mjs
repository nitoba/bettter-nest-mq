import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, changes) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of changes) {
    assert.equal(text.split(before).length - 1, 1, `Expected one anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('src/outbox/types.ts', [["import type { PreparedJob }", "import type { OutboxRetryOptions } from './retry-options.ts'\nimport type { PreparedJob }"], ['export interface OutboxMonitor {', 'export interface OutboxMonitor {\n  retryFailed(source: string, id: string, options: OutboxRetryOptions): Promise<OutboxSnapshot>']])
patch('src/outbox/service.ts', [["import { Injectable }", "import type { OutboxRetryOptions } from './retry-options.ts'\nimport { Injectable }"], ['  get(source: string, id: string):', '  retryFailed(source: string, id: string, options: OutboxRetryOptions): Promise<OutboxSnapshot> {\n    return this.monitor.retryFailed(source, id, options)\n  }\n  get(source: string, id: string):']])
patch('src/outbox/errors.ts', [["  | 'transaction'", "  | 'transaction'\n  | 'retry'"]])
patch('src/index.ts', [["import 'reflect-metadata'", "import 'reflect-metadata'\nexport type { OutboxRetryOptions, OutboxRetryExpected } from './outbox/retry-options.ts'"]])
for (const [path, text] of staged) writeFileSync(path, text)
