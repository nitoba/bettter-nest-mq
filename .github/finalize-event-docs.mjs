import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, changes) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of changes) {
    assert.equal(text.split(before).length - 1, 1, `Expected one documentation anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('README.md', [
  ['Current boundaries: polling result waits only;', 'Current boundaries: polling or opt-in event-assisted result waits;'],
  ['durable events, other adapters and ORM outbox bridges remain pending.', 'resumable event subscriptions, other adapters and ORM outbox bridges remain pending.']
])
patch('docs/execution.md', [[
  'Waits currently support polling only; durable-event wakeups are not exposed in this milestone.',
  'Waits default to polling and also support the explicit events strategy with a configured event reader and bounded fallback; see event-waits.md.'
]])
for (const path of ['docs/execution.md', 'docs/connections.md', 'docs/architecture.md', 'docs/execution-extensions.md', 'AGENTS.md']) {
  let text = staged.get(path) ?? readFileSync(path, 'utf8')
  text = text.replace('Earlier references to polling-only result waits are superseded by this integration. ', '')
  text = text.replace('see docs/event-waits.md (event-waits.md from this directory)', path === 'AGENTS.md' ? 'see docs/event-waits.md' : 'see event-waits.md')
  text = text.replace('Current waits use polling.', 'Waits default to polling, with event-assisted waiting available through an explicitly configured reader.')
  text = text.replace('Current waits use polling only.', 'Waits default to polling, with event-assisted waiting available through an explicitly configured reader.')
  staged.set(path, text)
}
for (const [path, text] of staged) writeFileSync(path, text)
