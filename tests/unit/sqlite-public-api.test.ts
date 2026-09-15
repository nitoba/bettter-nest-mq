import { expect, test } from 'bun:test'
import manifest from '../../package.json'

test('SQLite integrations are optional entry points rather than public engine peers', () => {
  expect(Object.keys(manifest.exports)).toContain('./sqlite/node')
  expect(Object.keys(manifest.exports)).toContain('./sqlite/bun')
  expect(Object.keys(manifest.dependencies)).toContain('better-effect-mq-sqlite')
  expect(Object.keys(manifest.peerDependencies)).not.toContain('better-effect-mq-sqlite')
})
