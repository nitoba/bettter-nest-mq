import { expect, test } from 'bun:test'
import { readFileSync } from 'node:fs'

test('Kysely has an optional integration entry point, not a root-level requirement', () => {
  const manifest = JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8'))
  expect(manifest.exports['./kysely']).toBeDefined()
  expect(manifest.peerDependenciesMeta.kysely?.optional).toBe(true)
  expect(manifest.dependencies.kysely).toBeUndefined()
})
