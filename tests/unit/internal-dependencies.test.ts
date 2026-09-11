import { expect, test } from 'bun:test'
import { readFile } from 'node:fs/promises'
import { z } from 'zod'

const PackageManifest = z.object({
  dependencies: z.record(z.string(), z.string()),
  peerDependencies: z.record(z.string(), z.string()),
  peerDependenciesMeta: z.record(z.string(), z.object({ optional: z.boolean().optional() }))
})

const manifest = PackageManifest.parse(
  JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'))
)

for (const name of [
  'better-effect',
  'better-result',
  'better-effect-mq',
  'better-effect-mq-postgres',
  'better-effect-mq-outbox'
]) {
  test(`${name} is managed internally, not a peer the Nest application must install`, () => {
    expect(manifest.dependencies[name]).toBeDefined()
    expect(manifest.peerDependencies[name]).toBeUndefined()
    expect(manifest.peerDependenciesMeta[name]).toBeUndefined()
  })
}

test('framework peers and opt-in schema/native-driver integrations remain explicit', () => {
  expect(manifest.peerDependencies['@nestjs/common']).toBeDefined()
  expect(manifest.peerDependencies['@nestjs/core']).toBeDefined()
  expect(manifest.peerDependenciesMeta['zod']?.optional).toBe(true)
  expect(manifest.peerDependenciesMeta['pg']?.optional).toBe(true)
})
