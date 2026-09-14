import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const manifest = JSON.parse(readFileSync('package.json', 'utf8'))
assert.equal(manifest.exports['./kysely'], undefined)
manifest.exports['./kysely'] = { types: './dist/kysely.d.mts', import: './dist/kysely.mjs' }
manifest.devDependencies.kysely = '0.29.5'
manifest.peerDependencies.kysely = '>=0.29.5 <0.30.0'
manifest.peerDependenciesMeta.kysely = { optional: true }
writeFileSync('package.json', JSON.stringify(manifest, null, 2) + '\n')
function patch(path, replacements) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of replacements) {
    assert.equal(text.split(before).length - 1, 1, `Expected exactly one anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  writeFileSync(path, text)
}
patch('tsdown.config.ts', [
  ["    postgres: 'src/integrations/postgres.ts'", "    postgres: 'src/integrations/postgres.ts',\n    kysely: 'src/integrations/kysely.ts'"],
  ["      'pg'\n", "      'pg',\n      'kysely'\n"]
])
patch('scripts/test-package.ts', [
  ["const optionalIntegrations = ['zod', 'pg']", "const optionalIntegrations = ['zod', 'pg', 'kysely']"],
  ["        zod: await installedVersion('zod'),", "        zod: await installedVersion('zod'),\n        kysely: await installedVersion('kysely'),"],
  ["      'event-waits'\n", "      'event-waits',\n      'kysely-outbox'\n"]
])
