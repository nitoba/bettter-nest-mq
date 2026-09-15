import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const manifest = JSON.parse(readFileSync('package.json', 'utf8'))
assert.equal(manifest.dependencies['better-effect-mq-sqlite'], undefined)
manifest.dependencies['better-effect-mq-sqlite'] = '0.1.2'
for (const host of ['node', 'bun']) manifest.exports[`./sqlite/${host}`] = { types: `./dist/sqlite-${host}.d.mts`, import: `./dist/sqlite-${host}.mjs` }
const staged = new Map([['package.json', JSON.stringify(manifest, null, 2) + '\n']])
function patch(path, changes) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of changes) {
    assert.equal(text.split(before).length - 1, 1, `Missing or repeated anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('tsdown.config.ts', [
  ["    kysely: 'src/integrations/kysely.ts'", "    kysely: 'src/integrations/kysely.ts',\n    'sqlite-node': 'src/integrations/sqlite-node.ts',\n    'sqlite-bun': 'src/integrations/sqlite-bun.ts'"],
  ["      'kysely'\n", "      'kysely',\n      'bun:sqlite'\n"]
])
patch('tests/unit/internal-dependencies.test.ts', [["  'better-effect-mq-outbox'\n", "  'better-effect-mq-outbox',\n  'better-effect-mq-sqlite'\n"]])
patch('scripts/test-package.ts', [
  ["  'better-effect-mq-outbox'\n", "  'better-effect-mq-outbox',\n  'better-effect-mq-sqlite'\n"],
  ['    console.log(\n      `Packed consumers passed', `    // Node/root declarations were already compiled with no Bun ambient types installed.
    await run(['node', 'node_modules/typescript/bin/tsc', '-p', 'tsconfig.sqlite-node.json'], directory)
    await run(['node', '--experimental-sqlite', 'dist/sqlite-node.js'], directory)
    const bunManifest = {
      ...integratedManifest,
      devDependencies: { ...integratedManifest.devDependencies, '@types/bun': await installedVersion('@types/bun') }
    }
    await writeFile(join(directory, 'package.json'), JSON.stringify(bunManifest))
    await run(['bun', 'install', '--ignore-scripts'], directory)
    await run(['node', 'node_modules/typescript/bin/tsc', '-p', 'tsconfig.sqlite-bun.json'], directory)
    await run(['bun', 'dist/sqlite-bun.js'], directory)
    console.log(
      \`Packed consumers passed`]
])
for (const [path, text] of staged) writeFileSync(path, text)
