import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { z } from 'zod'

const manifestSchema = z.object({
  repository: z.literal('nitoba/better-effect'),
  commit: z.literal('42c28fb0af7882eb048ee5d4ab1c1db81142c9dd'),
  files: z.array(z.object({ path: z.string(), gitBlob: z.string().regex(/^[a-f0-9]{40}$/) })).min(3)
})

const manifest = manifestSchema.parse(
  JSON.parse(await readFile(new URL('../tools/upstream-tooling.json', import.meta.url), 'utf8'))
)

for (const file of manifest.files) {
  const content = await readFile(new URL(`../${file.path}`, import.meta.url))
  const hash = createHash('sha1')
    .update(`blob ${content.byteLength}\0`)
    .update(content)
    .digest('hex')
  assert.equal(hash, file.gitBlob, `Upstream tooling drift: ${file.path}`)
}

const packageSchema = z.object({
  packageManager: z.literal('bun@1.4.2'),
  peerDependencies: z.object({ typescript: z.literal('>=6.0.0') })
})
packageSchema.parse(JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8')))
assert.equal((await readFile(new URL('../.bun-version', import.meta.url), 'utf8')).trim(), '1.4.2')
console.log(`Verified ${manifest.files.length} unchanged tooling files from ${manifest.commit}`)
