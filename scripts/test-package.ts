import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { cp, mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { z } from 'zod'

const root = fileURLToPath(new URL('..', import.meta.url))
const versionSchema = z.object({ version: z.string() })

async function installedVersion(name: string): Promise<string> {
  const text = await readFile(join(root, 'node_modules', name, 'package.json'), 'utf8')
  return versionSchema.parse(JSON.parse(text)).version
}

async function run(command: string[], cwd: string): Promise<void> {
  const child = Bun.spawn(command, { cwd, stdout: 'inherit', stderr: 'inherit' })
  assert.equal(await child.exited, 0, `Command failed: ${command.join(' ')}`)
}

const temporary = await mkdtemp(join(tmpdir(), 'better-nest-mq-consumer-'))
try {
  await run(['bun', 'pm', 'pack', '--destination', temporary], root)
  const archives = (await readdir(temporary)).filter((name) => name.endsWith('.tgz'))
  assert.equal(archives.length, 1, 'Expected exactly one package archive')
  const archiveName = archives[0]
  assert.ok(archiveName)
  const archive = join(temporary, archiveName)
  const entries = execFileSync('tar', ['-tzf', archive], { encoding: 'utf8' }).trim().split('\n')
  for (const entry of entries) {
    assert.match(
      entry,
      /^package\/(?:dist(?:\/.*)?|LICENSE|README\.md|CHANGELOG\.md|package\.json)\/?$/
    )
  }
  const declaration = await readFile(join(root, 'dist', 'index.d.mts'), 'utf8')
  assert.doesNotMatch(declaration, /from\s+['"](?:better-effect|better-result|zod)/)

  for (const compiler of ['typescript-minimum', 'typescript']) {
    const version = await installedVersion(compiler)
    const directory = join(temporary, compiler)
    await mkdir(directory)
    await cp(join(root, 'tests', 'package', 'consumer'), directory, { recursive: true })
    const manifest = {
      name: 'better-nest-mq-external-consumer',
      private: true,
      type: 'module',
      dependencies: {
        'better-nest-mq': pathToFileURL(archive).href,
        '@nestjs/common': await installedVersion('@nestjs/common'),
        '@nestjs/core': await installedVersion('@nestjs/core'),
        '@standard-schema/spec': await installedVersion('@standard-schema/spec'),
        'reflect-metadata': await installedVersion('reflect-metadata'),
        rxjs: await installedVersion('rxjs')
      },
      devDependencies: { typescript: version, '@types/node': await installedVersion('@types/node') }
    }
    await writeFile(join(directory, 'package.json'), JSON.stringify(manifest))
    await run(['bun', 'install', '--ignore-scripts'], directory)
    // Exercise the root with the optional peer genuinely absent, even if a package manager hoisted it.
    await rm(join(directory, 'node_modules', 'zod'), { recursive: true, force: true })
    await run(['node', 'node_modules/typescript/bin/tsc', '-p', 'tsconfig.json'], directory)
    await run(['node', 'dist/main.js'], directory)
    await run(['bun', 'dist/main.js'], directory)

    await writeFile(
      join(directory, 'package.json'),
      JSON.stringify({
        ...manifest,
        devDependencies: { ...manifest.devDependencies, zod: await installedVersion('zod') }
      })
    )
    await run(['bun', 'install', '--ignore-scripts'], directory)
    await run(['node', 'node_modules/typescript/bin/tsc', '-p', 'tsconfig.codec.json'], directory)
    await run(['node', 'dist/codec.js'], directory)
    await run(['bun', 'dist/codec.js'], directory)
    console.log(`Packed package passed Zod-free and codec consumers with TypeScript ${version}`)
  }
} finally {
  await rm(temporary, { recursive: true, force: true })
}
