import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { cp, mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { z } from 'zod'

const root = fileURLToPath(new URL('..', import.meta.url))
const versionSchema = z.object({ version: z.string() })
const optionalIntegrations = ['zod', 'pg', 'better-effect-mq-postgres', 'better-effect-mq-outbox']

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
  const declarations = (await readdir(join(root, 'dist'))).filter((name) => name.endsWith('.d.mts'))
  for (const file of declarations) {
    assert.doesNotMatch(
      await readFile(join(root, 'dist', file), 'utf8'),
      /from\s+['"](?:better-effect|better-result)/
    )
  }
  assert.doesNotMatch(
    await readFile(join(root, 'dist', 'index.d.mts'), 'utf8'),
    /from\s+['"](?:zod|pg)['"]/
  )

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
    // Verify optional integrations are truly absent, not accidentally supplied by workspace hoisting.
    for (const name of optionalIntegrations)
      await rm(join(directory, 'node_modules', name), { recursive: true, force: true })
    await run(
      [
        'node',
        '--input-type=module',
        '-e',
        `import { createRequire } from 'node:module'; import assert from 'node:assert/strict'; const require = createRequire(import.meta.url); for (const name of ${JSON.stringify(optionalIntegrations)}) assert.throws(() => require.resolve(name), { code: 'MODULE_NOT_FOUND' });`
      ],
      directory
    )
    await run(['node', 'node_modules/typescript/bin/tsc', '-p', 'tsconfig.json'], directory)
    await run(['node', 'dist/main.js'], directory)
    await run(['bun', 'dist/main.js'], directory)

    await writeFile(
      join(directory, 'package.json'),
      JSON.stringify({
        ...manifest,
        devDependencies: {
          ...manifest.devDependencies,
          zod: await installedVersion('zod'),
          pg: await installedVersion('pg'),
          '@types/pg': await installedVersion('@types/pg'),
          'better-effect-mq-postgres': await installedVersion('better-effect-mq-postgres'),
          'better-effect-mq-outbox': await installedVersion('better-effect-mq-outbox')
        }
      })
    )
    await run(['bun', 'install', '--ignore-scripts'], directory)
    for (const fixture of ['codec', 'postgres']) {
      await run(
        ['node', 'node_modules/typescript/bin/tsc', '-p', `tsconfig.${fixture}.json`],
        directory
      )
      await run(['node', `dist/${fixture}.js`], directory)
      await run(['bun', `dist/${fixture}.js`], directory)
    }
    console.log(
      `Packed package passed isolated root and optional integrations with TypeScript ${version}`
    )
  }
} finally {
  await rm(temporary, { recursive: true, force: true })
}
