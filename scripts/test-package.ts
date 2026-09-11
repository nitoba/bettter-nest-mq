import { verifyOrdinaryRecovery } from '../tests/postgres/dispatch-recovery.ts'
import { verifyPostgresControlledClock } from '../tests/postgres/controlled-clock.ts'
import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { cp, mkdtemp, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { z } from 'zod'

const root = fileURLToPath(new URL('..', import.meta.url))
const versionSchema = z.object({ version: z.string() })
const optionalIntegrations = ['zod', 'pg']
const internalPackages = [
  'better-effect',
  'better-result',
  'better-effect-mq',
  'better-effect-mq-postgres',
  'better-effect-mq-outbox'
]

async function installedVersion(name: string): Promise<string> {
  const text = await readFile(join(root, 'node_modules', name, 'package.json'), 'utf8')
  return versionSchema.parse(JSON.parse(text)).version
}

async function run(command: string[], cwd: string): Promise<void> {
  const child = Bun.spawn(command, { cwd, stdout: 'inherit', stderr: 'inherit' })
  assert.equal(await child.exited, 0, `Command failed: ${command.join(' ')}`)
}

const recoveryDatabase = process.env.MQ_TEST_DATABASE_URL
if (recoveryDatabase !== undefined) {
  await verifyOrdinaryRecovery(recoveryDatabase)
  await verifyPostgresControlledClock(recoveryDatabase)
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
  for (const entry of entries)
    assert.match(
      entry,
      /^package\/(?:dist(?:\/.*)?|LICENSE|README\.md|CHANGELOG\.md|package\.json)\/?$/
    )
  const declarations = (await readdir(join(root, 'dist'))).filter((name) => name.endsWith('.d.mts'))
  for (const file of declarations)
    assert.doesNotMatch(
      await readFile(join(root, 'dist', file), 'utf8'),
      /from\s+['"](?:better-effect|better-result)/
    )
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
    for (const name of internalPackages)
      assert.equal(
        name in manifest.dependencies || name in manifest.devDependencies,
        false,
        `Consumer must not declare internal dependency ${name}`
      )
    await writeFile(join(directory, 'package.json'), JSON.stringify(manifest))
    await run(['bun', 'install', '--ignore-scripts'], directory)
    // Internal adapters install automatically. A root-only user need not install pg or Zod.
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

    const integratedManifest = {
      ...manifest,
      devDependencies: {
        ...manifest.devDependencies,
        zod: await installedVersion('zod'),
        pg: await installedVersion('pg'),
        '@types/pg': await installedVersion('@types/pg')
      }
    }
    for (const name of internalPackages)
      assert.equal(
        name in integratedManifest.dependencies || name in integratedManifest.devDependencies,
        false,
        `Integrated consumer must not declare internal dependency ${name}`
      )
    await writeFile(join(directory, 'package.json'), JSON.stringify(integratedManifest))
    await run(['bun', 'install', '--ignore-scripts'], directory)
    for (const fixture of [
      'codec',
      'postgres',
      'execution',
      'controls',
      'json-fidelity',
      'outbox',
      'schedules'
    ]) {
      await run(
        ['node', 'node_modules/typescript/bin/tsc', '-p', `tsconfig.${fixture}.json`],
        directory
      )
      await run(['node', `dist/${fixture}.js`], directory)
      await run(['bun', `dist/${fixture}.js`], directory)
    }
    console.log(
      `Packed consumers passed with TypeScript ${version}; no engine or adapter dependencies were declared by the application`
    )
  }
} finally {
  await rm(temporary, { recursive: true, force: true })
}
