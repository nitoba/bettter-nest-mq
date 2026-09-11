import assert from 'node:assert/strict'
import { cpSync, mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
const root = process.cwd()
const directory = mkdtempSync(join(tmpdir(), 'packed-flow-check-'))
const run = (command, args, cwd = directory) => execFileSync(command, args, { cwd, stdio: 'inherit' })
try {
  run('bun', ['pm', 'pack', '--filename', join(directory, 'package.tgz')], root)
  cpSync(join(root, 'tests/package/consumer'), directory, { recursive: true })
  const dependencies = { 'better-nest-mq': `file:${directory}/package.tgz` }
  for (const name of ['@nestjs/common', '@nestjs/core', '@standard-schema/spec', 'reflect-metadata', 'rxjs', 'zod', 'pg', '@types/pg', '@types/node']) dependencies[name] = JSON.parse(readFileSync(join(root, 'node_modules', name, 'package.json'), 'utf8')).version
  writeFileSync(join(directory, 'package.json'), JSON.stringify({ name: 'isolated-flow-check', private: true, type: 'module', dependencies }))
  run('bun', ['install', '--ignore-scripts'])
  for (const compiler of ['typescript-minimum', 'typescript']) {
    run('node', [join(root, 'node_modules', compiler, 'bin/tsc'), '-p', 'tsconfig.flows.json'])
    run('node', ['dist/flows.js'])
    run('bun', ['dist/flows.js'])
  }
} finally { rmSync(directory, { recursive: true, force: true }) }
