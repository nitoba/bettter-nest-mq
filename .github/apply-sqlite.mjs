import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, changes) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of changes) {
    assert.equal(text.split(before).length - 1, 1, `Missing or repeated anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('README.md', [
  ['event-assisted result waits and managed Kysely outbox queries are implemented.', 'event-assisted result waits, managed Kysely outbox queries and native Node/Bun SQLite job storage are implemented.'],
  ['See [Kysely outbox transactions]', 'See [SQLite job storage](docs/sqlite.md), [Kysely outbox transactions]'],
  ['## Kysely outbox transactions', `## SQLite on Node and Bun

Use \`sqlite({ path: './data/jobs.db', namespace: 'app' })\` from \`better-nest-mq/sqlite/node\` or \`better-nest-mq/sqlite/bun\`. Jobs and Worker Services keep the same API. Run \`migrateSqlite({ path })\` explicitly before startup, which validates rather than migrates.

File-backed handles open only during application acquisition and close after worker/store cleanup. A native borrowed handle remains caller-owned and keeps its pragmas unless configuration is explicitly requested. Internal SQLite adapter dependencies are installed by the library; no Effect imports or manual engine installation are needed.

This increment covers ordinary file-backed jobs, schema codecs, retries, cancellation and recovery through restarted application/worker processes. It is local embedded storage, not a multi-host broker. SQLite flow/schedule/outbox/event resources and expanded cross-process controls remain separate work. See [docs/sqlite.md](docs/sqlite.md) for host-specific usage and limits.

## Kysely outbox transactions`]
])
patch('docs/roadmap.md', [
  [`## M4 — Additional adapters and resource bundles — pending

Add MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers with optional drivers and tested topology/transaction semantics. Reuse upstream conformance/failure tests. PostgreSQL currently shares its pool among JobStore, schedules, native outbox and flows; equivalent resources for other adapters remain pending.`,
   `## M4a — Native SQLite JobStore — implemented

Separate Node DatabaseSync and Bun Database entry points wrap the existing SQLite adapter. File/borrowed configuration is inert, migrations are explicit, schemas are validated at startup, raw connection namespaces remain stable and owned handles close after store/worker cleanup. Borrowed native handles remain caller-owned. The adapter is an ordinary internal dependency, not a required consumer peer.

The package tests use native file-backed databases with separate worker processes, both TypeScript compilers and Node/Bun. They verify scalar/null/Date persistence, retry history, idempotency, cancellation, promotion, preparation without publication and namespace isolation. Root and Node declarations are compiled without Bun ambient types. See sqlite.md.

## M4b — Additional adapters and SQLite resource bundles — pending

Add MySQL, Redis/Valkey and MongoDB wrappers with tested topology/transaction semantics. Extend SQLite with qualified flow/schedule/outbox/event resources and further cross-process control/failure tests. The common upstream migration set already includes extension tables, but that does not expose their Nest APIs. PostgreSQL continues to supply its existing optional resources; native synchronous SQLite remains local embedded storage, not a multi-host database.`]
])
patch('docs/dependencies.md', [
  ['- better-effect-mq-outbox\n', '- better-effect-mq-outbox\n- better-effect-mq-sqlite\n'],
  ['## Optional Kysely integration', `## Native SQLite integration

The SQLite adapter is pinned and installed internally, just like the PostgreSQL adapter. Applications import better-nest-mq/sqlite/node with Node's DatabaseSync or better-nest-mq/sqlite/bun with Bun's Database. Native modules load only inside the selected integration; the package root imports neither. TypeScript consumers use the corresponding host typings (@types/node or @types/bun). Node/root consumers are tested with Bun typings absent.

No additional native SQLite npm driver is required by these entry points. File-backed or explicitly borrowed databases follow the ownership/migration rules in sqlite.md. Optional SQLite resource tables are not a claim of implemented flow/outbox/schedule/event facades.

## Optional Kysely integration`]
])
patch('CHANGELOG.md', [['## Unreleased', `## Unreleased

### Native SQLite JobStore

- Add isolated Node and Bun SQLite entry points with typed native handles or local file paths.
- Reuse the published SQLite adapter as an internal dependency; keep the root free of native-host imports.
- Add explicit migration/validation helpers, immutable configuration and owned/borrowed resource lifecycles with startup rollback.
- Add file-backed source and installed-package regressions for JSON/null/Date, retries, idempotency, cancellation, promotion, identity isolation, new worker processes and shutdown draining.
- Preserve TypeScript 6/7, the 20 upstream tooling files and the complete existing PostgreSQL feature matrix.
`]])
for (const path of ['AGENTS.md', 'docs/connections.md', 'docs/architecture.md']) {
  let text = readFileSync(path, 'utf8')
  assert.ok(!text.includes('## SQLite JobStore boundary'))
  text += `\n\n## SQLite JobStore boundary\n\nNative SQLite JobStore integration is implemented through separate sqlite/node and sqlite/bun entry points; read docs/sqlite.md (sqlite.md within docs). The raw named token is passed to the upstream layerFor factory. Never replace it with an unscoped make/file wrapper that loses namespace or store disposal. Native databases are acquired lazily and owned handles close only after the existing runtime releases stores. Borrowed handles keep ownership and default pragmas. Schema migration is explicit.\n\nThe released SQLite adapter remains an internal dependency. Tests must execute actual files and native host drivers, not memory queue substitutes. Keep Node/root declaration compilation independent from Bun typings and verify installed consumers under both compilers. SQLite optional resource bundles remain pending despite extension tables in its complete migration set. Do not advertise multi-host distribution or asynchronous driver execution.\n`
  staged.set(path, text)
}
patch('tests/package/consumer/sqlite-common.ts', [
  ["import { join } from 'node:path'", "import { dirname, join } from 'node:path'"],
  ["    const args =\n      runtime === 'node'\n        ? ['--experimental-sqlite', script, 'consume', path]\n        : [script, 'consume', path]\n    const child = spawn(process.execPath, args, { stdio: 'inherit' })",
   `    const target = process.argv[2] === 'cross' ? (runtime === 'node' ? 'bun' : 'node') : runtime
    const targetScript = target === runtime ? script : join(dirname(script), \`sqlite-\${target}.js\`)
    const args = target === 'node' ? ['--experimental-sqlite', targetScript, 'consume', path] : [targetScript, 'consume', path]
    const child = spawn(target, args, { stdio: 'inherit' })`],
  ["    console.log(\n      `PASS ${runtime} SQLite:", "    console.log(\n      `PASS ${runtime}->${target} SQLite:"]
])
patch('scripts/test-package.ts', [[
  "    await run(['bun', 'dist/sqlite-bun.js'], directory)",
  "    await run(['bun', 'dist/sqlite-bun.js'], directory)\n    await run(['node', '--experimental-sqlite', 'dist/sqlite-node.js', 'cross'], directory)\n    await run(['bun', 'dist/sqlite-bun.js', 'cross'], directory)"
]])
for (const [path, text] of staged) writeFileSync(path, text)
