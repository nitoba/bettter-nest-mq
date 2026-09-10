# Contributing

Install Bun 1.4.2 and a supported Node version, then run:

```sh
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

The root package exposes the M1 contracts and M2 connection lifecycle described in docs/contracts.md and docs/connections.md. Read the roadmap before implementing new APIs. Do not simulate planned queue execution.

Use the unchanged upstream Oxlint/Oxfmt/plugin configuration and generated Bun lockfile. See docs/tooling.md for provenance. Keep TypeScript 6 compatibility and the primary compiler check; do not relax strictness to make a test pass.

Tests cover pure contracts, real Nest contexts, the real upstream engine lifecycle, compile-time regressions and external tarball consumers. Packed tests first remove optional Zod/database modules, then exercise the optional subpaths under Node and Bun. Run build before publint/test:package when invoking them individually; the complete check builds automatically.

For database work, provide MQ_TEST_DATABASE_URL for a dedicated PostgreSQL database and run `bun run test:postgres`. Tests create/drop random schemas, inspect pg_stat_activity and deliberately terminate tagged clients. Never use a production database. With the same environment, `bun run test:package` also runs the public PostgreSQL integration against that server. CI provides an isolated PostgreSQL 16 service.

Preserve owned/borrowed resource boundaries, stable connection identity and explicit migrations. Do not expose engine types or import optional drivers from the root. Keep source and package-consumer regressions for startup/shutdown failures; workspace-only tests can hide module/chunk identity problems.

The repository has no automatic npm publication. Releases require explicit authorization and successful complete checks.
