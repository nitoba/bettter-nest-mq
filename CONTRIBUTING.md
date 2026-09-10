# Contributing

Install Bun 1.4.2 and a supported Node version, then run:

```sh
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

The package lives at the repository root. Multiple workspaces and a task orchestrator are not needed until multiple independently packaged units exist.

Use `bun run format` before committing. Oxlint and Oxfmt intentionally match the pinned better-effect configuration; do not replace them with lookalike settings. `docs/tooling.md` explains the provenance check.

Tests are organized as unit, Nest integration, compile-time and packed-consumer checks. For behavior changes, add a failing regression test, implement the smallest complete behavior and run the complete quality gate. Do not use module-level mocks for the Nest or queue engine.

Use `bun run build` before `bun run test:package` or `bun run publint` when running those commands individually. The full `check` script builds automatically. The external consumer test uses Bun and the system Node executable, creates its files under the OS temporary directory and removes them on completion.

New runtime dependencies need a concrete implementation reason. Database drivers belong in optional integration entry points; the root module must remain independent of a particular database. Zod is a development dependency at this stage, not a required runtime validator for consumers.

No automatic npm publishing is configured. A release must be explicitly requested and pass the complete checks, including a clean packed-package consumer install.
