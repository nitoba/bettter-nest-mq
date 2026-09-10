# Tooling provenance and adaptations

## Exact copies

Reference: `nitoba/better-effect`, commit `42c28fb0af7882eb048ee5d4ab1c1db81142c9dd`.

The root Oxlint and Oxfmt JSON files and all of `tools/oxlint` are copied without changes. This includes the 15 anti-slop rules and their shared helpers. `tools/upstream-tooling.json` records the original Git blob for every copied file; `scripts/check-tooling.ts` recomputes each blob hash from the working tree.

This protects more than a similar set of lint rules: comments, paths and rule implementations are checked byte-for-byte. The provenance check performs no network access.

## Versions

Bun 1.4.2 and the Oxlint, Oxfmt, oxlint-tsgolint, @oxlint/plugins, tsdown, publint and Lefthook dependency specifications follow the reference root or better-effect-mq package. TypeScript uses the same 7.x development compiler, with an additional `typescript-minimum` alias restricted to 6.x. The public TypeScript peer floor is >=6.0.0 and optional for JavaScript-only consumers.

The generated bun.lock is the source of resolved versions; it is not hand-written. CI installs with --frozen-lockfile.

## Necessary Nest adaptations

The strict compiler flags follow packages/better-effect-mq/tsconfig.json. This project adds legacy decorators, emitted decorator metadata and Node types. The library build targets Node ESM and ES2023. The external consumer independently compiles with NodeNext and emitted metadata, preventing the Bun source runner from hiding package or decorator problems.

Lefthook uses the same lint/format/typecheck tools and staged-file behavior, with globs adapted to a single-package repository. Build entries, public package exports and CI paths are specific to this library. Turbo is not added for one package.

Vendored plugin source is integrity-checked rather than rewritten or linted as application code. The project's lint command targets src, tests, scripts and the build configuration under the unchanged root rules.

## Updating the baseline

Choose and review a new upstream commit first. Copy the two JSON configs and plugin files together; refresh the Git blob manifest from that same revision, and update the pinned revision in the provenance validator. Update dependency specifications deliberately, regenerate bun.lock with Bun, and run the complete check plus external consumer tests. Do not edit hashes simply to approve an unexplained local rule change.
