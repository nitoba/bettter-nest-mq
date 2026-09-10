# Project bootstrap design

The user approved the Nest-native facade architecture and requested the initial repository setup using Bun and the same tooling as better-effect. The destination repository is nitoba/bettter-nest-mq; the package name is better-nest-mq.

## Scope

Deliver one ESM package, not a new monorepo or a second queue engine. Copy the original Oxlint/Oxfmt configuration and custom plugin at a pinned revision. Use Bun 1.4.2, the upstream TypeScript compiler and an independent TypeScript 6.x compatibility check, tsdown, publint and Lefthook. Add a working Nest 12 configuration module and genuine package-consumer tests.

The module accepts shutdown configuration, normalizes defaults, rejects invalid grace periods, keeps resolved configuration immutable and supports standard Nest async factories/global registration. It opens no connections and starts no background processing. The stored shutdown policy is not represented as an implemented worker lifecycle.

## Acceptance

1. Frozen Bun installation succeeds with a generated, committed lockfile.
2. Vendored configs and plugin match the pinned Git blob hashes.
3. Source checks with both TypeScript compiler versions.
4. Tests cover defaults, overrides, invalid durations, immutability, factories, global opt-in and application-context isolation.
5. ESM and declarations build with tsdown and pass publint.
6. An actual tarball installs outside the workspace, compiles with both TypeScript versions and runs under Node and Bun with real Nest constructor injection.
7. CI is read-only after bootstrap and performs no npm publication.
8. Documentation separates implemented setup from future queue, driver, flow and outbox capabilities.

## Reference

Tooling source: nitoba/better-effect at 42c28fb0af7882eb048ee5d4ab1c1db81142c9dd. Full approved architecture is recorded in docs/architecture.md; staged implementation is recorded in docs/roadmap.md.
