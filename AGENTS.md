# Repository instructions

## Scope

This repository is the Nest-native facade for the better-effect-mq ecosystem. Read README.md, docs/architecture.md and docs/roadmap.md before adding public APIs. The current implementation is a bootstrap, not a functioning broker. Never implement a fake enqueue, a no-op worker or an adapter that silently falls back to memory.

## Toolchain

- Use Bun 1.4.2 for installation, scripts and tests. Commit bun.lock and use frozen installs in CI.
- TypeScript >=6.0.0 is the consumer floor. Check both the main compiler and the TypeScript 6 alias.
- Use the vendored Oxlint configuration with type-aware linting, Oxfmt, tsdown, publint and Lefthook. Do not introduce ESLint or Prettier.
- Do not weaken or exclude rules to make a change pass. The upstream configs and custom plugin are hash-checked; intentional updates require an explicit provenance refresh.
- Preserve strictness, exactOptionalPropertyTypes and noUncheckedIndexedAccess. Nest legacy decorators and decorator metadata must remain enabled.

## Architecture

- Public APIs use Nest dependency injection and Promise-returning methods, not Effect, Result, Layer or Runtime.
- Keep a future engine bridge internal. No runtime per request/job/provider and no global singleton shared across application contexts.
- Contracts are inert; producers do not need to import worker providers.
- Use Standard Schema for schema contracts. Bidirectional encoding must be explicit; a one-way transform is not an encoder.
- Outbox atomicity requires the actual business transaction. Never infer atomicity from an object merely named transaction.
- Distributed controls require storage capabilities. Do not emulate them with process-local semaphores.
- Preserve at-least-once semantics, fencing, stable identities and cooperative cancellation.
- Optional drivers must not be imported by the root entry point.

## Validation

Run `bun run check` before declaring a change complete. Add regression tests before behavior changes and use real Nest contexts instead of module mocks. Package tests must consume an actual tarball outside the workspace with both TypeScript 6 and the primary compiler. Keep public declarations free of engine types.

For storage work, add real database integration tests and failure-window tests. Passing in-memory tests does not establish transaction, lease or distributed guarantees.

## Commits and releases

Use conventional commits. Do not publish to npm, create releases or change repository settings without explicit authorization. The CI workflow is read-only and does not publish packages.
