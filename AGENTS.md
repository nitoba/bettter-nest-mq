# Repository instructions

## Scope

This repository is the Nest-native facade for the better-effect-mq ecosystem. Read README.md, docs/contracts.md, docs/architecture.md and docs/roadmap.md before changing public APIs. M0/M1 implement foundations, typed contracts, schemas and Nest registration, not an operational broker. Never add fake enqueue, no-op workers or adapters that silently fall back to memory.

## Toolchain

- Use Bun 1.4.2 for installation, scripts and tests. Commit its generated bun.lock; use frozen installs in CI.
- TypeScript >=6.0.0 is the consumer floor. Check both the primary compiler and TypeScript 6 alias.
- Keep the vendored type-aware Oxlint configuration, Oxfmt, tsdown, publint and Lefthook. Do not introduce ESLint or Prettier.
- Never weaken/exclude rules to pass a change. The upstream configs and plugin are hash-checked; deliberate updates need reviewed provenance.
- Preserve strictness, exactOptionalPropertyTypes, noUncheckedIndexedAccess, Nest legacy decorators and emitted metadata.

## Architecture

- Public APIs use Nest DI and Promises, not Effect, Result, Layer or Runtime.
- Keep the future engine bridge private. Use no runtime per request/job/provider and no process-global registry shared across application contexts.
- Contracts remain inert. Workers and resources must not start in decorators or QueueService constructors.
- Preserve Standard Schema input/output inference. Non-JSON values and non-idempotent transformations require explicit encoding; never guess an inverse.
- Keep Zod optional and imported only by its integration subpath, not the root. Validate actual packed root consumption without Zod installed.
- Queue declarations require singleton/static dependency trees. Attempt-scoped business services belong to the worker integration.
- Failed bootstrap can prevent Nest shutdown hooks from running. The engine host must roll back partially acquired resources itself.
- Outbox atomicity requires the real domain transaction. Never infer it from an argument merely named transaction.
- Storage capabilities determine distributed guarantees. Do not emulate them with local semaphores.
- Preserve at-least-once semantics, fencing, stable identities and cooperative cancellation.
- Optional database drivers must not be imported from the root entry point.

## Validation

Run `bun run check` before declaring a change complete. Add regression tests before behavior changes, and use real Nest contexts instead of module mocks. Packed-consumer tests must install an actual tarball outside the workspace with TypeScript 6 and the primary compiler, Node and Bun. Keep public declarations free of engine types.

For storage work add real database integration tests and failure-window tests. In-memory tests alone do not establish transaction, lease or distributed guarantees. Preserve complete registry validation before publishing a snapshot.

## Commits and releases

Use conventional commits. Do not publish npm packages, create releases or change repository settings without explicit authorization. The retained CI workflow is read-only and does not publish packages.
