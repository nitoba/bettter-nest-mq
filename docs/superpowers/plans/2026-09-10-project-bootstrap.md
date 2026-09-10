# Project Bootstrap Implementation Plan

> For agentic workers: execute this plan task-by-task using the executing-plans workflow and verify each deliverable before claiming completion.

**Goal:** Initialize the approved Nest-native queue facade repository with reproducible tooling and a tested module foundation.

**Architecture:** A root ESM package exposes a Nest configuration module. A private engine bridge and optional adapters are later milestones; there are no simulated queue APIs.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, Oxlint, Oxfmt, tsdown, publint, Lefthook.

**Spec:** docs/superpowers/specs/2026-09-10-project-bootstrap-design.md

## Global constraints

- Destination: nitoba/bettter-nest-mq; package: better-nest-mq.
- Preserve the upstream lint/format configs and plugin byte-for-byte.
- Use a real Bun-generated lockfile and frozen CI installs.
- No npm release, repository setting changes or simulated engine features.

## Task 1 — Reproducible tooling

Files: package.json, bun.lock, .bun-version, .oxlintrc.json, .oxfmtrc.json, tools/oxlint, tools/upstream-tooling.json, scripts/check-tooling.ts.

- Read the reference configuration at its pinned commit.
- Copy configs and the plugin together, record their Git blob hashes and install with Bun.
- Run `bun run check:tooling` and `bun install --frozen-lockfile`.
- Confirm TypeScript 6 and the primary compiler versions independently.

## Task 2 — Nest configuration and regression tests

Files: src/module, src/index.ts, tests/unit, tests/integration, tests/types.

- Define the minimal shutdown options and immutable resolved shape.
- Use ConfigurableModuleBuilder for forRoot/forRootAsync and opt-in global registration.
- Test defaults, zero/false, invalid values, caller-object isolation, all async factory modes and separate application contexts.
- Run `bun run typecheck`, `bun run typecheck:minimum` and `bun run test`.

## Task 3 — Package boundary

Files: tsdown.config.ts, scripts/test-package.ts, tests/package/consumer.

- Produce ESM, declarations and source maps while keeping Nest peers external.
- Run `bun run build` and `bun run publint`.
- Pack the actual package, verify the file allowlist and install in OS-temporary external projects.
- Compile external consumers with each TypeScript compiler and execute with Node and Bun.
- Clean temporary projects in a finally block.

## Task 4 — Repository handoff

Files: .github/workflows/ci.yml, lefthook.yml, README.md, CONTRIBUTING.md, AGENTS.md, docs.

- Adapt only repository paths and Nest-specific compilation settings, not the lint rules.
- Run all quality gates on the bootstrap branch, correct failures and inspect results.
- Remove the temporary write-enabled initializer, leaving a read-only CI workflow.
- Fast-forward main without rewriting history and verify the final commit's CI.
- Report exact scope, repository revision and observed check results.
