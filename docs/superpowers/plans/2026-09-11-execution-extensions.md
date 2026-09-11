# Execution extensions implementation and qualification

**Scope:** M3.1b in PR #10, on the isolated `work` branch. Named/versioned retry providers and an explicit MQ enhancer pipeline; no publication, new runtime, dependency upgrade or repository settings change. No subagent executor was available.

**Baseline:** main `ef06881367233c790e308191bc0b1510c3bb0b1c`; Bun 1.4.2; TypeScript 6/7; 266 source tests and 20 unchanged tooling files.

## Implemented retry providers

- [x] Reproduce rejection of custom producer compilation before adding support.
- [x] Add RetryPolicy metadata, public decision contracts and unique static provider discovery. Reject missing, duplicate or scoped policies before acquiring storage for consumed jobs.
- [x] Adapt native Retry.custom without serializing callbacks. Keep producer-only contexts independent of policy implementation.
- [x] Preserve the reserved name/version reference across enqueue/batch/prepare, schedules, flow children and outbox. Reject caller overrides and mismatches before business code.
- [x] Test native retry timing, predicates/budgets, stopping/throwing/async/invalid decisions and independent application contexts.

## Implemented MQ enhancers

- [x] Reproduce missing decorator behavior before implementing the public UseMqGuards/Pipes/Interceptors/Filters API.
- [x] Resolve enhancers with the worker's fresh ContextId. Compose deterministic class/method stages and nearest supporting filters; revalidate decoded pipe outputs.
- [x] Close single-use continuations on return and drain admitted downstream calls before settlement/resource release.
- [x] Apply the same pipeline to Process, FanOut and Collect. Preserve HTTP enhancer rejection and native cancellation, schema and lease authority.
- [x] Test order, shared scopes, inheritance/overrides, Date codecs, filter outputs, invalid payloads, accessor preflight, cancellation and continuation lifecycle.

## Qualification evidence and remaining gate

The local source suite has **293 passing tests, zero failures**. Both compilers, formatting, build/declarations, publint, the 20-file tooling integrity check and diff whitespace validation passed. The native Oxlint binary crashes in this container; neither its rules nor configuration was changed. The Node 22 and Node 24 jobs of PR CI **34655507037** passed the complete quality gate, including type-aware lint and installed non-database consumers.

The first live PostgreSQL run reached the newly added extension fixture after the existing database scenarios passed, then correctly rejected its schedule write because the fixture omitted explicit reconcile mode. The fixture now sets `schedules: { mode: 'reconcile' }`; the production guard remains unchanged.

The retained, read-only CI must qualify the final head's entire PostgreSQL matrix, including the new installed-package tests for producer-only references, outbox and schedules, persisted retry delays across recreated worker contexts, and custom child retries with scoped flow enhancers. Exact final run results belong in PR #10, not inferred from previous commits. All temporary development/transfer helpers have been removed from the branch tree.

## Explicit compatibility boundary

Upstream issue **nitoba/better-effect#389** tracks the pinned engine's incompatible representations for per-child backoff overrides. The facade inherits declared child policies, permits compatible attempt-budget changes and rejects changed backoffs before manifest creation. No native engine fix or release is claimed here.

The roadmap marks only retry providers and MQ enhancers implemented. Durable events, ORM transaction bridges, other adapters and operational/release qualification remain separate work.
