# Execution extensions implementation plan

> Execute the approved M3.1b roadmap in the isolated `work` branch. Do not publish or change repository settings. No subagent executor is available in this session.

**Goal:** Deliver named/versioned Nest retry providers and an explicit MQ enhancer pipeline without replacing the upstream supervisor.

**Architecture:** Retry decisions are synchronous static Nest providers, adapted to native Retry.custom. An internal metadata reference fences rolling policy changes; only references and native retry timestamps persist. MQ-specific decorators compose registered providers inside the existing attempt/phase context, leaving HTTP metadata rejected.

**Baseline:** main ef06881367233c790e308191bc0b1510c3bb0b1c; Bun 1.4.2; TypeScript 6/7; unchanged 20 tooling files. No dependency upgrade, SQL migration, new runtime or mandatory consumer dependency.

## Retry providers

- [x] Run the baseline (266 passing tests) and reproduce rejection of custom producer compilation in tests/unit/custom-retry.test.ts.
- [ ] Add RetryPolicy metadata, MqRetryPolicy and context/decision contracts. Discover unique static providers; reject missing/duplicate/scoped policies before store acquisition for consumed jobs.
- [ ] Adapt native Retry.custom in job compilation. Keep producer-only contexts independent of policy implementation. Persist `__better_nest_mq_retry` as a JSON name/version tuple; reject caller overrides and different publication-time policies.
- [ ] Apply that boundary to enqueue/batch/prepare, schedules, flow child plans, outbox validation and worker/flow invocations. Reject missing/mismatched references before business code.
- [ ] Test native persisted retry timing, typed predicates and budgets, rejected/throwing/async decisions, reference mismatches, aliases and independent application contexts. Verify real PostgreSQL restart through installed packages.

## MQ enhancers

- [ ] Add explicit UseMqGuards/Pipes/Interceptors/Filters and facade-owned interfaces; test exports before implementation.
- [ ] Resolve all providers with the worker's fresh Nest ContextId. Execute class then method guards/pipes/interceptors, then handler; handle failures with nearest matching filters. Revalidate transformed decoded payloads through the existing codec boundary.
- [ ] Close interceptor next handles on return, prevent repeated dispatch, drain admitted calls, and honor cancellation before business invocation. Apply to Process, FanOut and Collect without leaking readers beyond the phase.
- [ ] Test order, scopes, inheritance/override, failures, invalid payload/results, cancellation, escaped/repeated next, startup validation and flow phases. Keep HTTP enhancer rejection.

## Qualification

- [ ] Run focused tests and negative type tests, then both compilers, full tests, formatter, build, type-aware lint and publint locally.
- [ ] Remove the temporary read-only toolchain export workflow before delivery. Open a PR with code, documentation and precise evidence; run the retained Node22/24 and live PostgreSQL installed-consumer CI on its exact head.
- [ ] Mark only delivered roadmap items complete. Durable events, ORM bridges and additional adapters remain separate work.
