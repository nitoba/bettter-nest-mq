# Event-assisted result waits

## Delivered implementation

This continues the approved roadmap after PR #10. The facade delegates to the existing Job.awaitResult event strategy instead of adding another subscription supervisor. Public syntax: postgres({ events: true }) and job.awaitResult(id, { strategy: 'events', pollFallbackMs, timeoutMs, signal }). Polling remains the default. Missing explicit event readers reject configuration; runtime reader errors retain the engine's bounded polling fallback.

The reader is constructed from the stable raw named JobStore token and aliased to the operation token within the same private runtime. Its PostgreSQL resource shares the existing native pool and parsed JSON view. Startup resolves, checks and probes every opted-in reader before consumers activate. No required-writer activation, global parser change, automatic migration, new dependency or consumer peer was introduced.

Wait options distinguish polling intervals from event fallback intervals and reject unknown strategies, incompatible options and unsafe JavaScript timer values before execution. Caller timeout/abort ends the wait without cancelling the persisted job. The existing runtime cancellation and cleanup boundary also terminates admitted event waits during shutdown.

## Observed tests and review

The initial regression run 34691696610 reproduced polling-only rejection. The first implementation passed five behavioral tests but the compiler rejected a union passed to overloaded native wait methods; distinct literal overload calls corrected it without type casts. The expanded fixture initially had a nonconfigurable test wrapper preventing deliberate wake loss; its wrapper was made explicitly configurable without altering production behavior.

All ten focused regressions passed in run 34692177760: actual event-reader invocation, missing-reader rejection with polling still available, caller timeout, caller abort, timer validation, already-completed jobs, lost hints, runtime reader errors, shutdown cancellation and startup probe rejection. The full 303-test source suite and both TypeScript compilers subsequently passed in retained run 34692472144 before that run rejected a temporary unformatted documentation helper. The temporary tools are absent from the final tree; lint/format rules were not relaxed.

The first live package run reached every existing integration and returned all new event-wait results correctly, but its SQL inspection used the job state name `completed` instead of the persisted event type `job-completed`. The fixture now checks the documented event taxonomy and retains the requirement for ten matching events in the same job namespace. That change is test-only, not a production fallback or a reduced assertion.

Review covered resource ordering, stable namespace identity, declaration isolation, public option inference and the difference between missing reader configuration and runtime fallback. No independent subagent executor was available; the retained CI runs independent checks in parallel. No independent human review is claimed.

## Final integration gate

The final commit must pass the permanent read-only CI jobs for Node 22, Node 24 and real PostgreSQL installed consumers before merge. Historical source-only results are not a substitute for that gate. Final run IDs and merge status are recorded in PR #11, rather than predicting them in this document.

The installed fixture compiles with TypeScript 6 and 7 and runs under Node and Bun. It uses separate producer/worker application contexts, verifies scalar/null/Date values, terminal failures, cancellation, caller timeout, contract isolation, persisted event/job namespace matching, optional activation, restart reads and borrowed-pool ownership. Source regressions additionally verify lost-wake and runtime-error fallback and shutdown without waiting for the caller deadline. The previous controls, flows, schedules, transaction/outbox and JSON/parser matrix remains enabled.

## Scope limits

This increment does not expose resumable subscriptions, durable checkpoint management, retention administration, SSE endpoints or a process-wide wait multiplexer. PostgreSQL event readers may themselves poll the event log, so no zero-polling or lower-query-load guarantee is made. Events are wake-up hints; the job and its schemas remain authoritative for terminal results.

The package stays at 0.0.0 with unchanged dependency versions. No npm publication, repository settings change or production deployment is part of this delivery. User documentation is in docs/event-waits.md.
