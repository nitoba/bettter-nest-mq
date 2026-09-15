# SQLite event resource delivery

## Scope

Continue M4 after the native SQLite JobStore delivery. Reuse the released adapter's scoped event reader, the existing result strategy and one application runtime/database. Expose only a boolean reader opt-in through both native entries; keep retention, required writer activation and durable subscriptions separate.

## Investigation and ordering

The proposed schedules-and-events bundle was split after reproducing scalar payload corruption directly in better-effect-mq-sqlite 0.1.2. Schedule cloneRecord parses decoded strings again, and pause rewrites the original payload. The exact baseline and reproduction are recorded in better-effect#390. No schedule facade or payload workaround is shipped. Event readers are independent of the schedule code.

## Implementation

1. Observe the missing events option failing against the original facade.
2. Add events to the typed validated native SQLite options and reject malformed/accessor values.
3. Attach the native event layer to the already acquired connection, preserving raw tokens, namespace, ownership, existing aliasing and disposal.
4. Verify native SQL reads, scalar results and execute, fallback failures, caller isolation, startup rollback and shutdown.
5. Verify actual installed consumers under Node/Bun and TS6/7, including simultaneous reader/worker processes and a killed active worker followed by replacement recovery.
6. Update guide/roadmap boundaries and remove all temporary diagnostic workflows before exact-head retained CI and review/integration.

## Acceptance

The final candidate must pass Node 22/24 quality checks and the complete PostgreSQL plus native SQLite installed-consumer matrix. Preserve all 20 tooling hashes, existing dependency versions, package version 0.0.0 and read-only retained CI. Report observed run/head results in the PR, not inferred results from an earlier commit. No npm publication, production deployment or independent-review claim.
