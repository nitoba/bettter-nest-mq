# Complete PR #4 and internalize engine integrations

The user authorized continuing the open PR and clarified that Nest consumers must not manually install or configure better-effect, better-result, better-effect-mq or their PostgreSQL/outbox adapters. The existing architecture remains approved: engine packages are implementation dependencies, not consumer-facing peer obligations.

## Verified starting point

PR #4 started this continuation as an open draft on feat/distributed-controls. Main remained the merged M3 commit 2482d5e413e4aa6dd92fa2a0e700a2b8fe610450. The resumed regression at 5427dc14c359e671d91184a809952316471001bf passed TypeScript 6/7 and ran 164 tests: 161 passed and three failed specifically because a heartbeat advanced updatedAt beyond the mutation's sampled now. The expired-lease rejection already passed. Test-only API mistakes were corrected before interpreting these failures.

## Completed implementation

1. Controlled settlement, release and cancellation refresh only an explicitly rejected stale-clock mutation. The helper reads durable updatedAt and current wall time, never moves the requested time backward, preserves the original job ID/lease token/result, and retains retry delays. Refresh is bounded to three retries. It does not rerun handlers, replay ambiguous writes or suppress expired/replaced lease errors.
2. Deterministic tests cover completion, active cancellation, release, delayed retries, replaced/expired leases, repeated acknowledgments, bounded refresh and unrelated errors. Real PostgreSQL tests reproduce the heartbeat race and verify durable results, one attempt, cancellation and permit cleanup.
3. PostgreSQL/outbox adapters moved to normal dependencies alongside better-effect, better-result and better-effect-mq. None is a consumer peer obligation. Nest peers and optional Zod/native pg integrations remain application-facing. Bun generated the updated lockfile without changing pinned engine versions.
4. External tarball consumers declare no internal engine/adapter dependencies. Root usage is tested without pg/Zod; selected PostgreSQL/Zod integrations then use automatically installed internal adapters. Documentation now asks consumers to select only the native driver/schema library.
5. The existing immediate remote-backend teardown assertion was replaced with the same bounded server-observation check used by the ownership suite. It still fails on any remaining owned connection and does not terminate leaked clients to make the test pass.
6. Temporary branch-only generation scripts and the write-enabled development workflow are removed in the final cleanup commit. The retained CI remains read-only.

## Observed verification before the final cleanup gate

Run 34553523760 observed all clock/fencing tests passing and the two expected dependency-policy failures: 174 pass, 2 fail. After moving adapters into dependencies, the suite passed all 176 tests, both TypeScript compilers, tooling integrity, formatting, type-aware lint, build and publint.

Development run 34553967926 completed the full quality gate successfully, including real PostgreSQL and external package scenarios. The separate retained PostgreSQL job 103122600944 in run 34553970985 also passed: independent Node worker processes enforced global/per-key concurrency and fixed-window limits while parent consumers ran under Node and Bun with TypeScript 6/7. These scenarios include fast completion and a job held beyond its initial lease, cancellation permit reuse, unchanged policy revisions and recreated application contexts.

The exact final cleanup commit must pass the retained Node 22, Node 24 and PostgreSQL CI jobs before merge. Earlier successful development checks are evidence for their tested tree, not a substitute for the final-head gate. Final merge and CI identifiers belong in the PR conversation after verification.

## Deliberate boundaries

No npm publication, server deployment, automatic migrations, new runtime, second queue engine or fabricated flow/outbox functionality. PostgreSQL scalar-string payload qualification remains issue #5 and is documented as an outstanding production-release concern. Package internalization does not mean the Nest transactional-outbox API exists. Distributed policy reconciliation still requires a coordinated deployment writer and is not a cross-store transaction or leader election.
