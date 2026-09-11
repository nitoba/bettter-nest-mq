# Durable schedules — approved architecture continuation

The user authorized continuing the existing roadmap after native PostgreSQL outbox. Main is ec461986e399818df1a348af26a8fc45eebf8d55 and no PR was open at start. This change implements the previously approved persistent Schedule decorator/cron/interval architecture, not an in-memory timer that enqueues independently on every replica.

## Deliverable and public boundary

Repeatable @Schedule declarations attach to initialized @Job properties in Queue Services. They contain stable group/key, exactly one cron/everyMs strategy, optional timezone, JSON input payload, metadata, job-policy overrides, misfire and overlap policy. Defaults remain explicit and documented. Payload is validated and encoded through the existing job contract before any schedule write. Dates and transformed domain values use the job's codec input representation. Definitions are copied; creating/importing a queue performs no I/O.

An opt-in postgres({ schedules: true }) resource uses the same native pool, private JSON parser view and stable raw JobStore namespace. MqSchedulesService provides get/list, registered typed upsert, pause/resume/remove, explicit reconciliation and local scheduler status/sweep. Normal startup validates registered definitions; explicit reconcile mode belongs to one coordinated deployment writer. Reconciliation ignores omissions, preserves operator pause and unchanged revision/next-run state, and does not claim cross-store atomicity. Administration is explicit and scoped to registered queue identities, not an automatic HTTP endpoint.

execution.scheduler controls the scheduler independently of workers and outbox publisher. Lazy upstream JobScheduler services live in the same private runtime as stores/Clock/Workers/outbox. Validate and prepare every declared schedule before acquisition/writes, check required store resources, reconcile/validate after store readiness and activate scheduling only after all initialization prerequisites succeed. Cleanup stops scheduler ticks before resources close, including activation failures.

## Protocol preservation

Reuse upstream cron/timezone parsing, occurrence identity, due scans, tick revision/run-at fences, misfire/overlap decisions and transactional enqueue. Do not implement another scheduler loop or occurrence algorithm. Keep raw nestjs/<connection> tokens stable; any operation-token alias is only a runtime view over the same physical schedule store. Read the actual published contract before wiring the aliases.

Coordinate two independent schedulers through PostgreSQL; claim one occurrence at most once despite competing sweeps/restart. Delivery of the emitted job remains at-least-once. A schedule definition is not an arbitrary persisted JavaScript function. Dynamic payload work should be a coordinator job.

Inspect dispatch-key support in the pinned schedule protocol. A per-key-limited destination must not silently lose its key: preserve a validated derived key through an explicit occurrence request when supported, or reject unsupported combinations clearly before writes/activation. Do not invent a schema migration or payload envelope. Do not let the scheduler bypass existing JSON fidelity, job policies, controls, native parser semantics or ownership.

## Verification

Observe the new export regression fail against the 210-test baseline. Add tests for invalid/duplicate definitions, immutable/inherited metadata, schema/codec failures, one-shot preparation, policy precedence, read-only drift handling, preservation of pauses, adapter opt-in and lifecycle isolation. Qualify actual PostgreSQL schedules: cron/timezones, intervals, misfire policies, overlap skip, pause/resume/removal, persisted occurrence IDs/results across restart and competing independent Node scheduler processes. Installed tarballs compile with TypeScript 6/7 and execute under Node/Bun; dependency consumers never declare internal engine packages. Keep all 20 tooling hashes and strict lint settings unchanged. No npm release or production deployment.

Flows, ORM transaction bridges and other adapters remain separate tasks. Do not claim missing schedule features or untested guarantees.
