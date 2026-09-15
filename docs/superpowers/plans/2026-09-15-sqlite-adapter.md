# SQLite JobStore integration

## Delivered boundary

M4a adds native Node DatabaseSync and Bun Database entry points, inert file/borrowed configuration, explicit migration/validation, ownership-aware cleanup and ordinary Nest producers/workers over better-effect-mq-sqlite@0.1.2. The adapter is a normal internal dependency; no consumer engine peer is introduced.

Use the upstream named SqliteJobStore.layerFor factory, not an unscoped file wrapper that loses namespace or store finalizer composition. The existing host closes owned native databases only after releasing runtime stores. There is no second runtime, global acquired handle, replacement in-memory engine or automatic migration.

Public options require exactly one source and reject accessors, unknown resource flags, invalid timers, ambiguous input and implicit in-memory filenames. Borrowed handles retain ownership and their pragmas unless explicitly configured. Owned files use WAL. Code and declarations keep native host imports behind the matching integration subpaths.

## Observed regression and verification

The initial public API regression in run 34970902687 failed because sqlite/node and sqlite/bun exports were absent. The same inspection verified release 0.1.2 from npm. Package integration generated bun.lock with Bun; existing dependency versions and tooling files were not upgraded.

The first source check found insufficient union narrowing for file versus borrowed ownership. A private discriminated Source representation fixed the actual type boundary without assertions or reduced strictness. Run 34971860819 then passed both compilers and all 16 focused SQLite tests. It also passed the installed Node SQLite scenario and the preceding PostgreSQL scenarios before a Bun external compilation failure.

That failure consisted of full Bun ambient declarations conflicting with the pinned Node types plus an unchecked test-row property access. The final Bun fixture checks the actual unmodified published SQLite module declaration, not unrelated global augmentations, and validates the native row value directly. skipLibCheck remains false and no fake native declarations are used. docs/sqlite.md records the exact boundary and remaining ambient-type limitation.

At candidate 5f6dbaa936a665b9cb027c941da2af800ca9f018, retained CI 34972533825 completed both Node quality jobs successfully. The source suite passed 353 tests with zero failures, TypeScript 6.0.3/7.0.2, formatting, type-aware lint, build, declarations, publint and all 20 original tooling hashes. Actual installed SQLite consumers passed Node-to-Node, Bun-to-Bun, Node-to-Bun and Bun-to-Node scenarios under both compilers. Those non-PostgreSQL jobs do not by themselves establish a completed live PostgreSQL matrix.

The added native lifecycle tests cover owned-handler draining, duplicate normalized paths, explicit borrowed pragma changes and failure cleanup without closing caller handles. Package fixtures verify real files and independent processes, JSON/null/Date values, retries, idempotency, cancellation, promotion, preparation and namespace isolation. Root and Node declarations are compiled before Bun types are installed.

## Final integration gate

The temporary write-enabled SQLite development workflow is removed from the final tree. The exact cleanup/documentation head must pass all permanent read-only CI jobs, including the full PostgreSQL and installed native-host package matrix, before merge. Its result and final merge identity are recorded in PR #14 rather than predicted in this plan. A previous green source job or native fixture is not a substitute for that gate.

No npm publication, production deployment or automatic release belongs to this delivery. SQLite flow/schedule/outbox/event resources and broader distributed-control/crash qualification remain separate increments; migration tables alone do not implement those facades. The package remains 0.0.0, and Node/Bun are independently tested application execution choices, not runtimes that consumers must run simultaneously.
