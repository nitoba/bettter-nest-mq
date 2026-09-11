# PostgreSQL JSON fidelity — issue #5

The user authorized continued bug fixing and repository deliveries after PR #4. Start from main fcd4be44248545663fcb6201f44458d113692fa0. All engine packages and adapters remain normal internal dependencies; no user-facing Effect API or additional manual package installation is allowed.

## Investigation and deliverable

Reproduce scalar persistence through actual Nest QueueService APIs with a real PostgreSQL server. Cover strings that resemble JSON, empty/Unicode strings, numbers, booleans, JSON null, arrays and objects. Distinguish publication, handler input, result storage, failure storage and post-restart reads. The pinned adapter's parseJson currently parses every string, while node-postgres already decodes JSON/JSONB; confirm this against installed code and data before modifying the boundary.

If confirmed, normalize only the adapter's driver-result representation using supported per-query type parsers. Do not change stored envelopes, reinterpret application strings heuristically, change global pg parsers, mutate a borrowed pool/client, override application query behavior or open a second pool. Preserve SQL NULL versus JSON null, ordinary/native type handling, listener/transaction behavior and resource ownership. The adapter remains responsible for SQL and queue protocols. Do not patch files under node_modules at install time or require a new unpublished dependency release.

## Acceptance

Public enqueue, enqueueDecoded, batch publication, preparation, worker input/output and typed failure content must retain exact valid JSON values, including false/zero/empty string/null. Read persisted results after closing and reopening application contexts; inspect jsonb_typeof and SQL null state rather than relying on returned IDs. Include borrowed and owned pools, controlled and ordinary queues, root imports without optional drivers and installed tarball consumers with TypeScript 6/7 under Node/Bun.

Native application queries before/during/after MQ must retain their own parser semantics, and borrowed pools must remain usable. No global parser mutation, runtime duplication, database migration or npm release. Keep the 20 tooling hashes and existing regression suites. Remove temporary development workflows before the verified main merge and close issue #5 only when its regression coverage passes.
