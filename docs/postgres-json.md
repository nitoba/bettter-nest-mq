# PostgreSQL JSON fidelity

## The corrected boundary

Issue #5 exposed more than a rejected string payload. The pinned PostgreSQL adapter expected encoded JSON text when reading JSON/JSONB fields, while node-postgres already returned decoded JavaScript values. Reinterpreting every returned string as JSON caused ordinary strings to fail and JSON-looking strings to change type. Separately, a successful JSON null result became indistinguishable from a missing SQL NULL result.

The original public Nest regression tested 18 values with a real PostgreSQL server. Ten failed before the correction: ordinary/empty/Unicode strings, strings resembling JSON values, and null-result cases. The corrected integration returns exactly the declared JSON value through the same QueueService and worker APIs.

| Input value | Required stored/read value |
| --- | --- |
| `'123'` | A string, not the number 123 |
| `'true'` | A string, not boolean true |
| `'null'` | A string, not null or an absent result |
| `'{"nested":1}'` | A string, not a nested object |
| `''` | An empty string, not absence |
| `0` and `false` | Their original primitive values |
| `null` | A valid JSON null result, distinct from SQL NULL |
| Arrays and objects | Their original JSON structure, without an added envelope |

## No application changes or data migration

Consumers still use `postgres({ pool })` or `postgres({ connectionString })` and their existing job schemas. There is no additional package, flag, schema migration or serialization wrapper to configure. All better-effect/Result/queue/adapter packages remain normal internal dependencies managed by better-nest-mq.

The stored JSON, queue identities, job versions, namespace and native connections are unchanged. This is a driver-result compatibility correction, not a new persistence format. Generic JSON schemas and codecs continue to use the normal input/decoded/wire boundaries described in contracts.md.

## Adapter-only parser isolation

A passive, non-owning pool/client view gives the pinned adapter raw text for JSON and JSONB fields through per-query type parsers. Its existing decoder then parses exactly once. SQL NULL remains native null and therefore absent where the protocol expects an optional result; JSON null arrives as encoded text until the decoder produces the valid JSON value.

The view neither mutates node-postgres global parsers nor changes the native client or pool parser configuration. Application-specific JSON parsers continue to apply to application queries on the same borrowed pool. Non-JSON parsing delegates to the actual client's configuration. Standard text-mode query results are qualified here; this is not a blanket compatibility claim for arbitrary native-driver extensions or alternate binary transport configurations.

No extra pool or runtime is created. All transaction statements use the same checked-out native client, and notification/error event handling delegates to that client. Views are cached per native pool so the existing adapter's listener reservations retain a consistent pool identity. The view cannot close the native pool. Borrowed pools remain application-owned and usable after MQ closes; owned pools retain the existing lifecycle.

The compatibility helper is private to this integration and tied to the pinned adapter behavior. Future adapter upgrades must rerun these regression suites and re-evaluate whether that adapter still expects raw JSON text. Do not remove the boundary or change global pg parsers based only on object-payload tests.

## Validation and error semantics

A valid null completion must stay null through awaitResult, poll and the attempt ledger. A completed job whose result column is actually SQL NULL remains an invalid/missing-result condition; the library must not manufacture a successful null to hide corrupt state.

The package tests inspect PostgreSQL jsonb_typeof, JSON equality and result IS NULL directly, in addition to checking returned values. They validate payloads, results and typed domain-failure content independently, including retries where the first attempt stores a failure and the next stores a result.

A query/connection failure must still release its checked-out resource exactly once and surface the original failure. Parser isolation does not retry SQL or domain operations, invent an exactly-once guarantee or modify the worker's lease/fencing protocol.

## Existing records

Correctly stored records can be read with the corrected boundary without conversion. However, updating the library cannot infer the intended value of a result already produced by an incorrectly decoded payload. A value originally intended as a string may have been interpreted by application logic as another JSON type before a previous completion was written.

Do not automatically rewrite records or replay external side effects based on that ambiguity. Audit affected jobs against their original inputs and business contracts; selectively retry only when the application's idempotency and operational rules permit it. This change deliberately performs no automatic data repair.

## Regression coverage

The 18-value matrix includes ordinary/empty/Unicode/escaped strings, JSON-looking strings, positive/negative/zero numbers, booleans, null, arrays and nested objects. The public installed-package fixture repeats it for ordinary and controlled queues with owned pools, borrowed pools and application-defined JSON parsers.

It exercises single enqueue, enqueueDecoded, enqueueMany, prepare without publication, pending jobs with absent results, successful processing, typed failures, retries, attempt history and reads after recreating producer/worker/reader application contexts. Consumer manifests do not declare any internal engine or adapter package. The actual tarball is compiled with TypeScript 6 and 7 and executed under Node and Bun against PostgreSQL.

Separate driver regressions verify JSON versus JSONB, SQL NULL, unmodified native parsing, same-backend transaction rollback, notifications and query-failure cleanup. Existing distributed-control, heartbeat-clock, recovery, ownership and optional-dependency tests remain enabled. The tooling configuration is unchanged.

Use a dedicated test database: the fixtures create/drop random schemas and deliberately terminate tagged test sessions. No production database or npm publication is part of this qualification.
