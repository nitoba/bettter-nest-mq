# Approved architecture and implemented boundaries

## Status

M0 foundation, M1 contracts/Nest registration, M2 runtime/PostgreSQL lifecycle and M3 core producer/worker execution are implemented. The API now publishes and processes actual jobs. It is not full better-effect-mq feature parity: distributed-control APIs, custom retry providers, MQ enhancers/events, other adapters, flows, schedules and transactional outbox are still planned.

Read contracts.md, connections.md and execution.md for the exported behavior. The package remains unreleased at 0.0.0; no simulated methods stand in for missing features.

## Composition

Application code uses Nest Modules, Services, decorators, schemas and Promises. Engine-specific Effect, Result, Layer and Runtime types are private. Optional Zod and native PostgreSQL dependencies are isolated behind integration subpaths and tested as genuinely absent from root-only consumers.

Nest owns application Services. Queue properties are inert typed descriptors with stable identity and immutable policies; worker classes implement annotated methods. The root host validates the full registry and worker configuration, acquires named stores in one runtime with Clock, binds producers, then activates lazy upstream Worker layers. No runtime is created per queue, job, request or worker provider.

Per-application binding ownership uses unique symbols and concrete queue instances. Closed/unbound instances cannot access stores; producers are detached on shutdown/failure. Registries and resources are not process-global singletons. Omitting connections keeps contract-only mode without a runtime or implicit memory store.

## Contracts and persistence

Queue, Job, Retry and JobTimeout declarations preserve schema inference and distinguish metadata from runtime policy execution. Persisted identity includes connection/queue/name/version, not class/property names. The named connection token `nestjs/<name>` also contributes to PostgreSQL's namespace, so connection renaming changes the durable address.

Standard Schema describes validation; explicit codecs describe reverse encoding. Input, decoded value and JSON are different boundaries. Publication, worker reads, results and typed failures all validate against their declared contracts. JSON fidelity/round-trip checks prevent silent loss or double transforms. Application validators must be deterministic and side-effect-free.

Fixed/linear/exponential policies compile into the engine's serializable backoff representation. Total attempt budgets include the first run. Known JobFailureException content is validated before entering the typed failure channel. Unexpected exceptions remain defects and do not retry by default in this facade; explicit worker configuration can enable them. Invalid outputs are not successful jobs. Custom policy providers and zero executable timeout are rejected rather than guessed.

## Real producer operations

Job descriptors now expose enqueue/enqueueDecoded/enqueueMany, prepare, poll, awaitResult, execute, attempts, cancel, retry and promote. They use existing Job operations inside the host runtime. Batch payload validation precedes publication, while true atomicity remains adapter-specific. Prepared data is serializable with explicit routing and causes no write; it is not itself an outbox transaction.

Wait timeout/abort ends only the wait, not the durable job. execute is publish-and-wait, not a local handler call. Current waits use polling. Job lookups and mutations preserve queue/name/version guards, but those guards are not application tenant authorization. Delivery remains at-least-once and external effects need idempotency.

## Workers and DI

Worker/Process/JobData/JobContext map actual registered class providers onto the existing supervisor. The handler computation is lazy so the supervisor supplies its attempt scope, abort signal and job context. Nest constructor injection remains ordinary application DI. Request/transient-scoped dependencies get a fresh ContextId per attempt without a fabricated HTTP request.

Discovery rejects conflicting processors, missing contracts, invalid metadata, getters and ambiguous reuse of one JobDefinition for two identities before acquiring resources. Base processors are merged with subclass additions; an overridden method must carry its own parameter annotations to avoid inheriting a wrong positional map. Factory/value workers are not currently supported.

The invocation is not the Nest HTTP pipeline. Method/class guards, pipes, interceptors and filters are rejected explicitly; global HTTP enhancers do not apply. A future MQ enhancer pipeline must define and test its own execution context. Local Worker and Process concurrency limits are implemented; they are not distributed queue limits.

The engine owns claims, leases, heartbeat, retries, stalled recovery and settlement. Pending cancellation uses its atomic job operation. Active cancellation revalidates identity, writes a cancellation request and leaves the owning supervisor to settle with its lease. The facade never steals a lease or promises to undo an external effect.

## Resource ownership and lifecycle

PostgreSQL delegates to its existing adapter/migrator rather than copying queue SQL. Startup validates schemas and does not apply migrations. Borrowed pools stay caller-owned; owned pools are created lazily and closed after worker/store finalizers. Idle-client errors on owned pg pools are handled without leaking client credentials into logs.

Failed acquisition/activation rolls back locally because Nest may rethrow bootstrap failure from close before destruction hooks. Shutdown detaches producers, stops admission, drains/cooperatively aborts workers, releases stores and closes owned resources. Repeated closes are memoized and cleanup errors aggregate. No automatic process signals or hard interruption of arbitrary JavaScript/driver calls are promised.

Connection readiness reports sanitized lifecycle/protocol information; explicit probes check live connectivity. Local worker status/idle does not establish global queue completion. No administrative HTTP surface is installed automatically.

## Remaining durable features

Distributed controls are implemented through QueueControls metadata and an internal protocol-dispatch view over the existing raw store. Global/per-key concurrency and fixed-window admission call the upstream controlled operations; no local semaphore or new lease algorithm is substituted. The raw persistence token stays stable. Replica startup validates persisted policy read-only; explicit coordinated deployment reconciles without disabling omissions. See controls.md for group checks, partial multi-store deployments, typed dispatch keys and separate-process PostgreSQL qualification. Named/versioned custom retry providers must resolve through DI without persisting executable functions. Durable-event waits/subscriptions need defined cursor and recovery semantics.

Flows will retain the engine's persisted parent/children fan-out and collection model: stable manifests, bounded/paginated results and defined failure policies, with no worker slot held by waiting parents. This is not arbitrary function replay or automatic saga compensation.

Schedules will persist cron/interval/timezone/misfire/overlap decisions and reconcile safely during rolling deployments. Dynamic work belongs in coordinator jobs, not serialized functions or per-replica timers.

Application outbox must share the actual domain transaction. Preparation, append, post-commit publication and settlement remain separate, with independent publication and execution retries. SQL/MongoDB/Redis semantics are not interchangeable; each ORM bridge must establish transaction-resource identity. Application outbox is distinct from internal flow coordination. The current internal outbox dependency does not implement a Nest outbox facade.

## Verification and release

Real Nest/engine tests cover contracts, scopes, retries, cancellation, concurrency and shutdown. Actual tarballs compile with TS6/7, run with Node/Bun and verify optional dependency isolation. PostgreSQL tests prove persisted jobs/results across recreated producer/worker contexts and exercise ownership/recovery failures. Full parity still needs further distributed/crash/flow/outbox coverage before a production release; no exactly-once external effects or cross-database atomicity is claimed.

## Internal dependency ownership

The engine and its PostgreSQL/outbox adapters are installed as normal dependencies managed by this library. Nest applications do not need their own Effect/Result setup or manual adapter version selection. Only Nest/support peers and chosen native/schema integrations remain application-facing. Root isolation means no eager optional-driver loading, not the absence of internal packages from the dependency tree. See dependencies.md and controls.md for the qualified consumer and distributed-policy boundaries.
