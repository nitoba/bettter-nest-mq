# Approved architecture and implemented boundaries

## Status

M0 foundation, M1 contracts/Nest registration, M2 runtime/PostgreSQL lifecycle, M3 core execution, distributed controls, native PostgreSQL outbox, persistent schedules and durable PostgreSQL flows are implemented. The API now publishes and processes actual jobs and persisted fan-out/collect workflows. Named/versioned retry providers and explicit MQ enhancers are also implemented. Full parity still requires durable events, other adapters and ORM transaction bridges.

Read flows.md, schedules.md, outbox.md, contracts.md, connections.md and execution.md for the exported behavior. The package remains unreleased at 0.0.0; no simulated methods stand in for missing features.

## Composition

Application code uses Nest Modules, Services, decorators, schemas and Promises. Engine-specific Effect, Result, Layer and Runtime types are private. Optional Zod and native PostgreSQL dependencies are isolated behind integration subpaths and tested as genuinely absent from root-only consumers.

Nest owns application Services. Queue properties are inert typed descriptors with stable identity and immutable policies; worker classes implement annotated methods. The root host validates the full registry and worker configuration, acquires named stores in one runtime with Clock, binds producers, then activates lazy upstream Worker layers. No runtime is created per queue, job, request or worker provider.

Per-application binding ownership uses unique symbols and concrete queue instances. Closed/unbound instances cannot access stores; producers are detached on shutdown/failure. Registries and resources are not process-global singletons. Omitting connections keeps contract-only mode without a runtime or implicit memory store.

## Contracts and persistence

Queue, Job, Retry and JobTimeout declarations preserve schema inference and distinguish metadata from runtime policy execution. Persisted identity includes connection/queue/name/version, not class/property names. The named connection token `nestjs/<name>` also contributes to PostgreSQL's namespace, so connection renaming changes the durable address.

Standard Schema describes validation; explicit codecs describe reverse encoding. Input, decoded value and JSON are different boundaries. Publication, worker reads, results and typed failures all validate against their declared contracts. JSON fidelity/round-trip checks prevent silent loss or double transforms. Application validators must be deterministic and side-effect-free.

Fixed/linear/exponential policies compile into the engine's serializable backoff representation. Total attempt budgets include the first run. Known JobFailureException content is validated before entering the typed failure channel. Unexpected exceptions remain defects and do not retry by default in this facade; explicit worker configuration can enable them. Invalid outputs are not successful jobs. Custom policies use explicitly registered, named/versioned static Nest providers and the native synchronous retry hook. Zero executable timeout remains rejected.

## Real producer operations

Job descriptors now expose enqueue/enqueueDecoded/enqueueMany, prepare, poll, awaitResult, execute, attempts, cancel, retry and promote. They use existing Job operations inside the host runtime. Batch payload validation precedes publication, while true atomicity remains adapter-specific. Prepared data is serializable with explicit routing and causes no write; it is not itself an outbox transaction.

Wait timeout/abort ends only the wait, not the durable job. execute is publish-and-wait, not a local handler call. Waits default to polling, with event-assisted waiting available through an explicitly configured reader. Job lookups and mutations preserve queue/name/version guards, but those guards are not application tenant authorization. Delivery remains at-least-once and external effects need idempotency.

## Workers and DI

Worker/Process/JobData/JobContext map actual registered class providers onto the existing supervisor. The handler computation is lazy so the supervisor supplies its attempt scope, abort signal and job context. Nest constructor injection remains ordinary application DI. Request/transient-scoped dependencies get a fresh ContextId per attempt without a fabricated HTTP request.

Discovery rejects conflicting processors, missing contracts, invalid metadata, getters and ambiguous reuse of one JobDefinition for two identities before acquiring resources. Base processors are merged with subclass additions; an overridden method must carry its own parameter annotations to avoid inheriting a wrong positional map. Factory/value workers are not currently supported. The pinned supervisor also requires queue/name/version uniqueness within one Worker even across different connections; use separate Worker Services for those identities. This limitation is validated before acquisition rather than leaking an engine startup failure.

The invocation is not the Nest HTTP pipeline. Method/class HTTP guards, pipes, interceptors and filters are rejected explicitly; global HTTP enhancers do not apply. UseMqGuards/Pipes/Interceptors/Filters instead compose a separate MQ pipeline, sharing the worker ContextId and preserving schemas, cancellation and single-use continuation draining. See execution-extensions.md. Local Worker and Process concurrency limits are implemented; they are not distributed queue limits.

The engine owns claims, leases, heartbeat, retries, stalled recovery and settlement. Pending cancellation uses its atomic job operation. Active cancellation revalidates identity, writes a cancellation request and leaves the owning supervisor to settle with its lease. The facade never steals a lease or promises to undo an external effect.

## Resource ownership and lifecycle

PostgreSQL delegates to its existing adapter/migrator rather than copying queue SQL. Startup validates schemas and does not apply migrations. Borrowed pools stay caller-owned; owned pools are created lazily and closed after worker/store finalizers. Idle-client errors on owned pg pools are handled without leaking client credentials into logs.

Failed acquisition/activation rolls back locally because Nest may rethrow bootstrap failure from close before destruction hooks. Shutdown detaches producers, stops admission, drains/cooperatively aborts workers, releases stores and closes owned resources. Repeated closes are memoized and cleanup errors aggregate. No automatic process signals or hard interruption of arbitrary JavaScript/driver calls are promised.

Connection readiness reports sanitized lifecycle/protocol information; explicit probes check live connectivity. Local worker status/idle does not establish global queue completion. No administrative HTTP surface is installed automatically.

## Remaining durable features

Distributed controls are implemented through QueueControls metadata and an internal protocol-dispatch view over the existing raw store. Global/per-key concurrency and fixed-window admission call the upstream controlled operations; no local semaphore or new lease algorithm is substituted. The raw persistence token stays stable. Replica startup validates persisted policy read-only; explicit coordinated deployment reconciles without disabling omissions. See controls.md for group checks, partial multi-store deployments, typed dispatch keys and separate-process PostgreSQL qualification. Named/versioned custom retry providers resolve through static DI; reference metadata fences incompatible rolling deployments without persisting executable functions. Durable-event waits/subscriptions need defined cursor and recovery semantics.

Durable flows retain the engine's persisted parent/children model. FanOut persists a stable manifest and relinquishes the PostgreSQL parent lease; Collect runs only after a fresh claim. Waiting parents do not occupy ordinary execution capacity. Typed result readers are bounded and phase-owned, while recovery rotates across persisted parents/children instead of relying solely on fast terminal reports. The v1 JobStore inspection contract is not widened for `waiting-children`; FlowStore/v2 is the suspended-parent inspection boundary. This is not arbitrary function replay or automatic saga compensation. See flows.md.

## Implemented persistent scheduler

Schedule decorators and MqSchedulesService now compile JSON/schema/codec-validated definitions, provide explicit deployment validation/reconciliation and use the existing JobScheduler in the same runtime. PostgreSQL schedule records share the raw namespace, native pool and private parser view. Dynamic work belongs in coordinator jobs, not serialized functions or timer-per-replica enqueue calls.

Default replica startup validates definitions; a coordinated reconcile writer preserves pauses/cursors and never removes omissions. The scheduler role is independent of workers/publisher and drains admitted ticks before resource release. Independent-process PostgreSQL tests verify fenced occurrences, actual JSON results, timezone, catch-up and overlap behavior. The pinned record has no dispatchKey: derived-key/per-key destinations are rejected. Its skip policy discards due slots without a grace threshold. See schedules.md for those explicit limits and administration safety.

## Remaining transaction integration

Application outbox must share the actual domain transaction. Preparation, append, post-commit publication and settlement remain separate, with independent publication and execution retries. SQL/MongoDB/Redis semantics are not interchangeable; each ORM bridge must establish transaction-resource identity. Application outbox is distinct from internal flow coordination. The native PostgreSQL facade now implements managed transactions using one actual PoolClient, separate native/adapter JSON views, tracked-operation draining and poison-on-failure. Its opt-in source stores and upstream publisher share the existing runtime and pool. Publisher enablement is independent of worker enablement. Duplicate validation includes destination and dispatch key; repeated callbacks remain an application idempotency concern. No arbitrary external transaction/ORM bridge or automatic callback replay is promised.

## Verification and release

Real Nest/engine tests cover contracts, scopes, retries, cancellation, concurrency and shutdown. Actual tarballs compile with TS6/7, run with Node/Bun and verify optional dependency isolation. PostgreSQL tests prove persisted jobs/results across recreated producer/worker contexts and exercise ownership/recovery failures. Full parity still needs further distributed/crash/ORM transaction coverage before a production release; no exactly-once external effects or cross-database atomicity is claimed.

## Internal dependency ownership

The engine and its PostgreSQL/outbox adapters are installed as normal dependencies managed by this library. Nest applications do not need their own Effect/Result setup or manual adapter version selection. Only Nest/support peers and chosen native/schema integrations remain application-facing. Root isolation means no eager optional-driver loading, not the absence of internal packages from the dependency tree. See dependencies.md and controls.md for the qualified consumer and distributed-policy boundaries.

## Event-assisted wait integration

Event-assisted awaitResult/execute is implemented; see event-waits.md for the API and exact scope. Configure postgres({ events: true }) on the waiting application and select strategy: 'events' with pollFallbackMs. Polling remains the default. The raw event token shares the job namespace; only an in-runtime operation alias is added. No second pool/runtime, automatic migrations or required-writer activation is introduced.

Runtime reader errors/lost hints use the existing bounded fallback; missing explicit reader configuration still fails. Timeout and abort do not cancel durable work. Keep tests for shutdown, parser fidelity and installed consumers. Resumable subscriptions, checkpoint APIs and retention administration remain pending; this is not a promise of zero polling.
