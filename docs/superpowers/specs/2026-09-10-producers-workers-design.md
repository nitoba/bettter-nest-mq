# M3 — Real producers and decorated Nest workers

The user approved the architecture and authorized merging and advancing. PR #2 was already merged at 1aab58de37b621410dab95d2f21fe32b02070e41. This milestone connects the existing typed contracts and private runtime to the upstream Job operations and Worker supervisor rather than implementing another polling/lease/retry engine.

## Public behavior

QueueService properties gain enqueue, enqueueDecoded, enqueueMany, prepare, poll, awaitResult, execute, attempts, cancel, retry and promote. Input and decoded payload types remain distinct. A prepared request is serializable data, not a transaction or an already-published job. Calls fail explicitly before binding, after shutdown or in contract-only mode. All application APIs return Promises and facade-owned types.

Worker Services declare @Worker options and @Process(QueueClass, property) methods. @JobData and @JobContext map parameters explicitly; undecorated handler parameters are rejected instead of guessed. Context includes stable job identity, attempt/delivery information, metadata and the cooperative AbortSignal, not the lease token. Nest owns Service dependencies; each attempt can resolve request/transient-scoped dependencies using a fresh ContextId. No HTTP request is fabricated. This first execution pipeline does not silently promise HTTP guards/pipes/interceptors/filters; unsupported enhancer metadata must fail clearly until an explicit MQ enhancer integration exists.

## Composition

Compile all contracts and worker references before opening resources. Reject duplicate handlers, missing queues/properties, getters masquerading as methods, malformed options and conflicting identities. Build lazy upstream Worker service layers into the same application runtime as named stores, then resolve them only after stores are ready and producer bindings are available. A producer-only execution flag never starts consumers, even if worker metadata is present. A failure to activate any worker rolls back all activated workers/stores and detaches clients.

The existing engine host coordinates startup and shutdown. Runtime-owned Worker layers quiesce admission and protect active attempts/leases before store resources are released. No runtime per job or per Worker provider. All public producer calls are admitted through that host; stale clients cannot keep using closed stores.

## Schemas, failures and durable policies

Compile schemas to the upstream Codec contract, retaining explicit inverse encoders and JSON fidelity checks. Validate payloads before store writes, persisted payloads before handlers, handler results and known failure data before persistence. Typed JobFailureException values become typed engine failures; unexpected exceptions remain defects. Invalid result/failure contracts cannot become successful jobs. Retryable predicates are evaluated on validated domain-failure values. Runtime timeout, heartbeat, stalled recovery and retries use the engine supervisor.

Translate fixed, linear and exponential backoff declarations exactly; unsupported custom provider policies fail explicitly rather than degrading to another policy. A zero execution timeout is rejected for executable bindings because the engine requires a positive timeout. Local worker/handler concurrency is supported; distributed queue controls and custom retry providers remain separately documented extensions, not local approximations.

Stable identities and idempotency are supplied to upstream Job operations. Bulk validation occurs before publication, but storage-level batch atomicity is not promised universally. Explicit job identity guards prevent using another job's ID to read or mutate it.

## Waiting and operations

awaitResult supports polling with caller signal and bounded timeout. Timeout/abort stops only the wait, not the persisted job. execute publishes and waits; it never invokes a handler locally. Terminal typed failures are represented by JobFailureException, cancellation and infrastructure/execution failures by explicit facade errors. A minimal workers service exposes safe local status and awaitIdle, with no raw runtime handles.

## Verification

Observe a failing public API regression against M2. Add real Nest + upstream memory-store tests (explicit fixture only) for producers, retries, failure classes, schemas/codecs, idempotency, bulk validation, identity guards, cancellation, waiting timeout, worker/handler concurrency, request-scoped dependencies, duplicate registration, producer-only mode and shutdown. Run real PostgreSQL end-to-end jobs, close/recreate contexts, and consume the actual published-shape tarball under Node/Bun with TypeScript 6/7. Keep the original 20 tooling hashes, strict lint/format rules and optional dependency isolation. No npm release or automatic database migrations.

Flows, persistent schedules, transactional outbox/ORM bridges, other adapters and full administration/event subscriptions remain follow-on milestones.
