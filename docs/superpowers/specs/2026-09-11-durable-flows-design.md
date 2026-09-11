# Durable flows — approved roadmap continuation

The user authorized implementing the next durable-flow milestone after schedules PR #8. Base main is b35f4208b8f55db97d21296a86dacaec21412615. Preserve the approved Nest-native model: Flow names parent/child job references and policies; FanOut returns a typed finite plan; Collect consumes persisted outcomes through a bounded/paginated API. No step replay, Promise.all coordinator, fabricated handler, new runtime or user-facing Effect types.

## Public design

`flowJob(Queue, 'property')` is an immutable typed contract reference. `flowChildren(reference, [{ key, payload, options? }])` builds inert data without publication. `@Flow({ name, parent, children, onChildFailure, maxChildren?, maxDepth? })` is class metadata on an actual Nest Worker Service. `@FanOut()` and `@Collect()` mark separate methods; JobData/JobContext keep their meanings, FlowData provides the decoded parent input and FlowChildren provides a facade-owned results reader. Dynamic child input is validated and encoded once through existing contracts before fan-out settlement. Collect reads terminal outcomes with decoded result/domain failure types. A closed phase reader rejects further calls, and admitted reads drain before its invocation exits.

Job descriptors remain the producer API for flow parents. Parent waiting-children state and administrative reads/cancellation use the actual flow extension; no client-side child registry is authoritative. MqFlowsService exposes contract-scoped flow state and explicit cooperative cascade cancellation without claiming synchronous termination. Ordinary workers receive the flow definitions needed for reports/reconciliation even when another Service owns the parent phases.

## Storage/lifecycle

`postgres({ flows: true })` opts into the upstream FlowStore over the same physical native pool and durable raw job namespace. The flow adapter expects decoded JSON objects, unlike the current JobStore/schedule parser view; isolate that difference with per-query JSON parsing, never global parser mutation or payload envelopes. Operation-token flow aliases are runtime-only views of that same resource. Pin published contracts and verify each underlying register/get/relay signature before use.

Validate definitions, duplicate identities/phase methods, registered references, worker requirements and enabled flow resources before activation. Use the existing supervisor for fan-out, manifest CAS, report outbox, relay, reconciliation, nested depth and fail-fast/cancellation. A waiting parent does not occupy a concurrency slot. Drain admitted flow work before pool release and preserve existing schedules/outbox/control behavior.

Inspect published protocol limitations before advertising combinations: empty-handler workers, per-key destinations/parents, delay and dispatch-key forwarding, namespaces and worker flow registration. Unsupported combinations must fail explicitly before writes; never use hidden dummy handlers or silently weaken distributed control semantics. No source fork of the supervisor or npm release to mask a compatibility problem.

## Tests and operational contract

Observe an export regression fail against the 242-test baseline. Add metadata/type-negative tests, schema/codec/JSON fidelity, missing stores/identities, duplicate/conflicting registrations, inherited/scoped methods and role isolation. Run actual packed-package PostgreSQL tests with multiple processes, waiting parents, restart between fan-out and collection, continue/fail, cancellation, nesting, empty fan-out, bounded pages, corrupt/invalid inputs, lost responses and recovery where the pinned engine supports it. State exactly which cases were verified and which remain future qualification.

Keep TS6/7, Node22/24/Bun, 20 tooling hashes, normal internal dependencies and optional native driver isolation unchanged. Retained CI is read-only. Temporary branch helpers are removed before final exact-head validation and PR integration. Do not publish npm, change repository settings or deploy production.
