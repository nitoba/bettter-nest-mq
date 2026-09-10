# Typed contracts and Nest registration

This is the M1 implementation of the architecture already approved in docs/architecture.md. The user authorized continuing development and repository deliveries after the bootstrap.

## Deliverable

Implement inert QueueService job definitions, @Queue/@Job/@Retry/@JobTimeout, stable connection/queue/name/version identities, immutable configuration precedence, schema-derived input/output/failure types, known failure exceptions, Standard Schema validation, explicit codecs and Nest forFeature registration with an application-context-local registry.

Job declarations are usable contracts, not simulated producers. No enqueue, worker, storage adapter, flow, schedule or outbox operation is exported in this milestone. The registry discovers real singleton Nest providers, validates the complete set before publishing a snapshot, and rejects duplicate durable identities. Merely importing a queue never performs I/O.

## Schemas and persistence boundary

The core depends on the Standard Schema interface, not Zod runtime internals. Validation and JSON encoding have separate methods. An explicit codec supplies an output-to-input encoder. Encoding verifies that the encoded JSON decodes to the same domain value. A plain schema is only an identity encoder when validation leaves that decoded value unchanged; non-idempotent transforms and non-JSON values need an explicit encoder. Invalid JSON, lossy serialization and vendor exceptions have distinct errors.

The Zod subpath delegates encoding to Zod's supported encodeAsync API and remains optional. It accepts nested codecs, validates their reverse direction and does not infer an inverse for a one-way transform.

## Metadata and policies

Metadata is copied on write and options are frozen without freezing caller-owned schemas. Each queue class declares its own queue identity. Inherited job metadata may be reused, but overriding a decorated property with a getter or an unrelated value is rejected without invoking getters. Duplicate decorators and missing @Job declarations are errors.

Resolve policies in order: library defaults, module defaults, queue defaults, job defaults, property decorators. A retry policy is replaced as a unit, never partially merged across different backoff variants. Validate non-negative durations, positive safe attempt/version counts, finite jitter in [0, 1], exponential factors and named/versioned custom policies. Resolution does not execute retries or rate limits.

## Nest boundary

forFeature registers and exports the supplied QueueService classes. forRoot owns discovery and MqRegistry. Registration requires singleton/static dependency trees; scoped workers are a later execution concern, not a reason to make shared contracts request scoped. Aliases of one provider instance are deduplicated. All queues are validated before the registry snapshot changes. Initialization failure leaves no partial registry and close releases the snapshot.

## Verification

Start with a public API regression that fails against the bootstrap. Add real schema, metadata and Nest-context tests before their implementations. Cover async vendors, codec round trips, invalid/corrupt data, metadata inheritance, duplicate identities, policy precedence, registry isolation and failure atomicity. Compile type-negative cases under TypeScript 6 and 7. Extend the real external tarball consumer to exercise queue registration and optional Zod. Run the unchanged tooling integrity check, formatting, type-aware lint, build and publint. No npm publication or repository setting changes.
