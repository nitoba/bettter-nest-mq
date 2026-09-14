# Recovering failed outbox publications

`MqOutboxService.retryFailed` is an explicit administrative operation for publications in the `failed` state. It makes the same prepared job eligible for publication again after an operator resolves the underlying problem. It is not a job handler retry, a replay of a business transaction or a general record editor.

PostgreSQL supports this optional adapter capability when the source uses the existing `postgres({ outbox: true })` connection. No extra pool, runtime, migration or engine dependency installation is required. Other adapters without the capability fail explicitly; the facade never uses an active publisher's lease or silently falls back to another storage implementation.

## Inspect, resolve the cause, then retry

An injected outbox Service can be used from a protected administrative workflow:

```ts
import { Injectable } from '@nestjs/common'
import { MqOutboxService } from 'better-nest-mq'

@Injectable()
export class PublicationRecoveryService {
  constructor(private readonly outboxes: MqOutboxService) {}

  async recover(id: string) {
    const record = await this.outboxes.get('primary', id)
    if (!record || record.state !== 'failed') {
      throw new Error('A failed publication must be inspected before recovery')
    }

    // Resolve the failure before this operation, for example by restoring the destination.
    return this.outboxes.retryFailed('primary', record.id, {
      expected: {
        updatedAtMs: record.updatedAtMs,
        attemptsMade: record.attemptsMade,
        attemptsMax: record.attemptsMax
      },
      attempts: 3
    })
  }
}
```

For a human approval flow, preserve the expected values from the record actually presented to the operator. Do not silently replace them with a freshly fetched version after approval: doing so defeats the stale-state check.

`get` and `list` expose the last publication failure, target and counters for diagnosis. A missing target can be restored; transient infrastructure issues can be resolved. Recovery does not automatically bypass a permanent failure, modify payloads or retry endlessly. The explicit operator is responsible for deciding whether the underlying cause has been addressed.

The recovery application must register the original destination queue/job contract and the relevant connections. It may use `execution: { workers: false, outboxPublisher: false }`; a separately deployed publisher can perform the actual forwarding. Schema, retry-provider reference and dispatch-key validation use those registered contracts again before the administrative write. Restoring an incompatible payload or changing a target/job version requires a separate application decision, not this operation.

## Attempt budgets and preserved fields

`attempts` is the number of publication attempts available after this recovery. The lifetime `attemptsMade` counter is never reset. The new absolute `attemptsMax` is `attemptsMade + attempts`.

For example, a publication that exhausted 2 attempts and receives 3 more keeps `attemptsMade: 2` and changes `attemptsMax` to 5. If it failed permanently after its first attempt despite a previous budget of 10, granting 2 attempts sets the new maximum to 3, not 12. Unused old budget is not silently added to the requested recovery budget.

The outbox ID, destination, complete prepared request, stable job ID, dispatch key, original creation time and most recent failure are preserved. In particular, `request.attemptsMax` still describes the job handler's execution budget and is not altered by publication recovery. The timestamp advances monotonically even when two administrative operations sample the same millisecond. It remains a version guard, not a distributed wall-clock synchronization service.

The operation returns a snapshot of the accepted transition to `pending`. A concurrent publisher may immediately claim or publish it; the returned snapshot is not proof of its current state at a later instant, nor proof that its handler ran. Inspect the record or job separately when that distinction matters.

## Delayed recovery

An optional absolute timestamp controls when publication may resume:

```ts
await outboxes.retryFailed('primary', record.id, {
  expected: {
    updatedAtMs: record.updatedAtMs,
    attemptsMade: record.attemptsMade,
    attemptsMax: record.attemptsMax
  },
  attempts: 2,
  runAtMs: Date.now() + 60_000
})
```

This changes the outbox's eligibility, not the immutable job request's original schedule. A past timestamp is clamped to the accepted retry time; recovery never backdates its update. Timestamps and counters must be valid safe integers, and the adapter's column limits still apply. Publication is performed by the existing publisher, not a new per-record timer.

## Concurrency and shutdown

PostgreSQL performs one conditional UPDATE. It requires the record still to be failed, the expected version/counters to match, and the stored request, target, digest, timestamps and failure to remain unchanged since inspection. It additionally requires no active lease or published timestamp. The failure/content checks matter because a digest alone is not a complete routing/dispatch-key identity check.

Two administrators racing with the same inspected version cannot both succeed. Stale or concurrently changed data is rejected; there is no `force` override. Pending, active and published records are not reset. Old publisher lease tokens are not reused, and lease-guarded native operations remain responsible for rejecting those old tokens.

A failed network response does not prove the database rejected the UPDATE. The library does not automatically replay an uncertain administrative write. Re-read the record and inspect its version/state before deciding what to do. Likewise, this guard assumes the same durable record is managed normally: arbitrary external deletion/reinsertion or unsupported SQL edits are not an authenticated, versioned administration protocol.

An admitted PostgreSQL retry is tracked through completion before its store resources are disposed. Closing the application prevents new retry admission. A borrowed pool remains owned by the application. The library does not forcibly interrupt a blocked driver query; configure database timeouts and investigate lock holders as part of normal operations.

## At-least-once semantics and audit responsibility

A publication may already have reached its target even though it later exhausted acknowledgment/recovery attempts. Retrying therefore retains the exact original job ID and request so the native target's idempotent enqueue behavior can apply. It does not promise exactly-once side effects, deduplicate arbitrary business SQL or recreate a removed target job's historical idempotency evidence. Do not purge deduplication evidence while unresolved publications can still be recovered.

No domain transaction callback is executed during this operation. An existing business record is neither inserted again nor rolled back. Retrying a successfully published job that later fails execution uses the job's own administrative API, not `retryFailed` on its published outbox record.

The last failure and lifetime attempts remain available, but this is not an append-only administrative audit log. Applications requiring actor identity, approval records, reasons or compliance auditing must implement those alongside their protected administration workflow. No HTTP endpoint, authentication policy, scheduler or automatic failed-record scanner is installed.

Re-appending the original business outbox entry is not a substitute for recovery. Existing duplicate validation compares the configured budget as well as the destination/request, so an entry with the old budget may conflict after an explicit administrative budget change. Recovery does not weaken that validation or replay the original business transaction.

## Verification

The regression suite covers required expected values, immutable option copying, invalid budgets, numeric overflow, monotonic timestamps, preserved content/failure, unsupported adapters and closing while a write is admitted. PostgreSQL tests use real native leases to check stale acknowledgment/heartbeat rejection and repeated recovery guards, including a concurrent content change that does not advance the timestamp.

Installed public consumers exercise actual publisher exhaustion with a missing destination, restored destination contracts, two independent administrative application contexts, guarded requeue, schema rejection, a blocked retry UPDATE during shutdown, delayed eligibility, and later processing of the same job IDs without repeating business writes. These run alongside the existing complete package matrix with optional dependencies and internal-package installation checks.

This feature does not implement pruning, bulk reset, payload repair, record deletion, outbox history, external ORM transaction enrollment or recovery capabilities for other storage adapters. Those remain separate roadmap items. There is no npm release or production deployment implied by this administrative API.
