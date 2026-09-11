# Complete PR #4 and internalize engine integrations

The user authorized continuing the open PR and clarified that Nest consumers must not manually install or configure better-effect, better-result, better-effect-mq or their PostgreSQL/outbox adapters. The existing architecture remains approved: engine packages are implementation dependencies, not consumer-facing peer obligations.

## Verified starting point

PR #4 is open/draft on feat/distributed-controls. Main remains the merged M3 commit 2482d5e413e4aa6dd92fa2a0e700a2b8fe610450. The resumed regression at 5427dc14c359e671d91184a809952316471001bf passes TypeScript 6/7 and runs 164 tests: 161 pass and three fail specifically because a heartbeat advances updatedAt beyond the mutation's sampled now. The expired-lease rejection already passes. Earlier test-only API typos were corrected before interpreting these failures.

## Ordered implementation

1. Retry only the rejected controlled mutation when the upstream typed error explicitly identifies a stale now relative to updatedAt. Read current durable time, never move the requested time backward, preserve the original job ID/lease token/result, and retain retry delays. Bound refresh attempts; do not rerun a handler, retry arbitrary errors or suppress lease expiration.
2. Add negative regression coverage for changed/expired leases, duplicate acknowledgments, invalid timestamps and bounded refresh. Qualify the real PostgreSQL race plus the independent-worker scenarios through installed packages.
3. Move PostgreSQL/outbox engine packages from optional peers/development-only entries into normal dependencies, retaining lazy/isolated runtime imports. Keep Nest peers, optional Zod and optional native pg/types as consumer integrations. Verify a consumer manifest contains no better-effect-* or better-result dependencies except better-nest-mq itself.
4. Update external tarball tests and docs to require only the selected native driver/schema library, not internal adapters. Generate bun.lock with Bun; do not edit resolved versions by hand. Test the root without optional pg/Zod installed and test PostgreSQL with the automatically resolved engine dependencies.
5. Remove temporary branch-generation scripts/workflow, preserve exact upstream tooling and retained read-only CI, inspect final changes and merge only after the exact final head passes all quality and real-database gates.

## Non-goals

No npm publication, server deployment, automatic migrations, new runtime, second queue engine or fabricated flow/outbox functionality. Existing unsupported scalar PostgreSQL payload behavior remains issue #5 unless independently reproduced and fixed. Package internalization does not claim that the Nest outbox API is implemented.
