# Flow review checkpoint — 11 September 2026

**PR #9 is not merge-ready.** This checkpoint preserves the additional v2 read/recovery work present at 1c721bf415da1c3e8e995bf23a19ce2fdac7109c rather than replacing it with the earlier source tree. Historical test counts in the flow guide refer to their stated commits, not proof of the latest head.

## Evidence

At 89c66e8, development run 34616650927 passed the source/reference quality step (both TypeScript checks, 261 unit/Nest tests, formatting, build, type-aware lint and publint), then failed the installed PostgreSQL flow scenario.

Independent published-package run 34616650978 reproduced getJob/heartbeat/release rejecting waiting-children without importing Nest or facade code. This defect is recorded in [better-effect #387](https://github.com/nitoba/better-effect/issues/387). The native reproduction remains a real failing regression, not an expected-success test.

The later read/recovery adaptation at 1c721bf was checked by development run 34617284186. It still failed type checking: the cancel adapter returned a JobRecord instead of JobTransition, and recovery identifiers were plain strings instead of branded JobIds, affecting Worker layer inference. Its separately executed installed scenario progressed through manifest persistence and process death; all three children completed, but the resumed parent failed result encoding with code codec-encode. Do not equate that partial progress with a successful restart test. The remaining scenarios did not all complete.

## Required next checks

Resolve the adapter/Worker protocol issue coherently, review the v2 projection against every affected operation and correct the candidate's type and result-encoding failures. Then run the pure protocol reproduction, source checks and complete installed PostgreSQL scenarios. Review cancellation result/attempt semantics, branded IDs, resumed Collect selection and payload/result encoding rather than casting errors away.

Re-run the Node22/24 and PostgreSQL package matrix on the exact final head before merge. Preserve job/flow namespaces, public types, native parser behavior, lease ownership and the original 20 tooling hashes. Do not mark the database suite as optional or claim the entire integration has passed based on reference stores.

The checkpoint removes temporary write-enabled development tooling while retaining normal read-only CI and the isolated read-only native regression. No merge to main, npm publication or production deployment is included.
