import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Result } from 'better-result'
import { decodeFlowJobRow, type FlowJobRow } from '../../src/integrations/postgres-flow-reads.ts'

function suspended(): FlowJobRow {
  return {
    id: 'parent',
    name: 'aggregate',
    queue: 'parents',
    version: 1,
    state: 'waiting-children',
    dispatch_key: null,
    idempotency_key: null,
    payload: { values: [1] },
    metadata: {},
    priority: 0,
    run_at_ms: '1000',
    sequence: '1',
    attempts_max: 2,
    attempts_made: 0,
    attempt_sequence: 1,
    delivery_count: 1,
    stalled_count: 0,
    timeout_ms: null,
    created_at_ms: '1000',
    updated_at_ms: '1001',
    processed_at_ms: '1000',
    finished_at_ms: null,
    lease_owner: null,
    lease_token: null,
    lease_expires_at_ms: null,
    cancellation_requested_at_ms: null,
    result: null,
    result_present: false,
    failure: null,
    parent: null,
    backoff: null,
    flow: {
      flowName: 'aggregate',
      failFast: false,
      pending: 1,
      completed: 0,
      failed: 0,
      cancelled: 0
    }
  }
}
test('the read projection preserves a valid suspended flow without inventing a lease or changing its state', () => {
  const result = decodeFlowJobRow(suspended())
  assert.ok(Result.isOk(result))
  expect(result.value.state).toBe('waiting-children')
  expect(result.value.leaseToken).toBeUndefined()
  expect(result.value.flow?.pending).toBe(1)
})
test('the v2 projection rejects unknown states, missing flow metadata and unsafe counters', () => {
  expect(Result.isError(decodeFlowJobRow({ ...suspended(), state: 'invented' }))).toBe(true)
  expect(Result.isError(decodeFlowJobRow({ ...suspended(), flow: null }))).toBe(true)
  expect(
    Result.isError(decodeFlowJobRow({ ...suspended(), delivery_count: '9007199254740992' }))
  ).toBe(true)
})
test('SQL NULL and a present JSON null remain distinguishable in the flow read projection', () => {
  const input = {
    ...suspended(),
    state: 'completed',
    attempts_made: 1,
    finished_at_ms: '1002',
    updated_at_ms: '1002',
    flow: {
      flowName: 'aggregate',
      failFast: false,
      pending: 0,
      completed: 1,
      failed: 0,
      cancelled: 0
    }
  }
  const present = decodeFlowJobRow({ ...input, result_present: true })
  const absent = decodeFlowJobRow(input)
  assert.ok(Result.isOk(present) && Result.isOk(absent))
  expect(present.value.result).toBeNull()
  expect(absent.value.result).toBeUndefined()
})
