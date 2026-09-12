import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import * as api from '../../src/index.ts'

test('MQ enhancers are explicit public decorators and do not install HTTP metadata', () => {
  expect(api.UseMqGuards).toBeInstanceOf(Function)
  class Guard {
    canActivate() {
      return true
    }
  }
  class Target {}
  api.UseMqGuards(Guard)(Target)
  expect(Reflect.hasMetadata('__guards__', Target)).toBe(false)
  // @ts-expect-error Malformed JavaScript inputs must be rejected at runtime as well.
  assert.throws(() => api.UseMqPipes(null), api.ContractDefinitionException)
})
