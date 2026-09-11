import { expect, test } from 'bun:test'
import { z } from 'zod'
import {
  Flow,
  FanOut,
  Collect,
  flowJob,
  flowChildren,
  Job,
  QueueService,
  MqFlowException
} from '../../src/index.ts'
import { flowMetadata, flowPhaseMetadata } from '../../src/flows/decorators.ts'

class Jobs extends QueueService {
  @Job({ name: 'parent', version: 1 })
  readonly parent = this.job({ payload: z.string(), result: z.string() })
  @Job({ name: 'child', version: 1 })
  readonly child = this.job({ payload: z.object({ label: z.string() }), result: z.string() })
  readonly notAJob = false
}
const parent = flowJob(Jobs, 'parent')
const child = flowJob(Jobs, 'child')

test('flow references and plans are inert immutable snapshots without freezing caller inputs', () => {
  const payload = { label: 'initial' }
  const plan = flowChildren(child, [{ key: 'a', payload }])
  payload.label = 'changed'
  expect(plan.items[0]?.payload).toEqual({ label: 'initial' })
  expect(Object.isFrozen(plan)).toBe(true)
  expect(Object.isFrozen(payload)).toBe(false)
  expect(Object.isFrozen(child)).toBe(true)
})
test('flow metadata preserves typed identities, explicit failure policy and phase inheritance', () => {
  @Flow({ name: 'summary', parent, children: [child], onChildFailure: 'continue' })
  class Summary {
    @FanOut() split() {
      return []
    }
    @Collect() finish() {
      return 'done'
    }
  }
  class Subclass extends Summary {}
  expect(flowMetadata(Summary)?.name).toBe('summary')
  expect(flowMetadata(Subclass)).toBeUndefined()
  expect([...flowPhaseMetadata(Subclass.prototype)]).toEqual([
    ['split', 'fanOut'],
    ['finish', 'collect']
  ])
})
test('flow definitions reject ambiguous names, limits and unsupported fields', () => {
  expect(() => Flow({ name: '', parent, children: [child], onChildFailure: 'continue' })).toThrow(
    MqFlowException
  )
  expect(() =>
    Flow({ name: 'f', parent, children: [child], onChildFailure: 'continue', maxChildren: 0 })
  ).toThrow(MqFlowException)
  expect(() =>
    Flow(JSON.parse('{"name":"f","parent":null,"children":[],"onChildFailure":"ignore"}'))
  ).toThrow(MqFlowException)
})
test('child plans reject duplicate keys, lossy values, accessors and unsupported enqueue options', () => {
  expect(() =>
    flowChildren(child, [
      { key: 'a', payload: { label: 'x' } },
      { key: 'a', payload: { label: 'y' } }
    ])
  ).toThrow(MqFlowException)
  expect(() =>
    flowChildren(
      child,
      JSON.parse('[{"key":"a","payload":{"label":"x"},"options":{"delayMs":10}}]')
    )
  ).toThrow(MqFlowException)
  let read = false
  expect(() =>
    flowChildren(child, [
      {
        key: 'a',
        get payload() {
          read = true
          return { label: 'x' }
        }
      }
    ])
  ).toThrow(MqFlowException)
  expect(read).toBe(false)
  expect(() =>
    flowChildren(child, [{ key: 'a', payload: JSON.parse('{"label":null}') }])
  ).not.toThrow()
})
