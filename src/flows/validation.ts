import { isDeepStrictEqual } from 'node:util'
import { requireName, requireInteger, copyRetry } from '../contracts/policies.ts'
import type { JobJsonValue } from '../jobs/types.ts'
import { MqFlowException } from './errors.ts'
import type { FlowChildOptions } from './types.ts'

export function flowFields<Value extends object>(value: Value, allowed: readonly string[]): void {
  if (value === null || value === undefined)
    throw new MqFlowException('definition', 'Flow configuration must be a data object')
  const prototype = Object.getPrototypeOf(value)
  if (prototype !== Object.prototype && prototype !== null)
    throw new MqFlowException('definition', 'Flow configuration must be a plain data object')
  for (const key of Reflect.ownKeys(value)) {
    const field = Object.getOwnPropertyDescriptor(value, key)
    if (
      key !== String(key) ||
      !allowed.includes(String(key)) ||
      field?.get !== undefined ||
      field?.set !== undefined
    )
      throw new MqFlowException('definition', `Invalid flow field ${String(key)}`)
  }
}
export function flowName(value: string, field: string, limit: number): string {
  try {
    requireName(value, field)
    if (value.includes('\0') || value.length > limit)
      throw new Error('Name exceeds protocol limits')
    return value
  } catch (cause) {
    throw new MqFlowException('definition', `Invalid ${field}`, { cause })
  }
}
function inspectJson<Value>(value: Value, seen: Set<object>): void {
  if (value === null || value === undefined || Object(value) !== value) return
  if (seen.has(value)) throw new MqFlowException('definition', 'Flow inputs cannot contain cycles')
  seen.add(value)
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype)
    throw new MqFlowException(
      'definition',
      'Flow inputs must be JSON schema inputs, not decoded instances'
    )
  for (const key of Reflect.ownKeys(value)) {
    const field = Object.getOwnPropertyDescriptor(value, key)
    if (key !== String(key) || field?.get !== undefined || field?.set !== undefined)
      throw new MqFlowException('definition', 'Flow inputs cannot contain accessors or symbol keys')
    inspectJson(field?.value, seen)
  }
  seen.delete(value)
}
function freezeJson(value: JobJsonValue): void {
  if (value !== null && value instanceof Object) {
    for (const item of Object.values(value)) freezeJson(item)
    Object.freeze(value)
  }
}
export function copyFlowInput<Value>(value: Value): Value {
  try {
    inspectJson(value, new Set())
    const text = JSON.stringify(value)
    if (text === undefined) throw new Error('Missing JSON input')
    const copy = JSON.parse(text)
    if (!isDeepStrictEqual(copy, value)) throw new Error('Flow input changes during JSON encoding')
    freezeJson(copy)
    return copy
  } catch (cause) {
    if (cause instanceof MqFlowException) throw cause
    throw new MqFlowException(
      'definition',
      'Flow payload must have a lossless JSON representation',
      { cause }
    )
  }
}
export function flowChildOptions(options: FlowChildOptions): FlowChildOptions {
  try {
    flowFields(options, ['priority', 'retry', 'timeoutMs', 'metadata'])
    if (options.priority !== undefined) requireInteger(options.priority, 'flow.priority')
    if (options.timeoutMs !== undefined) requireInteger(options.timeoutMs, 'flow.timeoutMs', 1)
    let copied = { ...options }
    if (options.retry !== undefined) copied = { ...copied, retry: copyRetry(options.retry) }
    if (options.metadata !== undefined) {
      flowFields(options.metadata, Object.keys(options.metadata))
      for (const [key, value] of Object.entries(options.metadata)) {
        if (value !== String(value) || key.startsWith('__better_effect_flow_v2.'))
          throw new Error('Flow metadata must be strings and cannot override protocol fields')
      }
      copied = { ...copied, metadata: Object.freeze({ ...options.metadata }) }
    }
    return Object.freeze(copied)
  } catch (cause) {
    if (cause instanceof MqFlowException) throw cause
    throw new MqFlowException('definition', 'Invalid child policy', { cause })
  }
}
