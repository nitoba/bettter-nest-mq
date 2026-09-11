import 'reflect-metadata'
import {
  defaultFlowMaxChildren,
  defaultFlowMaxDepth,
  maxFlowNameLength,
  validateFlowLimits
} from 'better-effect-mq'
import { Result } from 'better-result'
import { JobData, flowChildrenParameter } from '../workers/decorators.ts'
import { FlowJobReference } from './references.ts'
import type { FlowOptions } from './types.ts'
import { MqFlowException } from './errors.ts'
import { flowFields, flowName } from './validation.ts'

const FLOW = Symbol('mq.flow')
const PHASES = Symbol('mq.flow.phases')
export type Phase = 'fanOut' | 'collect'
export function Flow(options: FlowOptions): ClassDecorator {
  flowFields(options, ['name', 'parent', 'children', 'onChildFailure', 'maxChildren', 'maxDepth'])
  flowName(options.name, 'flow.name', maxFlowNameLength)
  if (
    !(options.parent instanceof FlowJobReference) ||
    !Array.isArray(options.children) ||
    options.children.some((child) => !(child instanceof FlowJobReference))
  )
    throw new MqFlowException('definition', 'Flow parent and children must be flowJob references')
  if (options.onChildFailure !== 'fail' && options.onChildFailure !== 'continue')
    throw new MqFlowException('definition', 'Flow failure policy must be fail or continue')
  const limits = validateFlowLimits({
    maxChildren: options.maxChildren ?? defaultFlowMaxChildren,
    maxDepth: options.maxDepth ?? defaultFlowMaxDepth
  })
  if (Result.isError(limits))
    throw new MqFlowException('definition', 'Invalid flow limits', { cause: limits.error })
  const snapshot = Object.freeze({
    ...options,
    ...limits.value,
    children: Object.freeze([...options.children])
  })
  return (target) => {
    if (Reflect.hasOwnMetadata(FLOW, target))
      throw new MqFlowException('definition', 'Duplicate @Flow on one Service')
    Reflect.defineMetadata(FLOW, snapshot, target)
  }
}
function phase(kind: Phase): MethodDecorator {
  return (target, method, descriptor) => {
    if (method !== String(method) || !(descriptor.value instanceof Function))
      throw new MqFlowException('definition', 'Flow phases must be named methods')
    const previous: ReadonlyMap<string, Phase> = Reflect.getOwnMetadata(PHASES, target) ?? new Map()
    if (previous.has(String(method)) || [...previous.values()].includes(kind))
      throw new MqFlowException('definition', 'Duplicate flow phase')
    const next = new Map(previous)
    next.set(String(method), kind)
    Reflect.defineMetadata(PHASES, next, target)
  }
}
export function FanOut(): MethodDecorator {
  return phase('fanOut')
}
export function Collect(): MethodDecorator {
  return phase('collect')
}
export function FlowData(): ParameterDecorator {
  return JobData()
}
export function FlowChildren(): ParameterDecorator {
  return flowChildrenParameter()
}
export function flowMetadata<Target extends object>(
  target: Target
): Readonly<FlowOptions> | undefined {
  return Reflect.getOwnMetadata(FLOW, target)
}
export function flowPhaseMetadata<Target extends object>(
  target: Target
): ReadonlyMap<string, Phase> {
  const hierarchy: object[] = []
  let current = target
  while (current !== null) {
    hierarchy.push(current)
    current = Object.getPrototypeOf(current)
  }
  const phases = new Map<string, Phase>()
  for (const prototype of hierarchy.reverse()) {
    const own: ReadonlyMap<string, Phase> = Reflect.getOwnMetadata(PHASES, prototype) ?? new Map()
    for (const [method, kind] of own) phases.set(method, kind)
  }
  return phases
}
