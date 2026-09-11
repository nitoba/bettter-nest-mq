import 'reflect-metadata'
import { ContractDefinitionException } from '../contracts/errors.ts'
import { requireInteger, requireName } from '../contracts/policies.ts'
import { QueueService } from '../contracts/queue-service.ts'
import type { MqControlsOptions, QueueControlsOptions } from './types.ts'

const CONTROLS = Symbol('mq.queue.controls')

export function QueueControls(options: QueueControlsOptions): ClassDecorator {
  if (
    options.globalConcurrency === undefined &&
    options.perKeyConcurrency === undefined &&
    options.rateLimit === undefined
  )
    throw new ContractDefinitionException('QueueControls requires at least one explicit limit')
  if (options.globalConcurrency !== undefined)
    requireInteger(options.globalConcurrency, 'controls.globalConcurrency', 1)
  if (options.perKeyConcurrency !== undefined)
    requireInteger(options.perKeyConcurrency, 'controls.perKeyConcurrency', 1)
  if (options.rateLimit !== undefined) {
    requireInteger(options.rateLimit.max, 'controls.rateLimit.max', 1)
    requireInteger(options.rateLimit.durationMs, 'controls.rateLimit.durationMs', 1)
  }
  const metadata = Object.freeze({
    ...options,
    rateLimit: options.rateLimit === undefined ? undefined : Object.freeze({ ...options.rateLimit })
  })
  return (target) => {
    if (!(target.prototype instanceof QueueService))
      throw new ContractDefinitionException('@QueueControls requires a QueueService subclass')
    if (Reflect.hasOwnMetadata(CONTROLS, target))
      throw new ContractDefinitionException('Duplicate @QueueControls decorator')
    Reflect.defineMetadata(CONTROLS, metadata, target)
  }
}

/** Operational policies belong to the concrete queue identity, never an inherited surprise. */
export function queueControlsMetadata<Target extends object>(
  target: Target
): QueueControlsOptions | undefined {
  return Reflect.getOwnMetadata(CONTROLS, target)
}

export function resolveControlsOptions(
  options: MqControlsOptions = {}
): Readonly<Required<MqControlsOptions>> {
  const group = options.group ?? 'nestjs/queue-controls'
  const mode = options.mode ?? 'validate'
  requireName(group, 'controls.group')
  if (group.length > 128 || group.includes('\0'))
    throw new ContractDefinitionException('controls.group must fit 128 characters without NUL')
  if (mode !== 'validate' && mode !== 'reconcile')
    throw new ContractDefinitionException('controls.mode must be validate or reconcile')
  return Object.freeze({ group, mode })
}

export function validateDispatchKey(key: string): string {
  requireName(key, 'dispatchKey')
  if (key === '__none__' || key.length > 512 || key.includes('\0'))
    throw new ContractDefinitionException(
      'dispatchKey must be at most 512 characters, contain no NUL, and not use the reserved __none__ bucket'
    )
  return key
}
