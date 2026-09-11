import { MemoryJobStore } from 'better-effect-mq'

/** A single shared reference store exercises the protocol inside one test process. This is
 * not exported as a production adapter and makes no cross-process durability claim. Real
 * distributed guarantees are qualified separately against PostgreSQL worker processes. */
export function controlReferenceStore() {
  const store = MemoryJobStore.make()
  Object.defineProperty(store, 'descriptor', { value: Object.freeze({
    ...store.descriptor,
    capabilities: Object.freeze({ ...store.descriptor.capabilities, globalConcurrency: true, rateLimiting: true })
  }) })
  return store
}
