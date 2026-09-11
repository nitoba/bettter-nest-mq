import type { Pool } from 'pg'

/** Compare method descriptors without invoking accessors or capturing unbound callbacks.
 * Include every prototype level so own-property shadows and prototype mutations are detected. */
export function poolMethodDescriptors(pool: Pool): ReadonlyArray<ReadonlyArray<PropertyDescriptor | undefined>> {
  return ['query', 'connect', 'end'].map((name) => {
    const descriptors: Array<PropertyDescriptor | undefined> = []
    let target = pool
    while (target !== null) {
      descriptors.push(Object.getOwnPropertyDescriptor(target, name))
      target = Object.getPrototypeOf(target)
    }
    return descriptors
  })
}
