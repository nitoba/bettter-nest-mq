export function methodDescriptor<Target extends object>(
  target: Target,
  method: string
): PropertyDescriptor | undefined {
  let current = target
  while (current !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(current, method)
    if (descriptor !== undefined) return descriptor
    current = Object.getPrototypeOf(current)
  }
  return undefined
}
