import { z } from 'zod'

import { defineCodec } from '../contracts/schema.ts'
import type { SchemaCodec } from '../contracts/schema.ts'

/** Opt-in bridge using Zod's public reverse-validation/encoding API, including nested codecs. */
export function zodCodec<Output, Input>(
  schema: z.ZodType<Output, Input>
): SchemaCodec<z.ZodType<Output, Input>> {
  return defineCodec(schema, (value) => z.encodeAsync(schema, value))
}
