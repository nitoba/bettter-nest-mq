import { expect, test } from 'bun:test'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import { z } from 'zod'

test('the installed Zod 4 satisfies the Standard Schema contract', async () => {
  const schema = z.object({ name: z.string().min(1) }) satisfies StandardSchemaV1<{ name: string }>
  expect(await schema['~standard'].validate({ name: 'reports' })).toEqual({
    value: { name: 'reports' }
  })
  expect((await schema['~standard'].validate({ name: '' })).issues?.length).toBeGreaterThan(0)
})
