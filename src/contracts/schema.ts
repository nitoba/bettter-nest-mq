import { isDeepStrictEqual } from 'node:util'
import type { StandardSchemaV1 } from '@standard-schema/spec'

import {
  SchemaDefectException,
  SchemaEncodingException,
  SchemaValidationException
} from './errors.ts'

const CODEC = Symbol('mq.codec')

/** An explicit inverse for the Standard Schema's input-to-output direction. */
export interface SchemaCodec<Schema extends StandardSchemaV1> {
  readonly [CODEC]: true
  readonly schema: Schema
  encode(
    value: StandardSchemaV1.InferOutput<Schema>
  ): StandardSchemaV1.InferInput<Schema> | Promise<StandardSchemaV1.InferInput<Schema>>
}

export type ValueSchema = StandardSchemaV1 | SchemaCodec<StandardSchemaV1>
export type SchemaOf<Contract extends ValueSchema> =
  Contract extends SchemaCodec<infer Schema>
    ? Schema
    : Contract extends StandardSchemaV1
      ? Contract
      : never
export type SchemaInput<Contract extends ValueSchema> = StandardSchemaV1.InferInput<
  SchemaOf<Contract>
>
export type SchemaOutput<Contract extends ValueSchema> = StandardSchemaV1.InferOutput<
  SchemaOf<Contract>
>

export function defineCodec<Schema extends StandardSchemaV1>(
  schema: Schema,
  encoder: (
    value: StandardSchemaV1.InferOutput<Schema>
  ) => StandardSchemaV1.InferInput<Schema> | Promise<StandardSchemaV1.InferInput<Schema>>
): SchemaCodec<Schema> {
  const contract: SchemaCodec<Schema> = {
    [CODEC]: true,
    schema,
    encode(value) {
      return encoder(value)
    }
  }
  return Object.freeze(contract)
}

function standardSchema(contract: ValueSchema): StandardSchemaV1 {
  return CODEC in contract ? contract.schema : contract
}

/** Runtime validation boundary; input is deliberately not trusted merely because it is typed. */
export async function validateSchema<Contract extends ValueSchema, Input>(
  contract: Contract,
  input: Input
): Promise<SchemaOutput<Contract>> {
  const schema = standardSchema(contract)
  const result = await Promise.resolve()
    .then(() => schema['~standard'].validate(input))
    .catch((cause) => {
      throw new SchemaDefectException('validate', { cause })
    })

  if (result.issues !== undefined) throw new SchemaValidationException(result.issues)

  // SAFETY: this successful result came from this exact contract's Standard Schema validator.
  return result.value as SchemaOutput<Contract>
}

function jsonText<Value>(value: Value): string {
  let text: string | undefined
  try {
    text = JSON.stringify(value)
  } catch (cause) {
    throw new SchemaEncodingException('The encoded value is not JSON serializable', { cause })
  }
  if (text === undefined || !isDeepStrictEqual(JSON.parse(text), value)) {
    throw new SchemaEncodingException(
      'JSON encoding would lose or change data; provide an explicit codec'
    )
  }
  return text
}

async function encodeCodec<Schema extends StandardSchemaV1>(
  contract: SchemaCodec<Schema>,
  value: StandardSchemaV1.InferOutput<Schema>
): Promise<StandardSchemaV1.InferInput<Schema>> {
  try {
    return await contract.encode(value)
  } catch (cause) {
    throw new SchemaDefectException('encode', { cause })
  }
}

/** Encode an already decoded value, checking both JSON fidelity and the schema round trip. */
export async function encodeSchema<Contract extends ValueSchema>(
  contract: Contract,
  value: SchemaOutput<Contract>
): Promise<string> {
  if (CODEC in contract) {
    const text = jsonText(await encodeCodec(contract, value))
    const decoded = await decodeSchema(contract, text)
    if (!isDeepStrictEqual(decoded, value)) {
      throw new SchemaEncodingException('The codec does not round-trip the decoded value')
    }
    return text
  }

  const validated = await validateSchema(contract, value)
  if (!isDeepStrictEqual(validated, value)) {
    throw new SchemaEncodingException(
      'The schema transforms decoded values; provide an explicit codec'
    )
  }
  return jsonText(validated)
}

/** Decode persisted JSON and revalidate it against the current versioned contract. */
export async function decodeSchema<Contract extends ValueSchema>(
  contract: Contract,
  text: string
): Promise<SchemaOutput<Contract>> {
  let parsed: ReturnType<JSON['parse']>
  try {
    parsed = JSON.parse(text)
  } catch (cause) {
    throw new SchemaEncodingException('The persisted value is not valid JSON', { cause })
  }
  return validateSchema(contract, parsed)
}
