import type { KTCodec, KTSchemaMeta } from "../schema-codec.js";
import { KTSchemaValidationError } from "../schema-errors.js";

export type AjvErrorLike = {
  instancePath?: string
  message?: string
  keyword?: string
  params?: Record<string, unknown>
}

export type AjvValidateLike<Payload extends object> = ((data: unknown) => data is Payload) & {
  errors?: AjvErrorLike[] | null
}

export type AjvSchemaLike = Record<string, unknown>

export type AjvCompilerLike<Payload extends object> = {
  compile: (schema: AjvSchemaLike) => AjvValidateLike<Payload>
}

const deriveSchemaMeta = (schema: AjvSchemaLike): KTSchemaMeta => {
  const schemaNameCandidate = schema["x-schema-name"] ?? schema.title ?? schema.$id
  const schemaVersionCandidate = schema["x-schema-version"] ?? schema.schemaVersion ?? schema.version
  const schemaIdCandidate = schema.$id

  const result: KTSchemaMeta = {}

  if (typeof schemaNameCandidate === "string") {
    result.schemaName = schemaNameCandidate
  }

  if (typeof schemaVersionCandidate === "string") {
    result.schemaVersion = schemaVersionCandidate
  }

  if (typeof schemaIdCandidate === "string") {
    result.schemaId = schemaIdCandidate
  }

  return result
}

export const createAjvCodec = <Payload extends object>(params: {
  validate: AjvValidateLike<Payload>
  serialize?: (data: Payload) => string
  parse?: (data: string) => unknown
  schemaMeta?: KTSchemaMeta
}): KTCodec<Payload> => {
  const serialize = params.serialize ?? JSON.stringify
  const parse = params.parse ?? ((data: string) => JSON.parse(data) as unknown)

  const ensureValid = (data: unknown, source: 'encode' | 'decode' | 'validate'): Payload => {
    if (!params.validate(data)) {
      throw new KTSchemaValidationError({
        message: `AJV validation failed during ${source}`,
        source,
        details: params.validate.errors,
      })
    }

    return data
  }

  const codec: KTCodec<Payload> = {
    encode(data) {
      const validData = ensureValid(data, 'encode')

      return serialize(validData)
    },
    decode(data) {
      const stringData = Buffer.isBuffer(data)
        ? data.toString()
        : data

      const parsed = parse(stringData)

      return ensureValid(parsed, 'decode')
    },
    validate(data): asserts data is Payload {
      ensureValid(data, 'validate')
    },
  }

  codec.schemaMeta = {
    provider: "ajv",
    ...params.schemaMeta,
  }

  return codec
}

export const createAjvCodecFromSchema = <Payload extends object>(params: {
  ajv: AjvCompilerLike<Payload>
  schema: AjvSchemaLike
  serialize?: (data: Payload) => string
  parse?: (data: string) => unknown
  schemaMeta?: KTSchemaMeta
}): KTCodec<Payload> => {
  const validate = params.ajv.compile(params.schema)
  const schemaDerivedMeta = deriveSchemaMeta(params.schema)

  const createParams: {
    validate: AjvValidateLike<Payload>
    serialize?: (data: Payload) => string
    parse?: (data: string) => unknown
    schemaMeta: KTSchemaMeta
  } = {
    validate,
    schemaMeta: {
      ...schemaDerivedMeta,
      ...params.schemaMeta,
    },
  }

  if (params.serialize) {
    createParams.serialize = params.serialize
  }

  if (params.parse) {
    createParams.parse = params.parse
  }

  return createAjvCodec(createParams)
}
