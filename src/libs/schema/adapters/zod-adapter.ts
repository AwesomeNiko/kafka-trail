import type { KTCodec, KTSchemaMeta } from "../schema-codec.js";
import { KTSchemaValidationError } from "../schema-errors.js";

type ZodSafeParseSuccess<T> = {
  success: true
  data: T
}

type ZodSafeParseFailure = {
  success: false
  error: {
    message: string
  }
}

export type ZodSchemaLike<T> = {
  safeParse: (input: unknown) => ZodSafeParseSuccess<T> | ZodSafeParseFailure
  meta?: () => unknown
}

const deriveSchemaMeta = (schemaMeta: unknown): KTSchemaMeta => {
  if (!schemaMeta || typeof schemaMeta !== "object") {
    return {}
  }

  const metaRecord = schemaMeta as Record<string, unknown>
  const schemaNameCandidate = metaRecord.schemaName ?? metaRecord.id ?? metaRecord.title
  const schemaVersionCandidate = metaRecord.schemaVersion ?? metaRecord.version
  const schemaIdCandidate = metaRecord.schemaId ?? metaRecord.id

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

export const createZodCodec = <Payload extends object>(
  schema: ZodSchemaLike<Payload>,
  options?: {
    schemaMeta?: KTSchemaMeta
  },
): KTCodec<Payload> => {
  const parseWithSchema = (value: unknown, source: 'encode' | 'decode' | 'validate'): Payload => {
    const result = schema.safeParse(value)

    if (!result.success) {
      throw new KTSchemaValidationError({
        message: `Zod validation failed during ${source}: ${result.error.message}`,
        source,
        details: result.error,
      })
    }

    return result.data
  }

  const codec: KTCodec<Payload> = {
    encode(data) {
      return JSON.stringify(parseWithSchema(data, 'encode'))
    },
    decode(data) {
      const stringData = Buffer.isBuffer(data)
        ? data.toString()
        : data

      const parsed = JSON.parse(stringData) as unknown

      return parseWithSchema(parsed, 'decode')
    },
    validate(data): asserts data is Payload {
      parseWithSchema(data, 'validate')
    },
  }

  const schemaDerivedMeta = deriveSchemaMeta(schema.meta?.())
  codec.schemaMeta = {
    provider: "zod",
    ...schemaDerivedMeta,
    ...options?.schemaMeta,
  }

  return codec
}
