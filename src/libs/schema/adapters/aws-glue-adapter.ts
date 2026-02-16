import type { KTCodec, KTSchemaMeta } from "../schema-codec.js";
import { KTSchemaRegistryError } from "../schema-errors.js";

import type { AjvCompilerLike, AjvSchemaLike, AjvValidateLike } from "./ajv-adapter.js";
import { createAjvCodec } from "./ajv-adapter.js";
import type { ZodSchemaLike } from "./zod-adapter.js";
import { createZodCodec } from "./zod-adapter.js";

export type AwsGlueSchemaLookup = {
  registryName?: string
  schemaName: string
  schemaVersionId?: string
  schemaVersionNumber?: string | number
}

export type AwsGlueSchemaFetcherResult = {
  schemaDefinition: string | AjvSchemaLike
  schemaName?: string
  schemaVersionId?: string
  schemaVersionNumber?: string | number
  schemaArn?: string
  dataFormat?: string
}

export type AwsGlueSchemaFetcherLike = {
  getSchema: (lookup: AwsGlueSchemaLookup) => Promise<AwsGlueSchemaFetcherResult>
}

export type AwsGlueCodecCacheOptions = {
  store?: Map<string, AwsGlueResolvedSchemaCacheEntry>
  ttlMs?: number
}

export type AwsGlueResolvedSchemaCacheEntry = {
  schema: AjvSchemaLike
  schemaMeta: KTSchemaMeta
  expiresAt: number | null
}

export type AwsGlueCompiledSchemaCacheEntry = AwsGlueResolvedSchemaCacheEntry

type CreateAwsGlueCodecParamsBase = {
  glue: AwsGlueSchemaFetcherLike
  schema: AwsGlueSchemaLookup
  schemaMeta?: KTSchemaMeta
  cache?: AwsGlueCodecCacheOptions
}

export type AwsGlueZodSchemaFactoryParams = {
  schema: AjvSchemaLike
  lookup: AwsGlueSchemaLookup
  fetchedSchema: AwsGlueSchemaFetcherResult
  schemaMeta: KTSchemaMeta
}

export type AwsGlueZodSchemaFactory<Payload extends object> = (
  params: AwsGlueZodSchemaFactoryParams,
) => ZodSchemaLike<Payload>

type CreateAwsGlueCodecWithAjvParams<Payload extends object> = CreateAwsGlueCodecParamsBase & {
  validator?: "ajv"
  ajv: AjvCompilerLike<Payload>
  serialize?: (data: Payload) => string
  parse?: (data: string) => unknown
}

type CreateAwsGlueCodecWithZodParams<Payload extends object> = CreateAwsGlueCodecParamsBase & {
  validator: "zod"
  zodSchemaFactory: AwsGlueZodSchemaFactory<Payload>
}

type CreateAwsGlueCodecParams<Payload extends object> =
  | CreateAwsGlueCodecWithAjvParams<Payload>
  | CreateAwsGlueCodecWithZodParams<Payload>

const DEFAULT_AWS_GLUE_SCHEMA_CACHE = new Map<string, AwsGlueResolvedSchemaCacheEntry>()

const asSchemaVersion = (value: string | number | undefined): string | undefined => {
  if (typeof value === "string") {
    return value
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value)
  }

  return undefined
}

const createSchemaCacheKey = (lookup: AwsGlueSchemaLookup): string => {
  const registryName = lookup.registryName ?? "default"
  const schemaVersionId = lookup.schemaVersionId ?? ""
  const schemaVersionNumber = lookup.schemaVersionNumber ?? ""

  return `${registryName}::${lookup.schemaName}::${schemaVersionId}::${schemaVersionNumber}`
}

const getCacheEntry = (params: {
  store: Map<string, AwsGlueResolvedSchemaCacheEntry>
  key: string
  now: number
}) => {
  const cacheEntry = params.store.get(params.key)

  if (!cacheEntry) {
    return undefined
  }

  if (cacheEntry.expiresAt !== null && cacheEntry.expiresAt <= params.now) {
    params.store.delete(params.key)

    return undefined
  }

  return cacheEntry
}

const parseSchemaDefinition = (schemaDefinition: string | AjvSchemaLike): AjvSchemaLike => {
  if (typeof schemaDefinition !== "string") {
    return schemaDefinition
  }

  try {
    const parsed = JSON.parse(schemaDefinition) as unknown

    if (!parsed || typeof parsed !== "object") {
      throw new Error("Schema definition JSON must resolve to an object")
    }

    return parsed as AjvSchemaLike
  } catch (error) {
    throw new KTSchemaRegistryError({
      message: "Unable to parse AWS Glue schema definition as JSON object",
      details: error,
    })
  }
}

const deriveSchemaMeta = (params: {
  lookup: AwsGlueSchemaLookup
  fetchedSchema: AwsGlueSchemaFetcherResult
}): KTSchemaMeta => {
  const schemaName = params.fetchedSchema.schemaName ?? params.lookup.schemaName
  const schemaVersion = asSchemaVersion(params.fetchedSchema.schemaVersionNumber)
    ?? asSchemaVersion(params.lookup.schemaVersionNumber)
  const schemaId = params.fetchedSchema.schemaVersionId
    ?? params.lookup.schemaVersionId
    ?? params.fetchedSchema.schemaArn

  const schemaMeta: KTSchemaMeta = {
    provider: "aws-glue",
  }

  if (schemaName) {
    schemaMeta.schemaName = schemaName
  }

  if (schemaVersion) {
    schemaMeta.schemaVersion = schemaVersion
  }

  if (schemaId) {
    schemaMeta.schemaId = schemaId
  }

  return schemaMeta
}

const resolveSchemaFromGlue = (params: {
  schemaLookup: AwsGlueSchemaLookup
  fetchedSchema: AwsGlueSchemaFetcherResult
}): {
  schema: AjvSchemaLike
  schemaMeta: KTSchemaMeta
} => {
  if (params.fetchedSchema.dataFormat && params.fetchedSchema.dataFormat !== "JSON") {
    throw new KTSchemaRegistryError({
      message: `Unsupported AWS Glue schema format: ${params.fetchedSchema.dataFormat}. Only JSON is supported`,
      details: {
        schemaName: params.fetchedSchema.schemaName ?? params.schemaLookup.schemaName,
        dataFormat: params.fetchedSchema.dataFormat,
      },
    })
  }

  const parsedSchema = parseSchemaDefinition(params.fetchedSchema.schemaDefinition)
  const schemaMeta = deriveSchemaMeta({
    lookup: params.schemaLookup,
    fetchedSchema: params.fetchedSchema,
  })

  return {
    schema: parsedSchema,
    schemaMeta,
  }
}

const resolveGlueSchema = async (params: {
  glue: AwsGlueSchemaFetcherLike
  lookup: AwsGlueSchemaLookup
  cache: AwsGlueCodecCacheOptions | undefined
}): Promise<{
  schema: AjvSchemaLike
  schemaMeta: KTSchemaMeta
  fetchedSchema: AwsGlueSchemaFetcherResult
}> => {
  const store = params.cache?.store ?? DEFAULT_AWS_GLUE_SCHEMA_CACHE
  const cacheKey = createSchemaCacheKey(params.lookup)
  const now = Date.now()
  const existingEntry = getCacheEntry({
    store,
    key: cacheKey,
    now,
  })

  if (existingEntry) {
    return {
      schema: existingEntry.schema,
      schemaMeta: existingEntry.schemaMeta,
      fetchedSchema: {
        schemaDefinition: existingEntry.schema,
      },
    }
  }

  let fetchedSchema: AwsGlueSchemaFetcherResult

  try {
    fetchedSchema = await params.glue.getSchema(params.lookup)
  } catch (error) {
    throw new KTSchemaRegistryError({
      message: `Unable to fetch schema from AWS Glue registry for schemaName "${params.lookup.schemaName}"`,
      details: error,
    })
  }

  const resolvedSchema = resolveSchemaFromGlue({
    schemaLookup: params.lookup,
    fetchedSchema,
  })

  const ttlMs = params.cache?.ttlMs
  const expiresAt = typeof ttlMs === "number" && ttlMs > 0
    ? now + ttlMs
    : null
  const cacheEntry: AwsGlueResolvedSchemaCacheEntry = {
    schema: resolvedSchema.schema,
    schemaMeta: resolvedSchema.schemaMeta,
    expiresAt,
  }

  store.set(cacheKey, cacheEntry)

  return {
    schema: cacheEntry.schema,
    schemaMeta: cacheEntry.schemaMeta,
    fetchedSchema,
  }
}

export const clearAwsGlueSchemaCache = (store?: Map<string, AwsGlueResolvedSchemaCacheEntry>) => {
  const resolvedStore = store ?? DEFAULT_AWS_GLUE_SCHEMA_CACHE
  resolvedStore.clear()
}

export const createAwsGlueCodec = async <Payload extends object>(
  params: CreateAwsGlueCodecParams<Payload>,
): Promise<KTCodec<Payload>> => {
  const resolvedSchema = await resolveGlueSchema({
    glue: params.glue,
    lookup: params.schema,
    cache: params.cache,
  })

  const mergedSchemaMeta = {
    ...resolvedSchema.schemaMeta,
    ...params.schemaMeta,
  }

  if (params.validator === "zod") {
    if (!params.zodSchemaFactory) {
      throw new KTSchemaRegistryError({
        message: "zodSchemaFactory is required when validator is set to \"zod\"",
      })
    }

    const zodSchema = params.zodSchemaFactory({
      schema: resolvedSchema.schema,
      lookup: params.schema,
      fetchedSchema: resolvedSchema.fetchedSchema,
      schemaMeta: mergedSchemaMeta,
    })

    const codec = createZodCodec(zodSchema, {
      schemaMeta: mergedSchemaMeta,
    })

    return codec
  }

  if (!params.ajv) {
    throw new KTSchemaRegistryError({
      message: "ajv compiler is required for AWS Glue codec with ajv validator",
    })
  }

  const validate = params.ajv.compile(resolvedSchema.schema)

  const createCodecParams: {
    validate: AjvValidateLike<Payload>
    serialize?: (data: Payload) => string
    parse?: (data: string) => unknown
    schemaMeta: KTSchemaMeta
  } = {
    validate,
    schemaMeta: mergedSchemaMeta,
  }

  if (params.serialize) {
    createCodecParams.serialize = params.serialize
  }

  if (params.parse) {
    createCodecParams.parse = params.parse
  }

  return createAjvCodec(createCodecParams)
}
