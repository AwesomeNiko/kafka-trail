import { GlueClient, GetSchemaVersionCommand } from "@aws-sdk/client-glue";
import type { GlueClientConfig, GetSchemaVersionCommandInput } from "@aws-sdk/client-glue";

import type { KTCodec, KTSchemaMeta } from "../schema-codec.js";
import { KTSchemaRegistryError } from "../schema-errors.js";

import type { AjvCompilerLike, AjvSchemaLike, AjvValidateLike } from "./ajv-adapter.js";
import { createAjvCodec } from "./ajv-adapter.js";
import type { ZodSchemaLike } from "./zod-adapter.js";
import { createZodCodec } from "./zod-adapter.js";

export type AwsGlueSchemaLookup = {
  registryName?: string
  schemaName: string
  schemaArn?: string
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

export type AwsGlueClientLike = {
  send: (command: GetSchemaVersionCommand) => Promise<{
    SchemaDefinition?: string
    SchemaVersionId?: string
    VersionNumber?: number
    SchemaArn?: string
    DataFormat?: string
  }>
  destroy?: () => void
}

export type AwsGlueStaticCredentials = {
  accessKeyId: string
  secretAccessKey: string
  sessionToken?: string
}

export type AwsGlueAdapterPreload = {
  schemas: AwsGlueSchemaLookup[]
  cache?: AwsGlueCodecCacheOptions
}

export type CreateAwsGlueSchemaRegistryAdapterParams = {
  region: string
  credentials?: AwsGlueStaticCredentials
  profile?: string
  endpoint?: string
  preload?: AwsGlueAdapterPreload
  client?: AwsGlueClientLike
}

export type AwsGlueSchemaRegistryAdapter = AwsGlueSchemaFetcherLike & {
  preloadSchemas: (schemas: AwsGlueSchemaLookup[], options?: { cache?: AwsGlueCodecCacheOptions }) => Promise<void>
  destroy: () => void
}

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

const asVersionNumber = (value: string | number | undefined): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value)

    if (Number.isFinite(parsed)) {
      return parsed
    }
  }

  return undefined
}

const createSchemaCacheKey = (lookup: AwsGlueSchemaLookup): string => {
  const registryName = lookup.registryName ?? "default"
  const schemaArn = lookup.schemaArn ?? ""
  const schemaVersionId = lookup.schemaVersionId ?? ""
  const schemaVersionNumber = lookup.schemaVersionNumber ?? ""

  return `${registryName}::${lookup.schemaName}::${schemaArn}::${schemaVersionId}::${schemaVersionNumber}`
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
    ?? params.lookup.schemaArn

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

const createGetSchemaVersionInput = (lookup: AwsGlueSchemaLookup): GetSchemaVersionCommandInput => {
  if (lookup.schemaVersionId) {
    return {
      SchemaVersionId: lookup.schemaVersionId,
    }
  }

  const schemaVersionNumber = asVersionNumber(lookup.schemaVersionNumber)

  if (lookup.schemaVersionNumber !== undefined && schemaVersionNumber === undefined) {
    throw new KTSchemaRegistryError({
      message: `Invalid schemaVersionNumber for AWS Glue lookup: ${String(lookup.schemaVersionNumber)}`,
      details: {
        schemaName: lookup.schemaName,
        schemaVersionNumber: lookup.schemaVersionNumber,
      },
    })
  }

  const schemaId = {
    SchemaArn: lookup.schemaArn,
    SchemaName: lookup.schemaArn ? undefined : lookup.schemaName,
    RegistryName: lookup.schemaArn ? undefined : lookup.registryName,
  }

  const commandInput: GetSchemaVersionCommandInput = {
    SchemaId: schemaId,
  }

  if (schemaVersionNumber !== undefined) {
    commandInput.SchemaVersionNumber = {
      VersionNumber: schemaVersionNumber,
    }
  } else {
    commandInput.SchemaVersionNumber = {
      LatestVersion: true,
    }
  }

  return commandInput
}

const fetchSchemaFromAwsGlue = async (params: {
  client: AwsGlueClientLike
  lookup: AwsGlueSchemaLookup
}): Promise<AwsGlueSchemaFetcherResult> => {
  const commandInput = createGetSchemaVersionInput(params.lookup)
  const command = new GetSchemaVersionCommand(commandInput)
  const response = await params.client.send(command)

  if (!response.SchemaDefinition) {
    throw new KTSchemaRegistryError({
      message: `AWS Glue returned empty schema definition for schemaName "${params.lookup.schemaName}"`,
      details: response,
    })
  }

  const fetchedSchema: AwsGlueSchemaFetcherResult = {
    schemaDefinition: response.SchemaDefinition,
    schemaName: params.lookup.schemaName,
  }

  if (response.SchemaVersionId) {
    fetchedSchema.schemaVersionId = response.SchemaVersionId
  }

  if (response.VersionNumber !== undefined) {
    fetchedSchema.schemaVersionNumber = response.VersionNumber
  }

  if (response.SchemaArn) {
    fetchedSchema.schemaArn = response.SchemaArn
  }

  if (response.DataFormat) {
    fetchedSchema.dataFormat = response.DataFormat
  }

  return fetchedSchema
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

export const preloadAwsGlueSchemas = async (params: {
  glue: AwsGlueSchemaFetcherLike
  schemas: AwsGlueSchemaLookup[]
  cache?: AwsGlueCodecCacheOptions
}) => {
  for (const schemaLookup of params.schemas) {
    const resolveParams = {
      glue: params.glue,
      lookup: schemaLookup,
      cache: params.cache,
    }

    await resolveGlueSchema(resolveParams)
  }
}

const createGlueClient = (params: CreateAwsGlueSchemaRegistryAdapterParams): AwsGlueClientLike => {
  if (params.client) {
    return params.client
  }

  const clientConfig: GlueClientConfig = {
    region: params.region,
  }

  if (params.credentials) {
    clientConfig.credentials = params.credentials
  }

  if (params.profile) {
    clientConfig.profile = params.profile
  }

  if (params.endpoint) {
    clientConfig.endpoint = params.endpoint
  }

  const glueClient = new GlueClient(clientConfig)

  return glueClient
}

export const createAwsGlueSchemaRegistryAdapter = async (
  params: CreateAwsGlueSchemaRegistryAdapterParams,
): Promise<AwsGlueSchemaRegistryAdapter> => {
  const client = createGlueClient(params)

  const adapter: AwsGlueSchemaRegistryAdapter = {
    async getSchema(lookup) {
      const fetchParams = {
        client,
        lookup,
      }

      return fetchSchemaFromAwsGlue(fetchParams)
    },
    async preloadSchemas(schemas, options) {
      const preloadParams: {
        glue: AwsGlueSchemaFetcherLike
        schemas: AwsGlueSchemaLookup[]
        cache?: AwsGlueCodecCacheOptions
      } = {
        glue: adapter,
        schemas,
      }

      if (options?.cache) {
        preloadParams.cache = options.cache
      }

      await preloadAwsGlueSchemas(preloadParams)
    },
    destroy() {
      if (client.destroy) {
        client.destroy()
      }
    },
  }

  if (params.preload?.schemas.length) {
    const preloadParams: {
      glue: AwsGlueSchemaFetcherLike
      schemas: AwsGlueSchemaLookup[]
      cache?: AwsGlueCodecCacheOptions
    } = {
      glue: adapter,
      schemas: params.preload.schemas,
    }

    if (params.preload.cache) {
      preloadParams.cache = params.preload.cache
    }

    await preloadAwsGlueSchemas(preloadParams)
  }

  return adapter
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

    const zodSchemaFactoryParams = {
      schema: resolvedSchema.schema,
      lookup: params.schema,
      fetchedSchema: resolvedSchema.fetchedSchema,
      schemaMeta: mergedSchemaMeta,
    }
    const zodSchema = params.zodSchemaFactory(zodSchemaFactoryParams)

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
