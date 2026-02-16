import { GetSchemaVersionCommand } from "@aws-sdk/client-glue";
import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { Ajv } from "ajv";
import { z } from "zod";

import { clearAwsGlueSchemaCache, createAwsGlueCodec, createAwsGlueSchemaRegistryAdapter, type AwsGlueClientLike, type AwsGlueResolvedSchemaCacheEntry, type AwsGlueSchemaFetcherLike } from "../libs/schema/adapters/aws-glue-adapter.js";
import { KTSchemaRegistryError, KTSchemaValidationError } from "../libs/schema/schema-errors.js";

type GluePayload = {
  id: number
}

const GLUE_SCHEMA = {
  type: "object",
  properties: {
    id: { type: "number" },
  },
  required: ["id"],
  additionalProperties: false,
} as const

describe("aws glue schema adapter", () => {
  beforeEach(() => {
    clearAwsGlueSchemaCache()
  })

  it("should fetch and create codec from aws glue schema", async () => {
    const getSchema = jest.fn(() => {
      return Promise.resolve({
        schemaDefinition: JSON.stringify(GLUE_SCHEMA),
        schemaName: "test-user-events",
        schemaVersionId: "version-id-1",
        schemaVersionNumber: 4,
        dataFormat: "JSON",
      })
    })
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema,
    }
    const ajv = new Ajv()
    const codec = await createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        registryName: "test-registry",
        schemaName: "test-user-events",
      },
    })

    expect(getSchema).toHaveBeenCalledTimes(1)
    expect(codec.schemaMeta).toEqual({
      provider: "aws-glue",
      schemaName: "test-user-events",
      schemaVersion: "4",
      schemaId: "version-id-1",
    })
    expect(codec.encode({ id: 1 })).toBe(JSON.stringify({ id: 1 }))
    expect(codec.decode(JSON.stringify({ id: 2 }))).toEqual({ id: 2 })
  })

  it("should fetch schema through native aws adapter using schemaVersionId", async () => {
    const send = jest.fn((command: GetSchemaVersionCommand) => {
      expect(command).toBeInstanceOf(GetSchemaVersionCommand)
      expect(command.input).toEqual({
        SchemaVersionId: "version-id-native",
      })

      return Promise.resolve({
        SchemaDefinition: JSON.stringify(GLUE_SCHEMA),
        SchemaVersionId: "version-id-native",
        VersionNumber: 9,
        DataFormat: "JSON",
      })
    })
    const mockClient: AwsGlueClientLike = {
      send,
      destroy: jest.fn(),
    }
    const adapter = await createAwsGlueSchemaRegistryAdapter({
      region: "us-east-1",
      client: mockClient,
    })
    const ajv = new Ajv()
    const codec = await createAwsGlueCodec<GluePayload>({
      glue: adapter,
      ajv,
      schema: {
        schemaName: "native-events",
        schemaVersionId: "version-id-native",
      },
    })

    expect(send).toHaveBeenCalledTimes(1)
    expect(codec.decode(JSON.stringify({ id: 55 }))).toEqual({ id: 55 })
    adapter.destroy()
  })

  it("should preload schemas via native adapter and then use cache", async () => {
    const send = jest.fn((command: GetSchemaVersionCommand) => {
      expect(command.input).toEqual({
        SchemaId: {
          SchemaArn: undefined,
          SchemaName: "preloaded-events",
          RegistryName: "preloaded-registry",
        },
        SchemaVersionNumber: {
          VersionNumber: 3,
        },
      })

      return Promise.resolve({
        SchemaDefinition: JSON.stringify(GLUE_SCHEMA),
        SchemaVersionId: "preloaded-id",
        VersionNumber: 3,
        DataFormat: "JSON",
      })
    })
    const mockClient: AwsGlueClientLike = {
      send,
      destroy: jest.fn(),
    }
    const cacheStore = new Map<string, AwsGlueResolvedSchemaCacheEntry>()
    const adapter = await createAwsGlueSchemaRegistryAdapter({
      region: "us-east-1",
      client: mockClient,
      preload: {
        schemas: [{
          registryName: "preloaded-registry",
          schemaName: "preloaded-events",
          schemaVersionNumber: 3,
        }],
        cache: {
          store: cacheStore,
        },
      },
    })
    const ajv = new Ajv()
    const codec = await createAwsGlueCodec<GluePayload>({
      glue: adapter,
      ajv,
      cache: {
        store: cacheStore,
      },
      schema: {
        registryName: "preloaded-registry",
        schemaName: "preloaded-events",
        schemaVersionNumber: 3,
      },
    })

    expect(send).toHaveBeenCalledTimes(1)
    expect(codec.decode(JSON.stringify({ id: 77 }))).toEqual({ id: 77 })
    adapter.destroy()
  })

  it("should initialize native adapter with explicit static credentials", async () => {
    const send = jest.fn(() => {
      return Promise.resolve({
        SchemaDefinition: JSON.stringify(GLUE_SCHEMA),
        SchemaVersionId: "credentialed-id",
        VersionNumber: 1,
        DataFormat: "JSON",
      })
    })
    const mockClient: AwsGlueClientLike = {
      send,
      destroy: jest.fn(),
    }
    const adapter = await createAwsGlueSchemaRegistryAdapter({
      region: "us-east-1",
      credentials: {
        accessKeyId: "AKIA_TEST_KEY",
        secretAccessKey: "secret_test_key",
      },
      client: mockClient,
    })
    const fetched = await adapter.getSchema({
      schemaName: "credentialed-events",
      schemaVersionId: "credentialed-id",
    })

    expect(send).toHaveBeenCalledTimes(1)
    expect(fetched.schemaVersionId).toBe("credentialed-id")
    adapter.destroy()
  })

  it("should use in-memory cache and avoid duplicated glue fetch", async () => {
    const getSchema = jest.fn(() => {
      return Promise.resolve({
        schemaDefinition: JSON.stringify(GLUE_SCHEMA),
        schemaName: "cached-events",
        schemaVersionId: "version-id-cache",
        schemaVersionNumber: 1,
        dataFormat: "JSON",
      })
    })
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema,
    }
    const ajv = new Ajv()
    const codecFirst = await createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        registryName: "reg-cache",
        schemaName: "cached-events",
        schemaVersionId: "version-id-cache",
      },
    })
    const codecSecond = await createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        registryName: "reg-cache",
        schemaName: "cached-events",
        schemaVersionId: "version-id-cache",
      },
    })

    expect(getSchema).toHaveBeenCalledTimes(1)
    expect(codecFirst.decode(JSON.stringify({ id: 10 }))).toEqual({ id: 10 })
    expect(codecSecond.decode(JSON.stringify({ id: 11 }))).toEqual({ id: 11 })
  })

  it("should reuse cached schema across ajv and zod validators", async () => {
    const getSchema = jest.fn(() => {
      return Promise.resolve({
        schemaDefinition: JSON.stringify(GLUE_SCHEMA),
        schemaName: "mixed-validator-events",
        schemaVersionId: "version-id-mixed",
        schemaVersionNumber: 2,
        dataFormat: "JSON",
      })
    })
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema,
    }
    const ajv = new Ajv()
    const ajvCodec = await createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        registryName: "reg-mixed",
        schemaName: "mixed-validator-events",
        schemaVersionId: "version-id-mixed",
      },
    })
    const zodCodec = await createAwsGlueCodec<GluePayload>({
      validator: "zod",
      glue: glueFetcher,
      schema: {
        registryName: "reg-mixed",
        schemaName: "mixed-validator-events",
        schemaVersionId: "version-id-mixed",
      },
      zodSchemaFactory: () => {
        return z.object({
          id: z.number(),
        }).meta({
          id: "mixed-validator-events",
          schemaVersion: "2",
        })
      },
    })

    expect(getSchema).toHaveBeenCalledTimes(1)
    expect(ajvCodec.decode(JSON.stringify({ id: 10 }))).toEqual({ id: 10 })
    expect(zodCodec.decode(JSON.stringify({ id: 11 }))).toEqual({ id: 11 })
    expect(zodCodec.schemaMeta).toEqual({
      provider: "aws-glue",
      schemaName: "mixed-validator-events",
      schemaVersion: "2",
      schemaId: "version-id-mixed",
    })
  })

  it("should throw deterministic registry error when glue fetch failed", async () => {
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema: () => Promise.reject(new Error("network timeout")),
    }
    const ajv = new Ajv()

    await expect(createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        schemaName: "failed-schema",
      },
    })).rejects.toThrow(KTSchemaRegistryError)
    await expect(createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        schemaName: "failed-schema",
      },
    })).rejects.toThrow("Unable to fetch schema from AWS Glue registry")
  })

  it("should throw validation error for invalid payload", async () => {
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema: () => {
        return Promise.resolve({
          schemaDefinition: JSON.stringify(GLUE_SCHEMA),
          schemaName: "validate-events",
          dataFormat: "JSON",
        })
      },
    }
    const ajv = new Ajv()
    const codec = await createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        schemaName: "validate-events",
      },
    })

    expect(() => codec.encode({ id: "bad" } as unknown as GluePayload)).toThrow(KTSchemaValidationError)
    expect(() => codec.encode({ id: "bad" } as unknown as GluePayload)).toThrow("AJV validation failed during encode")
  })

  it("should validate payloads with zod validator mode", async () => {
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema: () => {
        return Promise.resolve({
          schemaDefinition: JSON.stringify(GLUE_SCHEMA),
          schemaName: "validate-events-zod",
          schemaVersionId: "version-id-zod",
          dataFormat: "JSON",
        })
      },
    }
    const codec = await createAwsGlueCodec<GluePayload>({
      validator: "zod",
      glue: glueFetcher,
      schema: {
        schemaName: "validate-events-zod",
      },
      zodSchemaFactory: () => {
        return z.object({
          id: z.number(),
        })
      },
    })

    expect(codec.decode(JSON.stringify({ id: 12 }))).toEqual({ id: 12 })
    expect(() => codec.encode({ id: "bad" } as unknown as GluePayload)).toThrow(KTSchemaValidationError)
    expect(() => codec.encode({ id: "bad" } as unknown as GluePayload)).toThrow("Zod validation failed during encode")
  })

  it("should throw deterministic registry error for unsupported glue data format", async () => {
    const glueFetcher: AwsGlueSchemaFetcherLike = {
      getSchema: () => {
        return Promise.resolve({
          schemaDefinition: "message Test {}",
          schemaName: "proto-events",
          dataFormat: "PROTOBUF",
        })
      },
    }
    const ajv = new Ajv()

    await expect(createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        schemaName: "proto-events",
      },
    })).rejects.toThrow(KTSchemaRegistryError)
    await expect(createAwsGlueCodec<GluePayload>({
      glue: glueFetcher,
      ajv,
      schema: {
        schemaName: "proto-events",
      },
    })).rejects.toThrow("Unsupported AWS Glue schema format")
  })
})
