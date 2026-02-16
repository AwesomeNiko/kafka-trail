import { describe, expect, it } from "@jest/globals";
import { Ajv } from "ajv";
import { z } from "zod";

import { createAjvCodecFromSchema } from "../libs/schema/adapters/ajv-adapter.js";
import { createZodCodec } from "../libs/schema/adapters/zod-adapter.js";
import { KTSchemaValidationError } from "../libs/schema/schema-errors.js";

type TestPayload = {
  id: number
}

const ZOD_SCHEMA_WITH_META = z.object({
  id: z.number(),
}).meta({
  id: "test-payload-id",
  schemaVersion: "1",
})

const ZOD_SCHEMA_PLAIN = z.object({
  id: z.number(),
})

const AJV_SCHEMA_WITH_META = {
  $id: "test-payload-id",
  title: "test-payload-title",
  type: "object",
  properties: {
    id: { type: "number" },
  },
  required: ["id"],
  additionalProperties: false,
  "x-schema-version": "1",
} as const

const AJV_SCHEMA_PLAIN = {
  type: "object",
  properties: {
    id: { type: "number" },
  },
  required: ["id"],
  additionalProperties: false,
} as const

const createAjvWithSchemaVersionKeyword = () => {
  const ajv = new Ajv()
  ajv.addKeyword({
    keyword: "x-schema-version",
    schemaType: "string",
  })

  return ajv
}

describe("schema adapters", () => {
  it("should derive schema metadata from zod schema", () => {
    const codec = createZodCodec(ZOD_SCHEMA_WITH_META)

    expect(codec.schemaMeta).toEqual({
      provider: "zod",
      schemaName: "test-payload-id",
      schemaId: "test-payload-id",
      schemaVersion: "1",
    })

    const encoded = codec.encode({ id: 1 })
    expect(encoded).toBe(JSON.stringify({ id: 1 }))
    expect(codec.decode(encoded)).toEqual({ id: 1 })
  })

  it("should allow overriding derived zod metadata via options", () => {
    const codec = createZodCodec(ZOD_SCHEMA_WITH_META, {
      schemaMeta: {
        schemaName: "override-name",
      },
    })

    expect(codec.schemaMeta).toEqual({
      provider: "zod",
      schemaName: "override-name",
      schemaId: "test-payload-id",
      schemaVersion: "1",
    })
  })

  it("should throw deterministic validation error for invalid zod payload", () => {
    const codec = createZodCodec(ZOD_SCHEMA_PLAIN)

    expect(() => codec.encode({ id: "invalid" } as unknown as TestPayload)).toThrow(KTSchemaValidationError)
    expect(() => codec.encode({ id: "invalid" } as unknown as TestPayload)).toThrow("Zod validation failed during encode")
  })

  it("should encode/decode with ajv schema and derive metadata", () => {
    const ajv = createAjvWithSchemaVersionKeyword()

    const codec = createAjvCodecFromSchema<TestPayload>({
      ajv,
      schema: AJV_SCHEMA_WITH_META,
    })

    expect(codec.schemaMeta).toEqual({
      provider: "ajv",
      schemaName: "test-payload-title",
      schemaId: "test-payload-id",
      schemaVersion: "1",
    })

    const encoded = codec.encode({ id: 7 })
    expect(encoded).toBe(JSON.stringify({ id: 7 }))
    expect(codec.decode(encoded)).toEqual({ id: 7 })
  })

  it("should throw deterministic validation error for invalid ajv payload", () => {
    const ajv = new Ajv()

    const codec = createAjvCodecFromSchema<TestPayload>({
      ajv,
      schema: AJV_SCHEMA_PLAIN,
    })

    expect(() => codec.decode(JSON.stringify({ id: "bad" }))).toThrow(KTSchemaValidationError)
    expect(() => codec.decode(JSON.stringify({ id: "bad" }))).toThrow("AJV validation failed during decode")
  })
})
