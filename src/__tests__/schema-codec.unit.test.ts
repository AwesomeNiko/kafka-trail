import { describe, expect, it, jest } from "@jest/globals";
import { Ajv } from "ajv";

import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createAjvCodecFromSchema } from "../libs/schema/adapters/ajv-adapter.js";
import { KTSchemaValidationError } from "../libs/schema/schema-errors.js";

type BatchValue = {
  value: number
}

const AJV_BATCH_SCHEMA = {
  $id: "batch-ajv-model",
  title: "batch-ajv-title",
  "x-schema-version": "1",
  type: "object",
  properties: {
    value: { type: "number" },
  },
  required: ["value"],
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

describe("schema codec integration", () => {
  it("should validate KTTopicBatch values with real ajv schema", () => {
    const ajv = createAjvWithSchemaVersionKeyword()
    const codec = createAjvCodecFromSchema<BatchValue>({
      ajv,
      schema: AJV_BATCH_SCHEMA,
    })
    const encodeSpy = jest.spyOn(codec, "encode")
    const decodeSpy = jest.spyOn(codec, "decode")

    const { BaseTopic } = CreateKTTopicBatch<
      Array<{
        value: BatchValue
        key: KafkaMessageKey
      }>
    >({
      topic: KafkaTopicName.fromString("test.batch.codec"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    }, codec)

    const payload = BaseTopic([
      {
        value: { value: 1 },
        key: KafkaMessageKey.fromString("k1"),
      },
    ])

    expect(codec.schemaMeta).toEqual({
      provider: "ajv",
      schemaName: "batch-ajv-title",
      schemaId: "batch-ajv-model",
      schemaVersion: "1",
    })
    expect(BaseTopic.decode(payload.messages[0]?.value || "{}")).toEqual({ value: 1 })
    expect(encodeSpy).toHaveBeenCalledWith({ value: 1 })
    expect(decodeSpy).toHaveBeenCalledWith(payload.messages[0]?.value || "{}")

    expect(() => BaseTopic([
      {
        value: {
          value: "bad",
        } as unknown as BatchValue,
        key: KafkaMessageKey.fromString("k2"),
      },
    ])).toThrow(KTSchemaValidationError)
    expect(encodeSpy).toHaveBeenCalledTimes(1)

    expect(() => BaseTopic.decode(JSON.stringify({
      value: "bad",
    }))).toThrow(KTSchemaValidationError)
    expect(decodeSpy).toHaveBeenCalledTimes(2)
  })
})
