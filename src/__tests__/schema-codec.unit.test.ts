import { describe, expect, it } from "@jest/globals";

import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import type { KTCodec } from "../libs/schema/schema-codec.js";

type BatchValue = {
  value: number
}

describe("schema codec integration", () => {
  it("should support codec in CreateKTTopicBatch", () => {
    const codec: KTCodec<BatchValue> = {
      encode: (data) => JSON.stringify({ value: data.value + 1 }),
      decode: (data) => {
        const dataString = Buffer.isBuffer(data)
          ? data.toString()
          : data

        return JSON.parse(dataString) as BatchValue
      },
    }

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

    expect(payload.messages[0]?.value).toBe(JSON.stringify({ value: 2 }))
    expect(BaseTopic.decode(payload.messages[0]?.value || "{}")).toEqual({ value: 2 })
  })
})
