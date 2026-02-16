import { describe, expect, it } from "@jest/globals";

import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { CreateKTTopic, KTTopic } from "../kafka/topic.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

describe("CreateKTTopic", () => {
  it("should encode payload with default parser and add traceId", () => {
    const { BaseTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString("test.create-topic.default"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    });

    const payload = BaseTopic({ fieldForPayload: 1 }, {
      messageKey: KafkaMessageKey.NULL,
      meta: {},
    });

    expect(payload.topicName).toBe("test.create-topic.default");
    expect(payload.message).toBe(JSON.stringify({ fieldForPayload: 1 }));
    expect(payload.messageKey).toBeNull();
    expect(payload.meta.traceId).toEqual(expect.any(String));
  });

  it("should use custom encoder and decoder", () => {
    const { BaseTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString("test.create-topic.custom"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    }, {
      encode: (data) => JSON.stringify({
        fieldForPayload: data.fieldForPayload + 100,
      }),
      decode: (data: string | Buffer) => {
        const dataString = Buffer.isBuffer(data)
          ? data.toString()
          : data;

        const parsed = JSON.parse(dataString) as {
          fieldForPayload: number
        };

        return {
          fieldForPayload: parsed.fieldForPayload + 100,
        };
      },
    });

    const payload = BaseTopic({ fieldForPayload: 1 }, {
      messageKey: KafkaMessageKey.fromString("custom-key"),
      meta: {},
    });

    expect(payload.message).toBe(JSON.stringify({ fieldForPayload: 101 }));
    expect(BaseTopic.decode(payload.message)).toEqual({ fieldForPayload: 201 });
  });

  it("should return DLQTopic = null when createDLQ is false", () => {
    const { DLQTopic } = CreateKTTopic({
      topic: KafkaTopicName.fromString("test.create-topic.no-dlq"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    });

    expect(DLQTopic).toBeNull();
  });

  it("should return DLQTopic when createDLQ is true and prefix topic with dlq.", () => {
    const { DLQTopic } = CreateKTTopic({
      topic: KafkaTopicName.fromString("test.create-topic.with-dlq"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: true,
      configEntries: [],
    });

    expect(DLQTopic).not.toBeNull();
    expect(DLQTopic?.topicSettings.topic).toBe("dlq.test.create-topic.with-dlq");
  });

  it("should create batch payload for handler topic", () => {
    const { BaseTopic } = CreateKTTopicBatch({
      topic: KafkaTopicName.fromString("test.create-topic.batch"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    });

    const payload = BaseTopic([
      {
        value: { fieldForPayload: 1 },
        key: KafkaMessageKey.fromString("batch-key-1"),
      },
      {
        value: { fieldForPayload: 2 },
        key: KafkaMessageKey.fromString("batch-key-2"),
      },
    ]);

    expect(payload.topicName).toBe("test.create-topic.batch");
    expect(payload.messages).toEqual([
      {
        value: JSON.stringify({ fieldForPayload: 1 }),
        key: "batch-key-1",
        headers: {},
      },
      {
        value: JSON.stringify({ fieldForPayload: 2 }),
        key: "batch-key-2",
        headers: {},
      },
    ]);
  });

  it("should throw clear runtime deprecation error for KTTopic", () => {
    expect(() => KTTopic({
      topic: KafkaTopicName.fromString("test.deprecated.topic"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    })).toThrow("Deprecated. use CreateKTTopic(...)");
  });
});
