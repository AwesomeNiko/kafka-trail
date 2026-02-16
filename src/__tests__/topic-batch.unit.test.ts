import { describe, expect, it } from "@jest/globals";

import { CreateKTTopicBatch, KTTopicBatch } from "../kafka/topic-batch.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

describe("CreateKTTopicBatch", () => {
  it("should create batch payload with encoded values and default headers", () => {
    const { BaseTopic } = CreateKTTopicBatch({
      topic: KafkaTopicName.fromString("test.batch.topic"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    });

    const payload = BaseTopic([
      {
        value: { a: 1 },
        key: KafkaMessageKey.fromString("k1"),
      },
      {
        value: { b: 2 },
        key: KafkaMessageKey.NULL,
      },
    ]);

    expect(payload).toEqual({
      topicName: "test.batch.topic",
      messages: [
        {
          value: JSON.stringify({ a: 1 }),
          key: "k1",
          headers: {},
        },
        {
          value: JSON.stringify({ b: 2 }),
          key: null,
          headers: {},
        },
      ],
    });
  });

  it("should keep provided headers", () => {
    const { BaseTopic } = CreateKTTopicBatch({
      topic: KafkaTopicName.fromString("test.batch.topic.headers"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    });

    const payload = BaseTopic([
      {
        value: { a: 1 },
        key: KafkaMessageKey.fromString("k1"),
        headers: {
          traceId: "trace-1",
          "x-test": "1",
        },
      },
    ]);

    expect(payload.messages[0]?.headers).toEqual({
      traceId: "trace-1",
      "x-test": "1",
    });
  });

  it("should return DLQTopic = null when createDLQ is false", () => {
    const { DLQTopic } = CreateKTTopicBatch({
      topic: KafkaTopicName.fromString("test.batch.topic.no-dlq"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    });

    expect(DLQTopic).toBeNull();
  });

  it("should return DLQTopic when createDLQ is true and prefix topic with dlq.", () => {
    const { DLQTopic } = CreateKTTopicBatch({
      topic: KafkaTopicName.fromString("test.batch.topic.with-dlq"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: true,
      configEntries: [],
    });

    expect(DLQTopic).not.toBeNull();
    expect(DLQTopic?.topicSettings.topic).toBe("dlq.test.batch.topic.with-dlq");
  });

  it("should throw clear runtime deprecation error for KTTopicBatch", () => {
    expect(() => KTTopicBatch({
      topic: KafkaTopicName.fromString("test.deprecated.batch-topic"),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
      configEntries: [],
    })).toThrow("Deprecated. use CreateKTTopicBatch(...)");
  });
});
