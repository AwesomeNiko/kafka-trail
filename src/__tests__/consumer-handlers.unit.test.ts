import { describe, expect, it } from "@jest/globals";

import { CreateKTTopic } from "../kafka/topic.ts";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.ts";

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
});

