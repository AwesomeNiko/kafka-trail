import { describe, expect, it, jest } from "@jest/globals";
import { z } from "zod";

import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { CreateKTTopic, KTTopic } from "../kafka/topic.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createZodCodec } from "../libs/schema/adapters/zod-adapter.js";
import { KTSchemaValidationError } from "../libs/schema/schema-errors.js";

type TopicPayload = {
  fieldForPayload: number
}

const createTopicSettings = (topic: string, createDLQ = false) => ({
  topic: KafkaTopicName.fromString(topic),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ,
  configEntries: [],
})

const ZOD_TOPIC_SCHEMA = z.object({
  fieldForPayload: z.number(),
}).meta({
  id: "topic-zod-model",
  schemaVersion: "1",
})

describe("CreateKTTopic", () => {
  it("should encode payload with default parser and add traceId", () => {
    const { BaseTopic } = CreateKTTopic<TopicPayload>(createTopicSettings("test.create-topic.default"));

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
    const { BaseTopic } = CreateKTTopic<TopicPayload>(createTopicSettings("test.create-topic.custom"), {
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

  it("should validate KTTopic payloads with real zod schema", () => {
    const codec = createZodCodec(ZOD_TOPIC_SCHEMA)
    const encodeSpy = jest.spyOn(codec, "encode")
    const decodeSpy = jest.spyOn(codec, "decode")

    const { BaseTopic } = CreateKTTopic<TopicPayload>(createTopicSettings("test.create-topic.validate"), codec);

    const validPayload = BaseTopic({ fieldForPayload: 1 }, {
      messageKey: KafkaMessageKey.fromString("validate-key"),
      meta: {},
    });

    expect(BaseTopic.decode(validPayload.message)).toEqual({ fieldForPayload: 1 });
    expect(encodeSpy).toHaveBeenCalledWith({ fieldForPayload: 1 })
    expect(decodeSpy).toHaveBeenCalledWith(validPayload.message)

    expect(() => BaseTopic({
      fieldForPayload: "bad",
    } as unknown as { fieldForPayload: number }, {
      messageKey: KafkaMessageKey.fromString("invalid-encode-key"),
      meta: {},
    })).toThrow(KTSchemaValidationError);
    expect(encodeSpy).toHaveBeenCalledTimes(1)

    expect(() => BaseTopic.decode(JSON.stringify({
      fieldForPayload: "bad",
    }))).toThrow(KTSchemaValidationError);
    expect(decodeSpy).toHaveBeenCalledTimes(2)
  });

  it("should return DLQTopic = null when createDLQ is false", () => {
    const { DLQTopic } = CreateKTTopic(createTopicSettings("test.create-topic.no-dlq"));

    expect(DLQTopic).toBeNull();
  });

  it("should return DLQTopic when createDLQ is true and prefix topic with dlq.", () => {
    const { DLQTopic } = CreateKTTopic(createTopicSettings("test.create-topic.with-dlq", true));

    expect(DLQTopic).not.toBeNull();
    expect(DLQTopic?.topicSettings.topic).toBe("dlq.test.create-topic.with-dlq");
  });

  it("should create batch payload for handler topic", () => {
    const { BaseTopic } = CreateKTTopicBatch(createTopicSettings("test.create-topic.batch"));

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
    expect(() => KTTopic(createTopicSettings("test.deprecated.topic"))).toThrow("Deprecated. use CreateKTTopic(...)");
  });
});
