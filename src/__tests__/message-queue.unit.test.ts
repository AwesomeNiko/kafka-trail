import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import { Ajv } from "ajv";
import { z } from "zod";

import { ProducerInitRequiredForDLQError, ProducerNotInitializedError } from "../custom-errors/kafka-errors.js";
import { KTHandler } from "../kafka/consumer-handler.js";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createAjvCodecFromSchema } from "../libs/schema/adapters/ajv-adapter.js";
import { createZodCodec } from "../libs/schema/adapters/zod-adapter.js";
import { KTSchemaValidationError } from "../libs/schema/schema-errors.js";
import { KTMessageQueue } from "../message-queue/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

// @ts-expect-error too much return arguments
const kafkaProducerInitMock = jest.spyOn(KTKafkaProducer.prototype, 'init').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaConsumerInitMock = jest.spyOn(KTKafkaConsumer.prototype, 'init').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaConsumerSubscribeTopicMock = jest.spyOn(KTKafkaConsumer.prototype, 'subscribeTopic').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerCreateTopicMock = jest.spyOn(KTKafkaProducer.prototype, 'createTopic').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerSendSingleMessageMock = jest.spyOn(KTKafkaProducer.prototype, 'sendSingleMessage').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerSendBatchMessagesMock = jest.spyOn(KTKafkaProducer.prototype, 'sendBatchMessages').mockImplementation(jest.fn);

type RuntimeTopicPayload = {
  fieldForPayload: number
}

type RuntimeBatchValue = {
  value: number
}

const ZOD_RUNTIME_SCHEMA = z.object({
  fieldForPayload: z.number(),
})

const AJV_RUNTIME_SCHEMA = {
  type: "object",
  properties: {
    value: { type: "number" },
  },
  required: ["value"],
  additionalProperties: false,
} as const

const createTopicSettings = (topicName: string, createDLQ = false) => ({
  topic: KafkaTopicName.fromString(topicName),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ,
})

const createTopicBatchSettings = (topicName: string, createDLQ = false) => ({
  topic: KafkaTopicName.fromString(topicName),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ,
  configEntries: [],
})

describe("KTMessageQueue test", () => {
  beforeEach(() => {
    kafkaProducerInitMock.mockClear();
    kafkaProducerCreateTopicMock.mockClear();
    kafkaProducerSendSingleMessageMock.mockClear();
    kafkaProducerSendBatchMessagesMock.mockClear();
    kafkaConsumerInitMock.mockClear();
    kafkaConsumerSubscribeTopicMock.mockClear();
  });

  it("should init for producer successfully", async () => {
    const mq = new KTMessageQueue();

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    expect(kafkaProducerInitMock).toHaveBeenCalledTimes(1);
  });

  it("should register handlers successfully", async () => {
    const { BaseTopic: TestExampleTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example"))

    const testExampleTopicHandler = KTHandler({
      topic: TestExampleTopic,
      run: (payload) => {
        const data = payload[0]

        if (!data) {
          return Promise.resolve()
        }

        return Promise.resolve()
      },
    })

    const mq = new KTMessageQueue();

    const { clearAll } = createKafkaMocks({
      topicName: TestExampleTopic.topicSettings.topic,
    });

    mq.registerHandlers([testExampleTopicHandler]);

    await mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    })

    expect(kafkaConsumerInitMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledWith(['test.example']);
    clearAll()
  });

  it("should throw clear error when publishSingleMessage is called before initProducer", async () => {
    const { BaseTopic: TestExampleTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example.publish.single"))
    const mq = new KTMessageQueue();
    const payload = TestExampleTopic({
      fieldForPayload: 1,
    }, {
      messageKey: KafkaMessageKey.fromString("key-1"),
      meta: {},
    })

    await expect(mq.publishSingleMessage(payload)).rejects.toBeInstanceOf(ProducerNotInitializedError);
    expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledTimes(0);
  });

  it("should throw clear error when publishBatchMessages is called before initProducer", async () => {
    const { BaseTopic: TestBatchTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example.publish.batch"))
    const mq = new KTMessageQueue();
    const batchPayload = {
      topicName: TestBatchTopic.topicSettings.topic,
      messages: [{
        key: KafkaMessageKey.fromString("key-2"),
        value: JSON.stringify({ fieldForPayload: 1 }),
        headers: {},
      }],
    }

    await expect(mq.publishBatchMessages(batchPayload)).rejects.toBeInstanceOf(ProducerNotInitializedError);
    expect(kafkaProducerSendBatchMessagesMock).toHaveBeenCalledTimes(0);
  });

  describe("publishTopicMessage runtime validation", () => {
    it("should publish valid zod payload via BaseTopic builder and publishSingleMessage", async () => {
      const codec = createZodCodec(ZOD_RUNTIME_SCHEMA);
      const encodeSpy = jest.spyOn(codec, "encode");
      const decodeSpy = jest.spyOn(codec, "decode");
      const { BaseTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example.runtime.topic"), codec);

      const mq = new KTMessageQueue();

      await mq.initProducer({
        kafkaSettings: {
          brokerUrls: ['localhost'],
          clientId: KafkaClientId.fromString("broker-client-id-1"),
          connectionTimeout: 30000,
        },
        pureConfig: {},
      });

      const payload = BaseTopic({
        fieldForPayload: 1,
      }, {
        messageKey: KafkaMessageKey.fromString("runtime-topic-key"),
        meta: {},
      });
      BaseTopic.decode(payload.message);
      await mq.publishSingleMessage(payload);

      expect(encodeSpy).toHaveBeenCalledWith({ fieldForPayload: 1 });
      expect(decodeSpy).toHaveBeenCalledWith(payload.message);
      expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledTimes(1);
      expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledWith({
        topicName: "test.example.runtime.topic",
        value: JSON.stringify({ fieldForPayload: 1 }),
        messageKey: "runtime-topic-key",
        headers: expect.objectContaining({
          traceId: expect.any(String),
        }),
      });
    });

    it("should reject invalid zod payload in BaseTopic builder before publishSingleMessage", () => {
      const codec = createZodCodec(ZOD_RUNTIME_SCHEMA);
      const encodeSpy = jest.spyOn(codec, "encode");
      const { BaseTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example.runtime.topic.invalid"), codec);

      expect(() => BaseTopic({
        fieldForPayload: "bad",
      } as unknown as { fieldForPayload: number }, {
        messageKey: KafkaMessageKey.fromString("runtime-topic-invalid-key"),
        meta: {},
      })).toThrow(KTSchemaValidationError);
      expect(encodeSpy).toHaveBeenCalledTimes(0);
      expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledTimes(0);
    });
  });

  describe("publishTopicBatch runtime validation", () => {
    it("should publish valid ajv payload via BaseTopicBatch builder and publishBatchMessages", async () => {
      const ajv = new Ajv();
      const codec = createAjvCodecFromSchema<RuntimeBatchValue>({
        ajv,
        schema: AJV_RUNTIME_SCHEMA,
      });
      const encodeSpy = jest.spyOn(codec, "encode");
      const decodeSpy = jest.spyOn(codec, "decode");
      const { BaseTopic } = CreateKTTopicBatch<Array<{
        value: {
          value: number
        },
        key: KafkaMessageKey
      }>>(createTopicBatchSettings("test.example.runtime.batch"), codec);
      const mq = new KTMessageQueue();

      await mq.initProducer({
        kafkaSettings: {
          brokerUrls: ['localhost'],
          clientId: KafkaClientId.fromString("broker-client-id-1"),
          connectionTimeout: 30000,
        },
        pureConfig: {},
      });

      const payload = BaseTopic([{
        value: { value: 10 },
        key: KafkaMessageKey.fromString("runtime-batch-key"),
      }]);
      BaseTopic.decode(payload.messages[0]?.value || "{}");
      await mq.publishBatchMessages(payload);

      expect(encodeSpy).toHaveBeenCalledWith({ value: 10 });
      expect(decodeSpy).toHaveBeenCalledWith(payload.messages[0]?.value || "{}");
      expect(kafkaProducerSendBatchMessagesMock).toHaveBeenCalledTimes(1);
      expect(kafkaProducerSendBatchMessagesMock).toHaveBeenCalledWith({
        topicName: "test.example.runtime.batch",
        messages: [{
          key: "runtime-batch-key",
          value: JSON.stringify({ value: 10 }),
          headers: {},
        }],
      });
    });

    it("should reject invalid ajv payload in BaseTopicBatch builder before publishBatchMessages", () => {
      const ajv = new Ajv();
      const codec = createAjvCodecFromSchema<RuntimeBatchValue>({
        ajv,
        schema: AJV_RUNTIME_SCHEMA,
      });
      const encodeSpy = jest.spyOn(codec, "encode");
      const { BaseTopic } = CreateKTTopicBatch<Array<{
        value: {
          value: number
        },
        key: KafkaMessageKey
      }>>(createTopicBatchSettings("test.example.runtime.batch.invalid"), codec);

      expect(() => BaseTopic([{
        value: {
          value: "bad",
        } as unknown as { value: number },
        key: KafkaMessageKey.fromString("runtime-batch-invalid-key"),
      }])).toThrow(KTSchemaValidationError);
      expect(encodeSpy).toHaveBeenCalledTimes(0);
      expect(kafkaProducerSendBatchMessagesMock).toHaveBeenCalledTimes(0);
    });
  });

  it("should throw clear error when initConsumer is called with DLQ-enabled handler", async () => {
    const { BaseTopic: DLQTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example.consumer.dlq", true))

    const mq = new KTMessageQueue();
    const dlqHandler = KTHandler({
      topic: DLQTopic,
      run: () => Promise.resolve(),
    })

    mq.registerHandlers([dlqHandler]);

    await expect(mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    })).rejects.toBeInstanceOf(ProducerInitRequiredForDLQError);

    expect(kafkaConsumerInitMock).toHaveBeenCalledTimes(0);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledTimes(0);
  });

  it("should allow initConsumer with DLQ-enabled handler when producer is initialized", async () => {
    const { BaseTopic: DLQTopic } = CreateKTTopic<RuntimeTopicPayload>(createTopicSettings("test.example.consumer.dlq.allowed", true))

    const mq = new KTMessageQueue();
    const dlqHandler = KTHandler({
      topic: DLQTopic,
      run: () => Promise.resolve(),
    })

    mq.registerHandlers([dlqHandler]);

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    await mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    });

    expect(kafkaProducerInitMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerInitMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledTimes(1);
  });
});
