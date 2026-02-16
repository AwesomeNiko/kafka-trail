import { randomUUID } from "node:crypto";

import { describe, expect, it } from "@jest/globals";
import { pino } from "pino";
import { z } from "zod";

import { KTHandler } from "../kafka/consumer-handler.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createZodCodec } from "../libs/schema/adapters/zod-adapter.js";
import { KTSchemaValidationError } from "../libs/schema/schema-errors.js";
import { KTMessageQueue } from "../message-queue/index.js";

const getIntTestConfig = () => {
  return {
    brokerUrl: process.env.KAFKA_BROKER_URL ?? "localhost:19092",
    timeoutMs: Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000),
  };
};

describe("Consumer handlers integration", () => {
  it("should validate producer payload with zod schema in integration flow", async () => {
    const { brokerUrl } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.zod.producer.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);
    const producerMq = new KTMessageQueue();

    try {
      const codec = createZodCodec(z.object({
        fieldForPayload: z.number(),
      }));
      const { BaseTopic: TestTopic } = CreateKTTopic<{
        fieldForPayload: number
      }>({
        topic: topicName,
        numPartitions: 1,
        batchMessageSizeToConsume: 10,
        createDLQ: false,
        configEntries: [],
      }, codec);

      await producerMq.initProducer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: producerClientId,
          connectionTimeout: 10_000,
        },
        pureConfig: {},
      });

      await producerMq.initTopics([TestTopic]);

      await expect(producerMq.publishSingleMessage(TestTopic({
        fieldForPayload: 1,
      }, {
        messageKey: KafkaMessageKey.fromString("zod-valid-producer-key"),
        meta: {},
      }))).resolves.toBeUndefined();

      expect(() => TestTopic({
        fieldForPayload: "bad",
      } as unknown as { fieldForPayload: number }, {
        messageKey: KafkaMessageKey.fromString("zod-invalid-producer-key"),
        meta: {},
      })).toThrow(KTSchemaValidationError);
    } finally {
      await producerMq.destroyAll();
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should validate consumer payload with zod schema in integration flow", async () => {
    const { brokerUrl, timeoutMs } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.zod.consumer.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);
    const consumerClientId = KafkaClientId.fromString(`consumer-${suffix}`);
    const consumerGroupId = `group-${suffix}`;

    const producerMq = new KTMessageQueue();
    const consumerMq = new KTMessageQueue();

    try {
      const codec = createZodCodec(z.object({
        fieldForPayload: z.number(),
      }));
      const { BaseTopic: TestTopic } = CreateKTTopic<{
        fieldForPayload: number
      }>({
        topic: topicName,
        numPartitions: 1,
        batchMessageSizeToConsume: 10,
        createDLQ: false,
        configEntries: [],
      }, codec);

      const receivePromise = new Promise<number>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Timeout waiting for consumed zod-valid message"));
        }, timeoutMs);

        const handler = KTHandler({
          topic: TestTopic,
          run: async (payload) => {
            await Promise.resolve();
            const [data] = payload;

            if (!data) {
              return;
            }

            clearTimeout(timeout);
            resolve(data.fieldForPayload);
          },
        });

        consumerMq.registerHandlers([handler]);
      });

      await producerMq.initProducer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: producerClientId,
          connectionTimeout: 10_000,
        },
        pureConfig: {},
      });

      await producerMq.initTopics([TestTopic]);

      await consumerMq.initConsumer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: consumerClientId,
          connectionTimeout: 10_000,
          consumerGroupId,
        },
        pureConfig: {},
      });

      await producerMq.publishSingleMessage(TestTopic({
        fieldForPayload: 42,
      }, {
        messageKey: KafkaMessageKey.fromString("zod-valid-consumer-key"),
        meta: {},
      }));

      await expect(receivePromise).resolves.toBe(42);

      expect(() => TestTopic.decode(JSON.stringify({
        fieldForPayload: "bad",
      }))).toThrow(KTSchemaValidationError);
    } finally {
      await Promise.all([
        producerMq.destroyAll(),
        consumerMq.destroyAll(),
      ]);
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should publish and consume message through kafka", async () => {
    const { brokerUrl, timeoutMs } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.consumer.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);
    const consumerClientId = KafkaClientId.fromString(`consumer-${suffix}`);
    const consumerGroupId = `group-${suffix}`;

    const producerMq = new KTMessageQueue();
    const consumerMq = new KTMessageQueue();

    try {
      const { BaseTopic: TestTopic } = CreateKTTopic<{
        fieldForPayload: number
      }>({
        topic: topicName,
        numPartitions: 1,
        batchMessageSizeToConsume: 10,
        createDLQ: false,
        configEntries: [],
      });

      const receivePromise = new Promise<number>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Timeout waiting for consumed message"));
        }, timeoutMs);

        const handler = KTHandler({
          topic: TestTopic,
          run: async (payload) => {
            const [data] = payload;

            if (!data) {
              return;
            }

            clearTimeout(timeout);
            resolve(data.fieldForPayload);

            return Promise.resolve()
          },
        });

        consumerMq.registerHandlers([handler]);
      });

      await producerMq.initProducer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: producerClientId,
          connectionTimeout: 10_000,
        },
        pureConfig: {},
      });

      await producerMq.initTopics([TestTopic]);

      await consumerMq.initConsumer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: consumerClientId,
          connectionTimeout: 10_000,
          consumerGroupId,
        },
        pureConfig: {},
      });

      const payload = TestTopic({ fieldForPayload: 42 }, {
        messageKey: KafkaMessageKey.fromString("42"),
        meta: {},
      });

      await producerMq.publishSingleMessage(payload);

      await expect(receivePromise).resolves.toBe(42);
    } finally {
      await Promise.all([
        producerMq.destroyAll(),
        consumerMq.destroyAll(),
      ]);
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should consume batch messages in batch mode", async () => {
    const { brokerUrl, timeoutMs } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.consumer.batch.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);
    const consumerClientId = KafkaClientId.fromString(`consumer-${suffix}`);
    const consumerGroupId = `group-${suffix}`;

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: [brokerUrl],
        clientId: producerClientId,
        connectionTimeout: 10_000,
      },
      pureConfig: {},
      logger: pino(),
    });
    const consumerMq = new KTMessageQueue();

    try {
      const { BaseTopic: BatchTopic } = CreateKTTopicBatch({
        topic: topicName,
        numPartitions: 1,
        batchMessageSizeToConsume: 10,
        createDLQ: false,
        configEntries: [],
      });

      const receivePromise = new Promise<number[]>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Timeout waiting for consumed batch"));
        }, timeoutMs);

        const handler = KTHandler({
          topic: BatchTopic,
          run: async (payload) => {
            await Promise.resolve();
            clearTimeout(timeout);
            resolve(payload.map((item) => (item as unknown as { fieldForPayload: number }).fieldForPayload));
          },
        });

        consumerMq.registerHandlers([handler]);
      });

      await kafkaProducer.init();
      await kafkaProducer.createTopic({
        topic: topicName,
        numPartitions: 1,
        configEntries: [],
      });

      await consumerMq.initConsumer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: consumerClientId,
          connectionTimeout: 10_000,
          consumerGroupId,
          batchConsuming: true,
        },
        pureConfig: {},
      });

      await kafkaProducer.sendBatchMessages(BatchTopic([
        {
          value: { fieldForPayload: 11 },
          key: KafkaMessageKey.fromString("batch-1"),
        },
        {
          value: { fieldForPayload: 12 },
          key: KafkaMessageKey.fromString("batch-2"),
        },
      ]));

      await expect(receivePromise).resolves.toEqual([11, 12]);
    } finally {
      await Promise.all([
        kafkaProducer.destroy(),
        consumerMq.destroyAll(),
      ]);
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should limit consumed batch size and provide resolveOffset in batch mode", async () => {
    const { brokerUrl, timeoutMs } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.consumer.batch.limit.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);
    const consumerClientId = KafkaClientId.fromString(`consumer-${suffix}`);
    const consumerGroupId = `group-${suffix}`;

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: [brokerUrl],
        clientId: producerClientId,
        connectionTimeout: 10_000,
      },
      pureConfig: {},
      logger: pino(),
    });
    const consumerMq = new KTMessageQueue();

    try {
      const { BaseTopic: BatchTopic } = CreateKTTopicBatch({
        topic: topicName,
        numPartitions: 1,
        batchMessageSizeToConsume: 1,
        createDLQ: false,
        configEntries: [],
      });

      const receivePromise = new Promise<{
        length: number
        hasResolveOffset: boolean
        lastOffset: string | undefined
      }>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Timeout waiting for limited consumed batch"));
        }, timeoutMs);

        const handler = KTHandler({
          topic: BatchTopic,
          run: async (payload, _ctx, _publisher, kafkaTopicParams) => {
            await Promise.resolve();

            kafkaTopicParams.resolveOffset?.(kafkaTopicParams.lastOffset ?? "0");
            clearTimeout(timeout);

            resolve({
              length: payload.length,
              hasResolveOffset: typeof kafkaTopicParams.resolveOffset === "function",
              lastOffset: kafkaTopicParams.lastOffset,
            });
          },
        });

        consumerMq.registerHandlers([handler]);
      });

      await kafkaProducer.init();
      await kafkaProducer.createTopic({
        topic: topicName,
        numPartitions: 1,
        configEntries: [],
      });

      await consumerMq.initConsumer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: consumerClientId,
          connectionTimeout: 10_000,
          consumerGroupId,
          batchConsuming: true,
        },
        pureConfig: {},
      });

      await kafkaProducer.sendBatchMessages(BatchTopic([
        {
          value: { fieldForPayload: 21 },
          key: KafkaMessageKey.fromString("batch-limit-1"),
        },
        {
          value: { fieldForPayload: 22 },
          key: KafkaMessageKey.fromString("batch-limit-2"),
        },
      ]));

      await expect(receivePromise).resolves.toEqual({
        length: 1,
        hasResolveOffset: true,
        lastOffset: "0",
      });
    } finally {
      await Promise.all([
        kafkaProducer.destroy(),
        consumerMq.destroyAll(),
      ]);
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should publish failed message to DLQ when handler throws and createDLQ is enabled", async () => {
    const { brokerUrl, timeoutMs } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.consumer.dlq.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);
    const consumerClientId = KafkaClientId.fromString(`consumer-${suffix}`);
    const dlqProducerClientId = KafkaClientId.fromString(`consumer-producer-${suffix}`);
    const consumerGroupId = `group-${suffix}`;
    const dlqError = "dlq-test-error";

    const producerMq = new KTMessageQueue();
    const consumerMq = new KTMessageQueue();

    try {
      const {
        BaseTopic: MainTopic,
        DLQTopic,
      } = CreateKTTopic<{
        fieldForPayload: number
      }>({
        topic: topicName,
        numPartitions: 1,
        batchMessageSizeToConsume: 10,
        createDLQ: true,
        configEntries: [],
      });

      if (!DLQTopic) {
        throw new Error("DLQ topic was not created");
      }

      const dlqReceivePromise = new Promise<{
        originalTopic: string
        errorMessage: string
        valueLength: number
      }>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error("Timeout waiting for DLQ message"));
        }, timeoutMs);

        const failingHandler = KTHandler({
          topic: MainTopic,
          run: async () => {
            await Promise.resolve();
            throw new Error(dlqError);
          },
        });

        const dlqHandler = KTHandler({
          topic: DLQTopic,
          run: async (payload) => {
            await Promise.resolve();
            const [dlqMessage] = payload as unknown as Array<{
              originalTopic: string
              errorMessage: string
              value: object[]
            }>;

            if (!dlqMessage) {
              return;
            }

            clearTimeout(timeout);
            resolve({
              originalTopic: dlqMessage.originalTopic,
              errorMessage: dlqMessage.errorMessage,
              valueLength: dlqMessage.value.length,
            });
          },
        });

        consumerMq.registerHandlers([failingHandler]);
        consumerMq.registerHandlers([dlqHandler]);
      });

      await producerMq.initProducer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: producerClientId,
          connectionTimeout: 10_000,
        },
        pureConfig: {},
      });

      await consumerMq.initProducer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: dlqProducerClientId,
          connectionTimeout: 10_000,
        },
        pureConfig: {},
      });

      await producerMq.initTopics([MainTopic]);
      await producerMq.initTopics([DLQTopic]);

      await consumerMq.initConsumer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: consumerClientId,
          connectionTimeout: 10_000,
          consumerGroupId,
        },
        pureConfig: {},
      });

      await producerMq.publishSingleMessage(MainTopic({
        fieldForPayload: 777,
      }, {
        messageKey: KafkaMessageKey.fromString("dlq-key-1"),
        meta: {},
      }));

      await expect(dlqReceivePromise).resolves.toEqual({
        originalTopic: topicName,
        errorMessage: dlqError,
        valueLength: 1,
      });
    } finally {
      await Promise.all([
        producerMq.destroyAll(),
        consumerMq.destroyAll(),
      ]);
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));
});
