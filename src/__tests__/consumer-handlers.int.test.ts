import { randomUUID } from "node:crypto";

import { describe, expect, it } from "@jest/globals";
import { pino } from "pino";

import { KTHandler } from "../kafka/consumer-handler.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { KTMessageQueue } from "../message-queue/index.js";

const getIntTestConfig = () => {
  return {
    brokerUrl: process.env.KAFKA_BROKER_URL ?? "localhost:19092",
    timeoutMs: Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000),
  };
};

describe("Consumer handlers integration", () => {
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
});
