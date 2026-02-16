import { randomUUID } from "node:crypto";

import { describe, expect, it } from "@jest/globals";

import { KTHandler } from "../kafka/consumer-handler.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { KTMessageQueue } from "../message-queue/index.js";

const getIntTestConfig = () => {
  return {
    brokerUrl: process.env.KAFKA_BROKER_URL ?? "localhost:19092",
    timeoutMs: Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 30_000),
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
          connectionTimeout: 30_000,
        },
        pureConfig: {},
      });

      await producerMq.initTopics([TestTopic]);

      await consumerMq.initConsumer({
        kafkaSettings: {
          brokerUrls: [brokerUrl],
          clientId: consumerClientId,
          connectionTimeout: 30_000,
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
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 30_000));
});
