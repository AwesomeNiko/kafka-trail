import { randomUUID } from "node:crypto";

import { describe, expect, it } from "@jest/globals";
import { pino } from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

const getIntTestConfig = () => {
  return {
    brokerUrl: process.env.KAFKA_BROKER_URL ?? "localhost:19092",
    timeoutMs: Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000),
  };
};

describe("Kafka producer integration", () => {
  it("should create topic and publish single message", async () => {
    const { brokerUrl } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.producer.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: [brokerUrl],
        clientId: producerClientId,
        connectionTimeout: 10_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    try {
      await kafkaProducer.init();

      await kafkaProducer.createTopic({
        topic: topicName,
        numPartitions: 2,
        configEntries: [],
      });

      await kafkaProducer.sendSingleMessage({
        topicName,
        messageKey: KafkaMessageKey.fromString("1"),
        value: JSON.stringify({ value: 1 }),
        headers: {},
      });
    } finally {
      await kafkaProducer.destroy();
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should reject partition decrease for existing topic", async () => {
    const { brokerUrl } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.producer.partitions.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: [brokerUrl],
        clientId: producerClientId,
        connectionTimeout: 10_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    try {
      await kafkaProducer.init();

      await kafkaProducer.createTopic({
        topic: topicName,
        numPartitions: 2,
        configEntries: [],
      });

      await expect(kafkaProducer.createTopic({
        topic: topicName,
        numPartitions: 1,
        configEntries: [],
      })).rejects.toBeInstanceOf(UnableDecreasePartitionsError);
    } finally {
      await kafkaProducer.destroy();
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));

  it("should send batch messages", async () => {
    const { brokerUrl } = getIntTestConfig();
    const suffix = randomUUID();
    const topicName = KafkaTopicName.fromString(`test.int.producer.batch.${suffix}`);
    const producerClientId = KafkaClientId.fromString(`producer-${suffix}`);

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: [brokerUrl],
        clientId: producerClientId,
        connectionTimeout: 10_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    try {
      await kafkaProducer.init();

      await kafkaProducer.createTopic({
        topic: topicName,
        numPartitions: 2,
        configEntries: [],
      });

      await kafkaProducer.sendBatchMessages({
        topicName,
        messages: [
          {
            key: KafkaMessageKey.fromString("1"),
            value: JSON.stringify({ value: 1 }),
            headers: {},
          },
          {
            key: KafkaMessageKey.fromString("2"),
            value: JSON.stringify({ value: 2 }),
            headers: {},
          },
        ],
      });
    } finally {
      await kafkaProducer.destroy();
    }
  }, Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 10_000));
});
