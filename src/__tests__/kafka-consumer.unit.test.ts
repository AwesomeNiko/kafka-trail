import { beforeEach, describe, expect, it } from "@jest/globals";
import { pino } from "pino";

import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import type { KTPartitionAssigner } from "../kafka/kafka-types.js";
import { KafkaClientId, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

const { consumerSubscribe, createConsumerMock } = createKafkaMocks({
  topicName: "consumer-test-topic",
});

describe("KafkaConsumer subscribe config", () => {
  beforeEach(() => {
    consumerSubscribe.mockClear();
    createConsumerMock.mockClear();
  });

  it("should subscribe with fromBeginning=true by default", async () => {
    const topicName = KafkaTopicName.fromString("consumer-test-topic");
    const consumer = new KTKafkaConsumer({
      kafkaSettings: {
        brokerUrls: ["localhost:19092"],
        clientId: KafkaClientId.fromString("consumer-test-client-id"),
        connectionTimeout: 30_000,
        consumerGroupId: "consumer-test-group-default",
      },
      pureConfig: {},
      logger: pino(),
    });

    await consumer.subscribeTopic([topicName]);

    expect(consumerSubscribe).toHaveBeenCalledTimes(1);
    expect(consumerSubscribe).toHaveBeenCalledWith({
      topics: [topicName],
    });
  });

  it("should subscribe with fromBeginning=true when explicitly enabled", async () => {
    const topicName = KafkaTopicName.fromString("consumer-test-topic");
    const consumer = new KTKafkaConsumer({
      kafkaSettings: {
        brokerUrls: ["localhost:19092"],
        clientId: KafkaClientId.fromString("consumer-test-client-id"),
        connectionTimeout: 30_000,
        consumerGroupId: "consumer-test-group-from-beginning",
        subscribeFromBeginning: true,
      },
      pureConfig: {},
      logger: pino(),
    });

    await consumer.subscribeTopic([topicName]);

    expect(consumerSubscribe).toHaveBeenCalledTimes(1);
    expect(consumerSubscribe).toHaveBeenCalledWith({
      topics: [topicName],
    });
  });

  it("should reject custom partition assigner functions", () => {
    const customPartitionAssigner = (() => {
      return null;
    }) as KTPartitionAssigner;

    expect(() => new KTKafkaConsumer({
      kafkaSettings: {
        brokerUrls: ["localhost:19092"],
        clientId: KafkaClientId.fromString("consumer-test-client-id"),
        connectionTimeout: 30_000,
        consumerGroupId: "consumer-test-group-custom-assigner",
        partitionAssignerFn: customPartitionAssigner,
      },
      pureConfig: {},
      logger: pino(),
    })).toThrow("Custom partition assigners are not supported by the confluent runtime");
  });
});
