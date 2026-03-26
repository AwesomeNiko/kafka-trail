import { jest } from "@jest/globals";

import { resetKafkaRuntimeFactoryForTests, setKafkaRuntimeFactoryForTests } from "../../kafka/runtime/runtime-factory.js";
import type { KTRuntimeAdmin, KTRuntimeConsumer, KTRuntimeProducer, KTRuntimeTopicConfig, KTRuntimeTopicMetadata, KTRuntimeConsumerRunConfig } from "../../kafka/runtime/transport-types.js";

const createKafkaMocks = ({
  topicName = "test-topic-name",
  partitions = 1,
  payloadToRun = [],
} = {}) => {
  const kafkaAdminConnectFn = jest.fn<() => Promise<void>>();
  const kafkaAdminDisconnectFn = jest.fn<() => Promise<void>>();
  const kafkaProducerConnectFn = jest.fn<() => Promise<void>>();
  const kafkaProducerDisconnectFn = jest.fn<() => Promise<void>>();
  const kafkaConsumerConnectFn = jest.fn<() => Promise<void>>();
  const kafkaConsumerDisconnectFn = jest.fn<() => Promise<void>>();
  const kafkaConsumerStopFn = jest.fn<() => Promise<void>>();

  const fetchTopicMetadataFn = jest.fn<(options: { topics: string[] }) => Promise<{ topics: KTRuntimeTopicMetadata[] }>>().mockImplementation(({ topics }) => {
    if (!topics.includes(topicName)) {
      return Promise.reject(new Error("Topic not found"))
    }

    return Promise.resolve({
      topics: [{
        name: topicName,
        partitions: Array.from({ length: partitions }).map((_, idx) => ({ partitionId: idx })),
      }],
    })
  });
  const createPartitionsFn = jest.fn<(options: { topicPartitions: Array<{ topic: string, count: number }> }) => Promise<boolean>>();
  const createTopicsFn = jest.fn<(options: { topics: KTRuntimeTopicConfig[], waitForLeaders?: boolean }) => Promise<boolean>>();
  const consumerSubscribe = jest.fn<(subscription: { topics: string[], fromBeginning: boolean }) => Promise<void>>();
  const sendMsgFn = jest.fn<(record: { topic: string, compression: unknown, messages: Array<{ key: string | null, value: string, headers?: Record<string, unknown> }> }) => Promise<unknown>>();

  const consumerRun = jest.fn<(config: KTRuntimeConsumerRunConfig) => Promise<void>>().mockImplementation(async (config) => {
    const messages = payloadToRun.map((v, idx) => ({
      key: null,
      value: Buffer.from(JSON.stringify(v)),
      offset: String(idx),
    }))

    if (config.mode === "eachMessage") {
      for (const message of messages) {
        await config.eachMessage({
          topic: topicName,
          partition: 0,
          message,
          heartbeat() {
            return Promise.resolve()
          },
        });
      }

      return Promise.resolve()
    }

    await config.eachBatch({
      batch: {
        topic: topicName,
        partition: 0,
        messages,
      },
      heartbeat() {
        return Promise.resolve()
      },
      resolveOffset() {
        // noop for unit tests
      },
    })

    return Promise.resolve()
  });

  const adminMock: KTRuntimeAdmin = {
    connect: kafkaAdminConnectFn,
    disconnect: kafkaAdminDisconnectFn,
    fetchTopicMetadata: fetchTopicMetadataFn,
    createPartitions: createPartitionsFn,
    createTopics: createTopicsFn,
  };

  const producerMock: KTRuntimeProducer = {
    connect: kafkaProducerConnectFn,
    disconnect: kafkaProducerDisconnectFn,
    send: sendMsgFn,
  };

  const consumerMock: KTRuntimeConsumer = {
    connect: kafkaConsumerConnectFn,
    disconnect: kafkaConsumerDisconnectFn,
    stop: kafkaConsumerStopFn,
    subscribe: consumerSubscribe,
    run: consumerRun,
  };

  const createAdminMock = jest.fn<() => KTRuntimeAdmin>(() => adminMock);
  const createProducerMock = jest.fn<() => KTRuntimeProducer>(() => producerMock);
  const createConsumerMock = jest.fn<() => KTRuntimeConsumer>(() => consumerMock);

  const createKafkaRuntimeMock = jest.fn(() => ({
    createAdmin: createAdminMock,
    createProducer: createProducerMock,
    createConsumer: createConsumerMock,
  }));

  setKafkaRuntimeFactoryForTests(() => createKafkaRuntimeMock());

  const clearAll = () => {
    resetKafkaRuntimeFactoryForTests()
    createKafkaRuntimeMock.mockClear()
    createAdminMock.mockClear()
    createProducerMock.mockClear()
    createConsumerMock.mockClear()
  }

  return {
    kafkaAdminConnectFn,
    kafkaProducerConnectFn,
    kafkaConsumerConnectFn,
    kafkaConsumerStopFn,
    fetchTopicMetadataFn,
    createPartitionsFn,
    createTopicsFn,
    consumerSubscribe,
    consumerRun,
    sendMsgFn,
    createKafkaRuntimeMock,
    createAdminMock,
    createProducerMock,
    createConsumerMock,
    clearAll,
  };
};

export { createKafkaMocks };
