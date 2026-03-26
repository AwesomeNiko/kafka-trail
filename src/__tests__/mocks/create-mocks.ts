import ConfluentKafka from "@confluentinc/kafka-javascript";
import { jest } from "@jest/globals";

import type { KTConsumerRunConfig, KTTopicConfig, KTTopicMetadata } from "../../kafka/kafka-types.js";

const { KafkaJS, AdminClient } = ConfluentKafka

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
  const kafkaLowLevelAdminDisconnectFn = jest.fn<() => void>();
  const kafkaLowLevelCreatePartitionsFn = jest.fn<(topic: string, desiredPartitions: number, timeout?: number, cb?: (err?: Error) => void) => void>()
    .mockImplementation((_topic, _desiredPartitions, _timeout, cb) => {
      cb?.()
    });

  const fetchTopicMetadataFn = jest.fn<(options: { topics: string[] }) => Promise<{ topics: KTTopicMetadata[] }>>().mockImplementation(({ topics }) => {
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
  const createTopicsFn = jest.fn<(options: { topics: KTTopicConfig[], waitForLeaders?: boolean }) => Promise<boolean>>();
  const consumerSubscribe = jest.fn<(subscription: { topics: string[] }) => Promise<void>>();
  const sendMsgFn = jest.fn<(record: { topic: string, messages: Array<{ key: string | null, value: string, headers?: Record<string, unknown> }> }) => Promise<unknown>>();

  const consumerRun = jest.fn<(config: KTConsumerRunConfig) => Promise<void>>().mockImplementation(async (config) => {
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

  const adminMock = {
    connect: kafkaAdminConnectFn,
    disconnect: kafkaAdminDisconnectFn,
    fetchTopicMetadata: fetchTopicMetadataFn,
    createPartitions: createPartitionsFn,
    createTopics: createTopicsFn,
  };

  const producerMock = {
    connect: kafkaProducerConnectFn,
    disconnect: kafkaProducerDisconnectFn,
    dependentAdmin: () => adminMock,
    _getInternalClient: () => ({}),
    send: sendMsgFn,
  };

  const consumerMock = {
    connect: kafkaConsumerConnectFn,
    disconnect: kafkaConsumerDisconnectFn,
    stop: kafkaConsumerStopFn,
    subscribe: consumerSubscribe,
    run: consumerRun,
  };

  const createAdminMock = jest.fn(() => adminMock);
  const createProducerMock = jest.fn(() => producerMock);
  const createConsumerMock = jest.fn(() => consumerMock);

  const lowLevelAdminMock = {
    disconnect: kafkaLowLevelAdminDisconnectFn,
    createPartitions: kafkaLowLevelCreatePartitionsFn,
  }

  const kafkaAdminSpy = jest.spyOn(KafkaJS.Kafka.prototype, "admin").mockImplementation(() => adminMock as never);
  const kafkaProducerSpy = jest.spyOn(KafkaJS.Kafka.prototype, "producer").mockImplementation(() => producerMock as never);
  const kafkaConsumerSpy = jest.spyOn(KafkaJS.Kafka.prototype, "consumer").mockImplementation(() => consumerMock as never);
  const lowLevelAdminSpy = jest.spyOn(AdminClient, "create").mockImplementation(() => lowLevelAdminMock as never);
  const lowLevelAdminFromProducerSpy = jest.spyOn(AdminClient, "createFrom").mockImplementation(() => lowLevelAdminMock as never);

  const clearAll = () => {
    createAdminMock.mockClear()
    createProducerMock.mockClear()
    createConsumerMock.mockClear()
    kafkaAdminSpy.mockClear()
    kafkaProducerSpy.mockClear()
    kafkaConsumerSpy.mockClear()
    lowLevelAdminSpy.mockClear()
    lowLevelAdminFromProducerSpy.mockClear()
  }

  return {
    kafkaAdminConnectFn,
    kafkaProducerConnectFn,
    kafkaConsumerConnectFn,
    kafkaConsumerStopFn,
    kafkaLowLevelAdminDisconnectFn,
    kafkaLowLevelCreatePartitionsFn,
    fetchTopicMetadataFn,
    createPartitionsFn,
    createTopicsFn,
    consumerSubscribe,
    consumerRun,
    sendMsgFn,
    createAdminMock,
    createProducerMock,
    createConsumerMock,
    clearAll,
  };
};

export { createKafkaMocks };
