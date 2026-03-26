import { EventEmitter } from "node:events";

import { jest } from "@jest/globals";

import * as kafkaBrokerModule from "../../kafka/kafka-broker.js";

const createKafkaMocks = ({
  topicName = "test-topic-name",
  partitions = 1,
  payloadToRun = [],
} = {}) => {
  const kafkaProducerConnectFn = jest.fn<(metadataOptions?: unknown, cb?: (err?: Error | null) => void) => void>()
    .mockImplementation((_metadataOptions, cb) => {
      cb?.(null)
    })
  const kafkaProducerDisconnectFn = jest.fn<(cb?: (err?: Error | null) => void) => void>()
    .mockImplementation((cb) => {
      cb?.(null)
    })
  const kafkaConsumerConnectFn = jest.fn<(metadataOptions?: unknown, cb?: (err?: Error | null) => void) => void>()
    .mockImplementation((_metadataOptions, cb) => {
      cb?.(null)
    })
  const kafkaConsumerDisconnectFn = jest.fn<(cb?: (err?: Error | null) => void) => void>()
    .mockImplementation((cb) => {
      cb?.(null)
    })

  const kafkaLowLevelAdminDisconnectFn = jest.fn<() => void>();
  const kafkaLowLevelCreatePartitionsFn = jest.fn<(topic: string, desiredPartitions: number, timeout?: number, cb?: (err?: Error | null) => void) => void>()
    .mockImplementation((_topic, _desiredPartitions, _timeout, cb) => {
      cb?.(null)
    });
  const kafkaLowLevelCreateTopicFn = jest.fn<(topic: {
    topic: string
    num_partitions: number
    replication_factor: number
    config: Record<string, string>
  }, timeout?: number, cb?: (err?: Error | null) => void) => void>()
    .mockImplementation((_topic, _timeout, cb) => {
      cb?.(null)
    })

  const getMetadataFn = jest.fn<(options: { topic: string, timeout: number }, cb: (err: Error | null, data: {
    orig_broker_id: number
    orig_broker_name: string
    brokers: unknown[]
    topics: Array<{
      name: string
      partitions: Array<{
        id: number
        leader: number
        replicas: number[]
        isrs: number[]
      }>
    }>
  }) => void) => void>()
    .mockImplementation(({ topic }, cb) => {
      if (topic !== topicName) {
        cb(null, {
          orig_broker_id: 0,
          orig_broker_name: "mock-broker",
          brokers: [],
          topics: [],
        })

        return
      }

      cb(null, {
        orig_broker_id: 0,
        orig_broker_name: "mock-broker",
        brokers: [],
        topics: [{
          name: topicName,
          partitions: Array.from({ length: partitions }).map((_, idx) => ({
            id: idx,
            leader: 0,
            replicas: [],
            isrs: [],
          })),
        }],
      })
    })

  const consumerSubscribe = jest.fn<(subscription: string[]) => void>();
  const consumerConsume = jest.fn<(batchSize: number, cb?: (err: Error | null, messages: Array<{
    topic: string
    partition: number
    offset: number
    key?: Buffer | string | null
    value: Buffer | null
  }>) => void) => void>()
    .mockImplementation((_batchSize, cb) => {
      const messages = payloadToRun.map((value, idx) => ({
        topic: topicName,
        partition: 0,
        offset: idx,
        key: null,
        value: Buffer.from(JSON.stringify(value)),
      }))

      cb?.(null, messages)
    })
  const consumerUnsubscribe = jest.fn<() => void>()
  const consumerSubscription = jest.fn<() => string[]>().mockReturnValue([])
  const consumerCommitSync = jest.fn<(offset: { topic: string, partition: number, offset: number }) => void>()
  const consumerCommitMessageSync = jest.fn<(message: { offset: number }) => void>()
  const consumerSetDefaultConsumeTimeout = jest.fn<(timeoutMs: number) => void>()

  const sendMsgFn = jest.fn<(params: {
    topic: string
    partition: null
    message: Buffer
    key: string | null
    timestamp: null
    opaque: unknown
    headers?: Array<Record<string, string | Buffer>>
  }) => void>()

  const producerEmitter = new EventEmitter()

  const producerMock = Object.assign(producerEmitter, {
    connect: kafkaProducerConnectFn,
    disconnect: kafkaProducerDisconnectFn,
    setPollInterval: jest.fn<(interval: number) => void>(),
    getMetadata: getMetadataFn,
    produce: jest.fn<(topic: string, partition: null, message: Buffer, key?: string | null, timestamp?: null, opaque?: unknown, headers?: Array<Record<string, string | Buffer>>) => void>()
      .mockImplementation((topic, partition, message, key, timestamp, opaque, headers) => {
        sendMsgFn({
          topic,
          partition,
          message,
          key: key ?? null,
          timestamp: timestamp ?? null,
          opaque,
          ...(headers ? { headers } : {}),
        })

        producerEmitter.emit("delivery-report", null, {
          topic,
          partition: 0,
          offset: 0,
          opaque,
          size: message.byteLength,
        })
      }),
  })

  const consumerMock = {
    connect: kafkaConsumerConnectFn,
    disconnect: kafkaConsumerDisconnectFn,
    subscribe: consumerSubscribe,
    consume: consumerConsume,
    unsubscribe: consumerUnsubscribe,
    subscription: consumerSubscription,
    commitSync: consumerCommitSync,
    commitMessageSync: consumerCommitMessageSync,
    setDefaultConsumeTimeout: consumerSetDefaultConsumeTimeout,
  };

  const lowLevelAdminMock = {
    disconnect: kafkaLowLevelAdminDisconnectFn,
    createPartitions: kafkaLowLevelCreatePartitionsFn,
    createTopic: kafkaLowLevelCreateTopicFn,
  }

  const producerFactorySpy = jest.spyOn(kafkaBrokerModule.rdKafkaFactories, "createProducer").mockImplementation(() => producerMock as never);
  const consumerFactorySpy = jest.spyOn(kafkaBrokerModule.rdKafkaFactories, "createConsumer").mockImplementation(() => consumerMock as never);
  const adminFactorySpy = jest.spyOn(kafkaBrokerModule.rdKafkaFactories, "createAdminClient").mockImplementation(() => lowLevelAdminMock as never);

  const clearAll = () => {
    producerFactorySpy.mockClear()
    consumerFactorySpy.mockClear()
    adminFactorySpy.mockClear()
  }

  return {
    kafkaProducerConnectFn,
    kafkaConsumerConnectFn,
    kafkaLowLevelAdminDisconnectFn,
    kafkaLowLevelCreatePartitionsFn,
    kafkaLowLevelCreateTopicFn,
    getMetadataFn,
    consumerSubscribe,
    consumerConsume,
    consumerCommitSync,
    consumerCommitMessageSync,
    sendMsgFn,
    producerFactorySpy,
    consumerFactorySpy,
    adminFactorySpy,
    clearAll,
  };
};

export { createKafkaMocks };
