import { jest } from "@jest/globals";
import type { Admin,
  AdminConfig, Consumer, ConsumerRunConfig,
  ITopicConfig,
  ITopicMetadata,
  ITopicPartitionConfig,
  Producer, ProducerRecord,
  RecordMetadata } from "kafkajs";
import KafkaJS from "kafkajs";

const createKafkaMocks = ({
  topicName = 'test-topic-name',
  partitions = 1,
  payloadToRun = [],
} = {}) => {
  const kafkaAdminConnectFn = jest.fn<() => Promise<void>>();
  const kafkaProducerConnectFn = jest.fn<() => Promise<void>>();
  const kafkaConsumerConnectFn = jest.fn<() => Promise<void>>();

  // @ts-expect-error too much return arguments
  const fetchTopicMetadataFn = jest.fn<( options: {topics: string[]}) => Promise<{topics: ITopicMetadata[]}>>().mockImplementation(({ topics }) => {

    if(!topics?.filter((argTopicName) => argTopicName === topicName).length) {
      throw new KafkaJS.KafkaJSProtocolError('This server does not host this topic-partition');
    }

    return Promise.resolve({
      topics: [{
        name: topicName,
        partitions: Array.from({ length: partitions }).map((_, idx) => ({ partitionId: idx })),
      }],
    })
  });
  const createPartitionsFn = jest.fn<(options: {validateOnly?: boolean, timeout?: number, topicPartitions: ITopicPartitionConfig[]}) => Promise<boolean>>();
  const createTopicsFn = jest.fn<(options: {validateOnly?: boolean, waitForLeaders?: boolean, timeout?: number, topics: ITopicConfig[]}) => Promise<boolean>>();
  const consumerSubscribe = jest.fn<() => Promise<void>>();
  const sendMsgFn = jest.fn<(record: ProducerRecord) => Promise<RecordMetadata[]>>();

  // @ts-expect-error too much return arguments
  const adminMockImpl: (config?: AdminConfig) => Admin = () => ({
    connect: kafkaAdminConnectFn,
    fetchTopicMetadata: fetchTopicMetadataFn,
    createPartitions: createPartitionsFn,
    createTopics: createTopicsFn,
  });

  // @ts-expect-error too much return arguments
  const kafkaProducerMockImpl: ()  => Producer = () => ({
    connect: kafkaProducerConnectFn,
    send: sendMsgFn,
  })

  // @ts-expect-error too much return arguments
  const kafkaConsumerMockImpl: ()  => Consumer = () => ({
    connect: kafkaConsumerConnectFn,
    subscribe: consumerSubscribe,
    async run(config?: ConsumerRunConfig): Promise<void> {
      if (!config) return Promise.resolve()
      const messages = payloadToRun.map((v, idx) => ({
        key: null,
        value: Buffer.from(JSON.stringify(v)),
        timestamp: '0',
        attributes: 0,
        offset: String(idx),
        headers: {},
      }))

      if (config.eachMessage) {
        for (const message of messages) {
          await config.eachMessage({
            topic: topicName,
            partition: 0,
            message,
            heartbeat(): Promise<void> {
              return Promise.resolve()
            },
            pause(): () => void {
              return () => undefined
            },
          });
        }

        return Promise.resolve()
      }

      if (!config.eachBatch) return Promise.resolve()

      await config.eachBatch({
        batch: {
          topic: topicName,
          partition: 0,
          highWatermark: '0',
          messages,
          isEmpty() {
            return messages.length === 0
          },
          firstOffset() {
            return messages[0]?.offset ?? null
          },
          lastOffset() {
            return messages[messages.length - 1]?.offset ?? '0'
          },
          offsetLag() {
            return '0'
          },
          offsetLagLow() {
            return '0'
          },
        },
        heartbeat(): Promise<void> {
          return Promise.resolve()
        },
        resolveOffset(): void {
          // noop for unit tests
        },
        pause(): () => void {
          return () => undefined
        },
        async commitOffsetsIfNecessary(): Promise<void> {
          return Promise.resolve()
        },
        uncommittedOffsets() {
          return { topics: [] }
        },
        isRunning() {
          return true
        },
        isStale() {
          return false
        },
      })

      return Promise.resolve()
    },
  })

  const kafkaAdminMock = jest
    .spyOn(KafkaJS.Kafka.prototype, "admin")
    .mockImplementation(adminMockImpl);

  const kafkaProducerMock = jest
    .spyOn(KafkaJS.Kafka.prototype, 'producer')
    .mockImplementation(kafkaProducerMockImpl);

  const kafkaConsumerMock = jest
    .spyOn(KafkaJS.Kafka.prototype, 'consumer')
    .mockImplementation(kafkaConsumerMockImpl);

  const clearAll = () => {
    kafkaAdminMock.mockClear()
    kafkaProducerMock.mockClear()
    kafkaConsumerMock.mockClear()
  }

  return {
    kafkaAdminConnectFn,
    kafkaProducerConnectFn,
    fetchTopicMetadataFn,
    createPartitionsFn,
    createTopicsFn,
    kafkaConsumerConnectFn,
    sendMsgFn,
    consumerSubscribe,
    kafkaAdminMock,
    kafkaProducerMock,
    kafkaConsumerMock,
    clearAll,
  };
};

export { createKafkaMocks };
