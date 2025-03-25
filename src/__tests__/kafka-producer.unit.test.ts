import { describe, expect, it, beforeEach } from "@jest/globals";
import { createKafkaMocks } from "./mocks/create-mocks.ts";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.ts";
import { KTKafkaProducer } from "../kafka/kafka-producer.ts";
import { CompressionTypes } from "kafkajs";
import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.ts";

const {
  kafkaAdminConnectFn,
  kafkaProducerConnectFn,
  fetchTopicMetadataFn,
  createPartitionsFn,
  createTopicsFn,
  sendMsgFn,
  kafkaAdminMock,
  kafkaProducerMock,
} = createKafkaMocks({
  topicName: 'basic-topic-name',
  partitions: 2,
});

describe("KafkaProducer test", () => {
  beforeEach(() => {
    kafkaAdminConnectFn.mockClear();
    kafkaProducerConnectFn.mockClear();
    fetchTopicMetadataFn.mockClear();
    createPartitionsFn.mockClear();
    createTopicsFn.mockClear(),
    sendMsgFn.mockClear(),
    kafkaAdminMock.mockClear();
    kafkaProducerMock.mockClear();
  });

  it("should init successful", async () => {
    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000
      },
      logger: console
    });

    await kafkaProducer.init();

    expect(kafkaAdminConnectFn).toHaveBeenCalledTimes(1);
    expect(kafkaProducerConnectFn).toHaveBeenCalledTimes(1);
  });

  it("should create topic", async () => {
    const TOPIC_NAME = KafkaTopicName.fromString('test-topic');
    const PARTITIONS = 2;

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000
      },
      logger: console
    });

    await kafkaProducer.init();

    await kafkaProducer.createTopic(TOPIC_NAME, PARTITIONS);

    expect(fetchTopicMetadataFn).toHaveBeenCalledTimes(1);
    expect(fetchTopicMetadataFn).toHaveBeenCalledWith({ topics: [TOPIC_NAME] });
    expect(createTopicsFn).toHaveBeenCalledTimes(1);
    expect(createTopicsFn).toHaveBeenCalledWith({
      topics: [{
        topic: TOPIC_NAME,
        numPartitions: PARTITIONS,
        configEntries: [],
      }],
      waitForLeaders: true,
    });
  });
  it("should scale existed topic partitions", async () => {
    const TOPIC_NAME = KafkaTopicName.fromString('basic-topic-name');
    const PARTITIONS = 4;

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000
      },
      logger: console
    });

    await kafkaProducer.init();

    await kafkaProducer.createTopic(TOPIC_NAME, PARTITIONS);

    expect(fetchTopicMetadataFn).toHaveBeenCalledTimes(1);
    expect(fetchTopicMetadataFn).toHaveBeenCalledWith({ topics: [TOPIC_NAME] });
    expect(createPartitionsFn).toHaveBeenCalledTimes(1);
    expect(createPartitionsFn).toHaveBeenCalledWith({
      topicPartitions: [
        {
          topic: TOPIC_NAME,
          count: PARTITIONS,
        },
      ],
    });
  });
  it("shouldn't scale down existed topic partitions", async () => {
    const TOPIC_NAME = KafkaTopicName.fromString('basic-topic-name');
    const PARTITIONS = 1;

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000
      },
      logger: console
    });

    await kafkaProducer.init();

    await expect(kafkaProducer.createTopic(TOPIC_NAME, PARTITIONS)).rejects.toBeInstanceOf(UnableDecreasePartitionsError);
  });
  it('should send single message', async () => {
    const TOPIC_NAME = KafkaTopicName.fromString('basic-topic-name');
    const MESSAGE_KEY = KafkaMessageKey.fromString('1');
    const MESSAGE = '1';

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000
      },
      logger: console
    });

    await kafkaProducer.sendSingleMessage({
      topicName: TOPIC_NAME,
      messageKey: MESSAGE_KEY,
      message: MESSAGE,
    });

    expect(sendMsgFn).toHaveBeenCalledTimes(1);
    expect(sendMsgFn).toHaveBeenCalledWith({
      topic: TOPIC_NAME,
      compression: CompressionTypes.LZ4,
      messages: [{
        key: MESSAGE_KEY?.toString(),
        value: '1',
        headers: {},
      }],
    });
  });
});