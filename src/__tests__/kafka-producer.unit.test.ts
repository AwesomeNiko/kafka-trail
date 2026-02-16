import { describe, expect, it, beforeEach } from "@jest/globals";
import { CompressionTypes } from "kafkajs";
import { pino } from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

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
    createTopicsFn.mockClear();
    sendMsgFn.mockClear();
    kafkaAdminMock.mockClear();
    kafkaProducerMock.mockClear();
  });

  it("should init successful", async () => {
    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000,
      },
      pureConfig: {},
      logger: pino(),
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
        connectionTimeout: 30_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    await kafkaProducer.init();

    await kafkaProducer.createTopic({
      topic: TOPIC_NAME,
      numPartitions: PARTITIONS,
      configEntries: [],
    });

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
        connectionTimeout: 30_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    await kafkaProducer.init();

    await kafkaProducer.createTopic({
      topic: TOPIC_NAME,
      numPartitions: PARTITIONS,
      configEntries: [],
    });

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
        connectionTimeout: 30_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    await kafkaProducer.init();

    await expect(kafkaProducer.createTopic({
      topic: TOPIC_NAME,
      numPartitions: PARTITIONS,
      configEntries: [],
    })).rejects.toBeInstanceOf(UnableDecreasePartitionsError);
  });
  it('should send single message', async () => {
    const TOPIC_NAME = KafkaTopicName.fromString('basic-topic-name');
    const MESSAGE_KEY = KafkaMessageKey.fromString('1');
    const MESSAGE = '1';

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ['localhost:19092'],
        clientId: KafkaClientId.fromString('producer-test-client-id'),
        connectionTimeout: 30_000,
      },
      pureConfig: {},
      logger: pino(),
    });

    await kafkaProducer.sendSingleMessage({
      topicName: TOPIC_NAME,
      messageKey: MESSAGE_KEY,
      value: MESSAGE,
      headers: {},
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

  it("should use configured compression codec for batch messages", async () => {
    const TOPIC_NAME = KafkaTopicName.fromString("basic-topic-name");
    const MESSAGE_KEY = KafkaMessageKey.fromString("1");

    const kafkaProducer = new KTKafkaProducer({
      kafkaSettings: {
        brokerUrls: ["localhost:19092"],
        clientId: KafkaClientId.fromString("producer-test-client-id"),
        connectionTimeout: 30_000,
        compressionCodec: {
          codecType: CompressionTypes.GZIP,
        },
      },
      pureConfig: {},
      logger: pino(),
    });

    await kafkaProducer.sendBatchMessages({
      topicName: TOPIC_NAME,
      messages: [{
        key: MESSAGE_KEY,
        value: "1",
        headers: {},
      }],
    });

    expect(sendMsgFn).toHaveBeenCalledTimes(1);
    expect(sendMsgFn).toHaveBeenCalledWith({
      topic: TOPIC_NAME,
      compression: CompressionTypes.GZIP,
      messages: [{
        key: MESSAGE_KEY?.toString(),
        value: "1",
        headers: {},
      }],
    });
  });
});
