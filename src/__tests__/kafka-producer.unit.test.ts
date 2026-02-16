import { describe, expect, it, beforeEach } from "@jest/globals";
import { CompressionTypes } from "kafkajs";
import { pino } from "pino";

import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

const {
  kafkaAdminConnectFn,
  kafkaProducerConnectFn,
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
