import { describe, expect, it, beforeEach } from "@jest/globals";
import { pino } from "pino";

import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { KTCompressionTypes } from "../kafka/kafka-types.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

const {
  kafkaProducerConnectFn,
  sendMsgFn,
  producerFactorySpy,
  adminFactorySpy,
} = createKafkaMocks({
  topicName: 'basic-topic-name',
  partitions: 2,
});

describe("KafkaProducer test", () => {
  beforeEach(() => {
    kafkaProducerConnectFn.mockClear();
    sendMsgFn.mockClear();
    producerFactorySpy.mockClear();
    adminFactorySpy.mockClear();
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

    await kafkaProducer.init();
    await kafkaProducer.sendSingleMessage({
      topicName: TOPIC_NAME,
      messageKey: MESSAGE_KEY,
      value: MESSAGE,
      headers: {},
    });

    expect(sendMsgFn).toHaveBeenCalledTimes(1);
    expect(sendMsgFn).toHaveBeenCalledWith({
      topic: TOPIC_NAME,
      partition: null,
      message: Buffer.from("1"),
      key: MESSAGE_KEY,
      timestamp: null,
      opaque: expect.any(Object),
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
          codecType: KTCompressionTypes.GZIP,
        },
      },
      pureConfig: {},
      logger: pino(),
    });

    await kafkaProducer.init();
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
      partition: null,
      message: Buffer.from("1"),
      key: MESSAGE_KEY,
      timestamp: null,
      opaque: expect.any(Object),
    });
  });
});
