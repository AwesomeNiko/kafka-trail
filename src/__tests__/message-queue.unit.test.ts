import { beforeEach, describe, expect, it, jest } from "@jest/globals";

import { KTHandler } from "../kafka/consumer-handler.ts";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.ts";
import { KTKafkaProducer } from "../kafka/kafka-producer.ts";
import { KTTopic } from "../kafka/topic.ts";
import { KafkaClientId, KafkaTopicName } from "../libs/branded-types/kafka/index.ts";
import { KTMessageQueue } from "../message-queue/index.ts";

import { createKafkaMocks } from "./mocks/create-mocks.ts";

// @ts-expect-error too much return arguments
const kafkaProducerInitMock = jest.spyOn(KTKafkaProducer.prototype, 'init').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaConsumerInitMock = jest.spyOn(KTKafkaConsumer.prototype, 'init').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaConsumerSubscribeTopicMock = jest.spyOn(KTKafkaConsumer.prototype, 'subscribeTopic').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerCreateTopicMock = jest.spyOn(KTKafkaProducer.prototype, 'createTopic').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerSendSingleMessageMock = jest.spyOn(KTKafkaProducer.prototype, 'sendSingleMessage').mockImplementation(jest.fn);

describe("KTMessageQueue test", () => {
  beforeEach(() => {
    kafkaProducerInitMock.mockClear();
    kafkaProducerCreateTopicMock.mockClear();
    kafkaProducerSendSingleMessageMock.mockClear();
    kafkaConsumerInitMock.mockClear();
    kafkaConsumerSubscribeTopicMock.mockClear();
  });

  it("should init for producer successfully", async () => {
    const mq = new KTMessageQueue();

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    expect(kafkaProducerInitMock).toHaveBeenCalledTimes(1);
  });

  it("should register handlers successfully", async () => {
    const TestExampleTopic = KTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
    })

    const testExampleTopicHandler = KTHandler({
      topic: TestExampleTopic,
      run: (payload) => {
        const data = payload[0]

        if (!data) {
          return Promise.resolve()
        }

        return Promise.resolve()
      },
    })

    const mq = new KTMessageQueue();

    const { clearAll } = createKafkaMocks({
      topicName: TestExampleTopic.topicSettings.topic,
    });

    mq.registerHandlers([testExampleTopicHandler]);

    await mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    })

    expect(kafkaConsumerInitMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledWith(['test.example']);
    clearAll()
  });
});
