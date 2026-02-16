import { beforeEach, describe, expect, it, jest } from "@jest/globals";

import { ProducerInitRequiredForDLQError, ProducerNotInitializedError } from "../custom-errors/kafka-errors.js";
import { KTHandler } from "../kafka/consumer-handler.js";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { KTMessageQueue } from "../message-queue/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

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
// @ts-expect-error too much return arguments
const kafkaProducerSendBatchMessagesMock = jest.spyOn(KTKafkaProducer.prototype, 'sendBatchMessages').mockImplementation(jest.fn);

describe("KTMessageQueue test", () => {
  beforeEach(() => {
    kafkaProducerInitMock.mockClear();
    kafkaProducerCreateTopicMock.mockClear();
    kafkaProducerSendSingleMessageMock.mockClear();
    kafkaProducerSendBatchMessagesMock.mockClear();
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
    const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
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

  it("should throw clear error when publishSingleMessage is called before initProducer", async () => {
    const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.publish.single'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
    })
    const mq = new KTMessageQueue();
    const payload = TestExampleTopic({
      fieldForPayload: 1,
    }, {
      messageKey: KafkaMessageKey.fromString("key-1"),
      meta: {},
    })

    await expect(mq.publishSingleMessage(payload)).rejects.toBeInstanceOf(ProducerNotInitializedError);
    expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledTimes(0);
  });

  it("should throw clear error when publishBatchMessages is called before initProducer", async () => {
    const { BaseTopic: TestBatchTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.publish.batch'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
    })
    const mq = new KTMessageQueue();
    const batchPayload = {
      topicName: TestBatchTopic.topicSettings.topic,
      messages: [{
        key: KafkaMessageKey.fromString("key-2"),
        value: JSON.stringify({ fieldForPayload: 1 }),
        headers: {},
      }],
    }

    await expect(mq.publishBatchMessages(batchPayload)).rejects.toBeInstanceOf(ProducerNotInitializedError);
    expect(kafkaProducerSendBatchMessagesMock).toHaveBeenCalledTimes(0);
  });

  it("should throw clear error when initConsumer is called with DLQ-enabled handler", async () => {
    const { BaseTopic: DLQTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.consumer.dlq'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: true,
    })

    const mq = new KTMessageQueue();
    const dlqHandler = KTHandler({
      topic: DLQTopic,
      run: () => Promise.resolve(),
    })

    mq.registerHandlers([dlqHandler]);

    await expect(mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    })).rejects.toBeInstanceOf(ProducerInitRequiredForDLQError);

    expect(kafkaConsumerInitMock).toHaveBeenCalledTimes(0);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledTimes(0);
  });

  it("should allow initConsumer with DLQ-enabled handler when producer is initialized", async () => {
    const { BaseTopic: DLQTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.consumer.dlq.allowed'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: true,
    })

    const mq = new KTMessageQueue();
    const dlqHandler = KTHandler({
      topic: DLQTopic,
      run: () => Promise.resolve(),
    })

    mq.registerHandlers([dlqHandler]);

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    await mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    });

    expect(kafkaProducerInitMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerInitMock).toHaveBeenCalledTimes(1);
    expect(kafkaConsumerSubscribeTopicMock).toHaveBeenCalledTimes(1);
  });
});
