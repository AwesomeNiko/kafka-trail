import { beforeEach, describe, expect, it, jest } from "@jest/globals";

import { KTHandler } from "../kafka/consumer-handler.ts";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.ts";
import { KTKafkaProducer } from "../kafka/kafka-producer.ts";
import { CreateKTTopic } from "../kafka/topic.ts";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.ts";
import { KTMessageQueue } from "../message-queue/index.ts";

import { createKafkaMocks } from "./mocks/create-mocks.ts";

// @ts-expect-error too much return arguments
const kafkaProducerInitMock = jest.spyOn(KTKafkaProducer.prototype, 'init').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerCreateTopicMock = jest.spyOn(KTKafkaProducer.prototype, 'createTopic').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaProducerSendSingleMessageMock = jest.spyOn(KTKafkaProducer.prototype, 'sendSingleMessage').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaConsumerInitMock = jest.spyOn(KTKafkaConsumer.prototype, 'init').mockImplementation(jest.fn);
// @ts-expect-error too much return arguments
const kafkaConsumerSubscribeTopicMock = jest.spyOn(KTKafkaConsumer.prototype, 'subscribeTopic').mockImplementation(jest.fn);

describe("Consumer handlers test", () => {
  beforeEach(() => {
    kafkaProducerInitMock.mockClear();
    kafkaProducerCreateTopicMock.mockClear();
    kafkaProducerSendSingleMessageMock.mockClear();
    kafkaConsumerInitMock.mockClear();
    kafkaConsumerSubscribeTopicMock.mockClear();
  });

  it("should publishSingleMessage directly successful", async () => {
    const mq = new KTMessageQueue();

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.1'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
    })

    const testExamplePayload = TestExampleTopic({ fieldForPayload: 1 }, {
      messageKey: KafkaMessageKey.NULL,
      meta: {},
    })

    await mq.publishSingleMessage(testExamplePayload)

    expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledWith({
      topicName: TestExampleTopic.topicSettings.topic,
      value: JSON.stringify({ fieldForPayload: 1 }), //Because using default encoder
      messageKey: null,
      headers: {
        traceId: expect.any(String),
      },
    });
  });

  it("should consume data and publish from handler successful", async () => {
    const mq = new KTMessageQueue();

    const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.2'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
    })

    const testExampleTopicHandler = KTHandler({
      topic: TestExampleTopic,
      run: async (payload, _, publisher) => {
        const data = payload[0]

        if (!data) {
          return
        }

        const testExamplePayload = TestExampleTopic({ fieldForPayload: data.fieldForPayload + 1 }, {
          messageKey: KafkaMessageKey.fromString('testMessageKey'),
          meta: {},
        })

        await publisher.publishSingleMessage(testExamplePayload)
      },
    })

    const { clearAll } = createKafkaMocks({
      topicName: TestExampleTopic.topicSettings.topic,
      // @ts-expect-error custom payload
      payloadToRun: [{ fieldForPayload: 1 }],
    });

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    mq.registerHandlers([testExampleTopicHandler]);

    await mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-2"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    })

    expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledWith({
      topicName: TestExampleTopic.topicSettings.topic,
      value: JSON.stringify({ fieldForPayload: 2 }), //Because using default encoder
      messageKey: 'testMessageKey',
      headers: {
        traceId: expect.any(String),
      },
    });

    clearAll()
  });

  it("should use custom encoder and decoder properly", async () => {
    const mq = new KTMessageQueue();

    const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
      fieldForPayload: number
    }>({
      topic: KafkaTopicName.fromString('test.example.2'),
      numPartitions: 1,
      batchMessageSizeToConsume: 10,
      createDLQ: false,
    }, {
      encode: (data) => {
        return JSON.stringify({
          fieldForPayload: data.fieldForPayload + 100,
        })
      },
      decode: (data:string | Buffer) => {
        if (Buffer.isBuffer(data)) {
          data = data.toString()
        }

        const payload = JSON.parse(data) as {
          fieldForPayload: number
        }

        return {
          fieldForPayload: payload.fieldForPayload + 100,
        }
      },
    })

    const testExampleTopicHandler = KTHandler({
      topic: TestExampleTopic,
      run: async (payload, _, publisher) => {
        const data = payload[0]

        if (!data) {
          return
        }

        const testExamplePayload = TestExampleTopic({ fieldForPayload: data.fieldForPayload }, {
          messageKey: KafkaMessageKey.fromString('testMessageKey'),
          meta: {},
        })

        await publisher.publishSingleMessage(testExamplePayload)
      },
    })

    const { clearAll } = createKafkaMocks({
      topicName: TestExampleTopic.topicSettings.topic,
      // @ts-expect-error custom payload
      payloadToRun: [{ fieldForPayload: 1 }],
    });

    await mq.initProducer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-1"),
        connectionTimeout: 30000,
      },
      pureConfig: {},
    });

    mq.registerHandlers([testExampleTopicHandler]);

    await mq.initConsumer({
      kafkaSettings: {
        brokerUrls: ['localhost'],
        clientId: KafkaClientId.fromString("broker-client-id-2"),
        connectionTimeout: 30000,
        consumerGroupId: 'group - ' + new Date().toString(),
      },
      pureConfig: {},
    })

    expect(kafkaProducerSendSingleMessageMock).toHaveBeenCalledWith({
      topicName: TestExampleTopic.topicSettings.topic,
      value: JSON.stringify({ fieldForPayload: 201 }), //Because using default encoder
      messageKey: 'testMessageKey',
      headers: {
        traceId: expect.any(String),
      },
    });

    clearAll()
  });
});
