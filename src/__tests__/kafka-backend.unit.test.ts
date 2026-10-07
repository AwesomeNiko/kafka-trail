import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import * as otel from "@opentelemetry/api";
import type { Consumer, EachBatchPayload, EachMessagePayload } from "kafkajs";
import pino from "pino";

import { KTHandler, type KTRun } from "../kafka/consumer-handler.js";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import { CreateKTTopicBatch } from "../kafka/topic-batch.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { KTMessageQueue } from "../message-queue/index.js";

import { createKafkaMocks } from "./mocks/create-mocks.js";

type Payload = { value: number }
type BatchInput = Array<{ value: Payload, key: KafkaMessageKey }>
type TestContext = { serviceName: string, logger: pino.Logger }

const topicName = KafkaTopicName.fromString("test.kafka.backend");
const { kafkaConsumerMock, kafkaAdminMock, kafkaAdminDisconnectFn, describeClusterFn, describeGroupFn, sendMsgFn } = createKafkaMocks({ topicName });
const consumerRun = jest.fn<Consumer["run"]>().mockResolvedValue(undefined);
const createConsumer = kafkaConsumerMock.getMockImplementation();

if (!createConsumer) {
  throw new Error("Consumer mock is required");
}

kafkaConsumerMock.mockImplementation((config) => {
  const consumer = createConsumer(config);
  consumer.run = consumerRun;

  return consumer;
});

const context: TestContext = {
  serviceName: "test-service",
  logger: pino({ level: "silent" }),
};

const kafkaConfig = {
  kafkaSettings: {
    brokerUrls: ["localhost:19092"],
    clientId: KafkaClientId.fromString("test-backend-client"),
    connectionTimeout: 30_000,
    consumerGroupId: "test-backend-group",
  },
  pureConfig: {},
};

const createTopic = (createDLQ = false) => CreateKTTopic<Payload>({
  topic: topicName,
  numPartitions: 1,
  batchMessageSizeToConsume: 2,
  createDLQ,
}).BaseTopic;

const createEachMessagePayload = (): EachMessagePayload => ({
  topic: topicName,
  partition: 2,
  message: {
    key: Buffer.from("key-10"),
    value: Buffer.from(JSON.stringify({ value: 1 })),
    offset: "10",
    timestamp: "0",
    attributes: 0,
    headers: {},
  },
  heartbeat: jest.fn<EachMessagePayload["heartbeat"]>().mockResolvedValue(undefined),
  pause: () => () => undefined,
});

const createEachBatchPayload = () => {
  const message = createEachMessagePayload().message;

  return {
    batch: {
      topic: topicName,
      partition: 2,
      highWatermark: "13",
      messages: [
        message,
        { ...message, offset: "11", value: Buffer.from(JSON.stringify({ value: 2 })) },
        { ...message, offset: "12", value: Buffer.from(JSON.stringify({ value: 3 })) },
      ],
      isEmpty: () => false,
      firstOffset: () => "10",
      lastOffset: () => "12",
      offsetLag: () => "0",
      offsetLagLow: () => "0",
    },
    heartbeat: jest.fn<EachBatchPayload["heartbeat"]>().mockResolvedValue(undefined),
    resolveOffset: jest.fn<EachBatchPayload["resolveOffset"]>(),
    commitOffsetsIfNecessary: () => Promise.resolve(),
    uncommittedOffsets: () => ({ topics: [] }),
    isRunning: () => true,
    isStale: () => false,
    pause: () => () => undefined,
  };
};

const consume = async (batchConsuming: boolean, batchPayload = createEachBatchPayload()) => {
  const config = consumerRun.mock.calls[0]?.[0];

  if (batchConsuming) {
    if (!config?.eachBatch) {
      throw new Error("Batch callback is required");
    }

    await config.eachBatch(batchPayload);
  } else {
    if (!config?.eachMessage) {
      throw new Error("Message callback is required");
    }

    await config.eachMessage(createEachMessagePayload());
  }
};

describe("Kafka backend through KTMessageQueue", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("rejects a Kafka healthcheck before initialization and after shutdown", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    await expect(queue.checkKafkaConnection()).rejects.toThrow("Kafka is not initialized");
    expect(describeClusterFn).not.toHaveBeenCalled();
    expect(describeGroupFn).not.toHaveBeenCalled();

    await queue.initProducer(kafkaConfig);
    await queue.destroyProducer();

    await expect(queue.checkKafkaConnection()).rejects.toThrow("Kafka is not initialized");
    expect(describeClusterFn).not.toHaveBeenCalled();
    expect(describeGroupFn).not.toHaveBeenCalled();
  });

  it("checks Kafka through the existing producer admin and propagates broker errors", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    await queue.initProducer(kafkaConfig);
    await expect(queue.checkKafkaConnection()).resolves.toBeUndefined();
    expect(kafkaAdminMock).toHaveBeenCalledTimes(1);
    expect(describeClusterFn).toHaveBeenCalledTimes(1);

    const error = new Error("Kafka unavailable");
    describeClusterFn.mockRejectedValueOnce(error);
    await expect(queue.checkKafkaConnection()).rejects.toBe(error);
    expect(kafkaAdminMock).toHaveBeenCalledTimes(1);
    await queue.destroyProducer();
  });

  it("checks consumer-only Kafka through the existing consumer and propagates broker errors", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    queue.registerHandlers([KTHandler({ topic: createTopic(), run: () => Promise.resolve() })]);
    await queue.initConsumer(kafkaConfig);
    expect(kafkaAdminMock).not.toHaveBeenCalled();

    await Promise.all([queue.checkKafkaConnection(), queue.checkKafkaConnection()]);
    expect(describeGroupFn).toHaveBeenCalledTimes(2);
    expect(describeClusterFn).not.toHaveBeenCalled();
    expect(kafkaAdminMock).not.toHaveBeenCalled();

    const error = new Error("Kafka authentication failed");
    describeGroupFn.mockRejectedValueOnce(error);
    await expect(queue.checkKafkaConnection()).rejects.toBe(error);
    await queue.destroyConsumer();
    expect(kafkaAdminDisconnectFn).not.toHaveBeenCalled();
    await expect(queue.checkKafkaConnection()).rejects.toThrow("Kafka is not initialized");
    expect(describeGroupFn).toHaveBeenCalledTimes(3);
  });

  it("prefers the producer admin and uses the consumer only when the producer is absent", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    queue.registerHandlers([KTHandler({ topic: createTopic(), run: () => Promise.resolve() })]);
    await queue.initProducer(kafkaConfig);
    await queue.initConsumer(kafkaConfig);
    await queue.checkKafkaConnection();
    expect(describeClusterFn).toHaveBeenCalledTimes(1);
    expect(kafkaAdminMock).toHaveBeenCalledTimes(1);

    const error = new Error("Kafka unavailable");
    describeClusterFn.mockRejectedValueOnce(error);
    await expect(queue.checkKafkaConnection()).rejects.toBe(error);
    expect(describeClusterFn).toHaveBeenCalledTimes(2);
    expect(kafkaAdminMock).toHaveBeenCalledTimes(1);
    expect(describeGroupFn).not.toHaveBeenCalled();

    await queue.destroyProducer();
    await queue.checkKafkaConnection();
    expect(describeGroupFn).toHaveBeenCalledTimes(1);
    expect(describeClusterFn).toHaveBeenCalledTimes(2);
    expect(kafkaAdminMock).toHaveBeenCalledTimes(1);
    await queue.destroyAll();
    expect(kafkaAdminDisconnectFn).toHaveBeenCalledTimes(1);
  });

  it.each([
    { batchConsuming: false, concurrency: undefined, expected: 1 },
    { batchConsuming: true, concurrency: undefined, expected: 1 },
    { batchConsuming: false, concurrency: 5, expected: 5 },
    { batchConsuming: true, concurrency: 5, expected: 5 },
  ])("passes concurrency=$expected to KafkaJS (batch=$batchConsuming)", async ({ batchConsuming, concurrency, expected }) => {
    const queue = new KTMessageQueue({ ctx: () => context });
    queue.registerHandlers([KTHandler({ topic: createTopic(), run: () => Promise.resolve() })]);

    await queue.initConsumer({
      ...kafkaConfig,
      kafkaSettings: {
        ...kafkaConfig.kafkaSettings,
        batchConsuming,
        ...(concurrency === undefined ? {} : { partitionsConsumedConcurrently: concurrency }),
      },
    });

    expect(consumerRun).toHaveBeenCalledTimes(1);
    expect(consumerRun.mock.calls[0]?.[0]?.partitionsConsumedConcurrently).toBe(expected);
  });

  it("preserves the handler context and facade publisher", async () => {
    const topic = createTopic();
    const queue = new KTMessageQueue({ ctx: () => context });
    const run = jest.fn<KTRun<Payload, TestContext>>().mockImplementation(async (values, _ctx, publisher) => {
      const value = values[0];

      if (value) {
        await publisher.publishSingleMessage(topic(value, { messageKey: KafkaMessageKey.NULL, meta: {} }));
      }
    });
    const handler = KTHandler({ topic, run });

    queue.registerHandlers([handler]);
    expect(queue.getRegisteredHandler(topicName)).toBe(handler);
    await queue.initProducer(kafkaConfig);
    await queue.initConsumer(kafkaConfig);
    await consume(false);

    expect(run).toHaveBeenCalledWith([{ value: 1 }], context, queue, {
      partition: 2,
      lastOffset: "10",
      heartBeat: expect.any(Function),
    });
    expect(run.mock.calls[0]?.[1]).toBe(context);
    expect(run.mock.calls[0]?.[2]).toBe(queue);
    expect(sendMsgFn).toHaveBeenCalledWith(expect.objectContaining({
      topic: topicName,
      messages: [expect.objectContaining({ value: JSON.stringify({ value: 1 }) })],
    }));
  });

  it("preserves the batch limit, offset resolution and heartbeat", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    const run = jest.fn<KTRun<Payload, TestContext>>().mockResolvedValue(undefined);
    const batchPayload = createEachBatchPayload();

    queue.registerHandlers([KTHandler({ topic: createTopic(), run })]);
    await queue.initConsumer({ ...kafkaConfig, kafkaSettings: { ...kafkaConfig.kafkaSettings, batchConsuming: true } });
    await consume(true, batchPayload);

    expect(run).toHaveBeenCalledWith([{ value: 1 }, { value: 2 }], context, queue, {
      partition: 2,
      lastOffset: "11",
      heartBeat: expect.any(Function),
      resolveOffset: expect.any(Function),
    });
    expect(batchPayload.resolveOffset).toHaveBeenCalledWith("11");
    expect(batchPayload.heartbeat).toHaveBeenCalled();
  });

  it("infers decoded payloads for a batch topic handler", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    const topic = CreateKTTopicBatch<BatchInput>({
      topic: topicName,
      numPartitions: 1,
      batchMessageSizeToConsume: 2,
      createDLQ: false,
    }).BaseTopic;
    const handler = KTHandler({
      topic,
      run: async (values) => {
        const payloads: Payload[] = values;
        expect(payloads.map((payload) => payload.value)).toEqual([1, 2]);
        await Promise.resolve();
      },
    });
    const typedHandler: KTHandler<Payload, TestContext> = handler;
    const run = jest.spyOn(handler, "run");

    queue.registerHandlers([typedHandler]);
    await queue.initConsumer({ ...kafkaConfig, kafkaSettings: { ...kafkaConfig.kafkaSettings, batchConsuming: true } });
    await consume(true);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("preserves arrays as payloads for a regular topic handler", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    const topic = CreateKTTopic<BatchInput>(createTopic().topicSettings).BaseTopic;
    const arrayValue: BatchInput = [{ value: { value: 1 }, key: KafkaMessageKey.NULL }];
    const batchPayload = createEachBatchPayload();
    batchPayload.batch.messages = [{
      ...createEachMessagePayload().message,
      value: Buffer.from(JSON.stringify(arrayValue)),
    }];
    const handler = KTHandler({
      topic,
      run: async (values) => {
        const batches: BatchInput[] = values;
        expect(batches).toEqual([arrayValue]);
        await Promise.resolve();
      },
    });
    const run = jest.spyOn(handler, "run");

    queue.registerHandlers([handler]);
    await queue.initConsumer({ ...kafkaConfig, kafkaSettings: { ...kafkaConfig.kafkaSettings, batchConsuming: true } });
    await consume(true, batchPayload);
    expect(run).toHaveBeenCalledTimes(1);
  });

  it.each([
    { name: "tombstone-only batches", values: [null, null, null], expectedValues: [], expectedOffset: "12" },
    { name: "trailing tombstones", values: [1, null, null], expectedValues: [{ value: 1 }], expectedOffset: "12" },
    { name: "mixed batches", values: [null, 1, null, 2, 3], expectedValues: [{ value: 1 }, { value: 2 }], expectedOffset: "13" },
    { name: "tombstones beyond the batch limit", values: [1, 2, null, 3], expectedValues: [{ value: 1 }, { value: 2 }], expectedOffset: "11" },
  ])("resolves the last consumed offset for $name", async ({ values, expectedValues, expectedOffset }) => {
    const queue = new KTMessageQueue({ ctx: () => context });
    const run = jest.fn<KTRun<Payload, TestContext>>().mockResolvedValue(undefined);
    const batchPayload = createEachBatchPayload();
    batchPayload.batch.messages = values.map((value, index) => ({
      ...createEachMessagePayload().message,
      offset: String(10 + index),
      value: value === null ? null : Buffer.from(JSON.stringify({ value })),
    }));

    queue.registerHandlers([KTHandler({ topic: createTopic(), run })]);
    await queue.initConsumer({ ...kafkaConfig, kafkaSettings: { ...kafkaConfig.kafkaSettings, batchConsuming: true } });
    await consume(true, batchPayload);

    expect(run).toHaveBeenCalledTimes(1);
    expect(run).toHaveBeenCalledWith(expectedValues, context, queue, {
      partition: 2,
      lastOffset: expectedOffset,
      heartBeat: expect.any(Function),
      resolveOffset: expect.any(Function),
    });
    expect(batchPayload.resolveOffset).toHaveBeenCalledTimes(1);
    expect(batchPayload.resolveOffset).toHaveBeenCalledWith(expectedOffset);
  });

  it.each([false, true])("propagates handler errors without DLQ (batch=%s)", async (batchConsuming) => {
    const queue = new KTMessageQueue({ ctx: () => context });
    const error = new Error("handler failed");
    const run = jest.fn<KTRun<Payload, TestContext>>().mockRejectedValue(error);
    const batchPayload = createEachBatchPayload();

    queue.registerHandlers([KTHandler({ topic: createTopic(), run })]);
    await queue.initConsumer({ ...kafkaConfig, kafkaSettings: { ...kafkaConfig.kafkaSettings, batchConsuming } });

    await expect(consume(batchConsuming, batchPayload)).rejects.toBe(error);
    expect(batchPayload.resolveOffset).not.toHaveBeenCalled();
    expect(sendMsgFn).not.toHaveBeenCalled();
  });

  it.each([
    { batchConsuming: false, batchTopic: false },
    { batchConsuming: true, batchTopic: false },
    { batchConsuming: false, batchTopic: true },
    { batchConsuming: true, batchTopic: true },
  ])("publishes handler failures to typed DLQ (batch=$batchConsuming, batchTopic=$batchTopic)", async ({ batchConsuming, batchTopic }) => {
    const queue = new KTMessageQueue({ ctx: () => context });
    const run = jest.fn<KTRun<Payload, TestContext>>().mockRejectedValue(new Error("handler failed"));
    const batchPayload = createEachBatchPayload();
    const { BaseTopic, DLQTopic } = batchTopic
      ? CreateKTTopicBatch<BatchInput>(createTopic(true).topicSettings)
      : CreateKTTopic<Payload>(createTopic(true).topicSettings);

    if (!DLQTopic) {
      throw new Error("DLQ topic is required");
    }

    queue.registerHandlers([KTHandler({ topic: BaseTopic, run })]);
    await queue.initProducer(kafkaConfig);
    await queue.initConsumer({ ...kafkaConfig, kafkaSettings: { ...kafkaConfig.kafkaSettings, batchConsuming } });
    await consume(batchConsuming, batchPayload);

    const record = sendMsgFn.mock.calls[0]?.[0];
    expect(record?.topic).toBe(`dlq.${topicName}`);
    const value = record?.messages[0]?.value;

    if (typeof value !== "string") {
      throw new Error("DLQ payload is required");
    }

    expect(JSON.parse(value) as unknown).toEqual({
      originalTopic: topicName,
      originalPartition: 2,
      originalOffset: batchConsuming ? "11" : "10",
      key: batchConsuming ? JSON.stringify(["key-10", "key-10", "key-10"]) : "key-10",
      value: batchConsuming ? [{ value: 1 }, { value: 2 }] : [{ value: 1 }],
      errorMessage: "handler failed",
      failedAt: expect.any(Number),
    });
    const payloads: Payload[] = DLQTopic.decode(value).value;
    expect(payloads).toEqual(batchConsuming ? [{ value: 1 }, { value: 2 }] : [{ value: 1 }]);

    if (batchConsuming) {
      expect(batchPayload.resolveOffset).toHaveBeenCalledWith("11");
    }
  });

  it("preserves producer, consumer and admin access and shutdown", async () => {
    const queue = new KTMessageQueue({ ctx: () => context });
    expect(queue.getProducer()).toBeUndefined();
    expect(queue.getConsumer()).toBeUndefined();
    expect(queue.getAdmin()).toBeUndefined();
    await queue.destroyAll();
    queue.registerHandlers([KTHandler({ topic: createTopic(), run: () => Promise.resolve() })]);
    await queue.initProducer(kafkaConfig);
    await queue.initConsumer(kafkaConfig);

    expect(queue.getProducer()).toBeInstanceOf(KTKafkaProducer);
    expect(queue.getConsumer()).toBeInstanceOf(KTKafkaConsumer);
    expect(queue.getAdmin()).toBe(queue.getProducer()?.getAdmin());
    const producerDestroy = jest.spyOn(KTKafkaProducer.prototype, "destroy").mockResolvedValue([undefined, undefined]);
    const consumerDestroy = jest.spyOn(KTKafkaConsumer.prototype, "destroy").mockResolvedValue(undefined);

    try {
      await queue.destroyAll();
      expect(producerDestroy).toHaveBeenCalledTimes(1);
      expect(consumerDestroy).toHaveBeenCalledTimes(1);
    } finally {
      producerDestroy.mockRestore();
      consumerDestroy.mockRestore();
    }
  });

  it("preserves injected tracing and closes consumer and handler spans on failure", async () => {
    const tracer = otel.trace.getTracer("test");
    const span = tracer.startSpan("test");
    const end = jest.spyOn(span, "end");
    const startSpan = jest.spyOn(tracer, "startSpan").mockReturnValue(span);
    const getTracer = jest.spyOn(otel.trace, "getTracer").mockReturnValue(tracer);
    const queue = new KTMessageQueue({ ctx: () => context, tracingSettings: { otel, addPayloadToTrace: true } });
    const error = new Error("handler failed");

    queue.registerHandlers([KTHandler({ topic: createTopic(), run: () => Promise.reject(error) })]);

    try {
      await queue.initConsumer(kafkaConfig);
      await expect(consume(false)).rejects.toBe(error);
      expect(startSpan).toHaveBeenCalledWith(`kafka-trail: handler ${topicName}`, {
        kind: otel.SpanKind.CONSUMER,
        attributes: expect.objectContaining({ "messaging.kafka.payload": JSON.stringify([{ value: 1 }]) }),
      });
      expect(end).toHaveBeenCalledTimes(2);
    } finally {
      getTracer.mockRestore();
      startSpan.mockRestore();
      end.mockRestore();
    }
  });
});
