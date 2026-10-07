import { clearInterval } from "node:timers";

import { ArgumentIsRequired, NoHandlersError, ProducerInitRequiredForDLQError, ProducerNotInitializedError } from "../custom-errors/kafka-errors.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createHandlerTraceAttributes } from "../libs/helpers/observability.js";
import type { KTObservability } from "../libs/helpers/observability.js";
import type { KTTracing } from "../libs/helpers/tracing.js";

import type { KTHandler, KTHandlerPublisher } from "./consumer-handler.js";
import type { KafkaBrokerConfig, KafkaLogger } from "./kafka-broker.js";
import type { KTKafkaConsumerConfig } from "./kafka-consumer.js";
import { KTKafkaConsumer } from "./kafka-consumer.js";
import { KTKafkaProducer } from "./kafka-producer.js";
import type { KTTopicBatchPayload } from "./topic-batch.js";
import { DLQKTTopic, type KTTopicEvent, type KTTopicPayloadWithMeta } from "./topic.js";

type KTHandlerKafkaParams = {
  heartBeat: () => Promise<void>,
  partition: number,
  lastOffset: string | undefined,
  resolveOffset?: (offset: string) => void,
}

type KTPublishToDlqParams<Ctx extends object> = {
  handler: KTHandler<object, Ctx & KafkaLogger>,
  originalTopic: KafkaTopicName,
  originalOffset: string | undefined,
  originalPartition: number,
  key: KafkaMessageKey,
  value: object[],
  errorMessage: string,
}

type KTRunHandlerWithTracingParams<Ctx extends object> = {
  handler: KTHandler<object, Ctx & KafkaLogger>,
  topicName: KafkaTopicName,
  partition: number,
  lastOffset: string | undefined,
  batchedValues: object[],
  payloadContentLength: number,
  kafkaTopicParams: KTHandlerKafkaParams,
  failedKey: KafkaMessageKey,
}

class KafkaBackend<Ctx extends object> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #registeredHandlers: Map<KafkaTopicName, KTHandler<any, Ctx & KafkaLogger>> = new Map();
  #ktProducer: KTKafkaProducer | undefined;
  #ktConsumer: KTKafkaConsumer | undefined;
  #ctx: Ctx & KafkaLogger
  #publisher: KTHandlerPublisher
  #tracing: KTTracing
  #observability: KTObservability

  constructor(params: {
    ctx: Ctx & KafkaLogger,
    publisher: KTHandlerPublisher,
    tracing: KTTracing,
    observability: KTObservability,
  }) {
    this.#ctx = params.ctx
    this.#publisher = params.publisher
    this.#tracing = params.tracing
    this.#observability = params.observability
  }

  getConsumer(): KTKafkaConsumer | undefined {
    return this.#ktConsumer;
  }

  getProducer(): KTKafkaProducer | undefined {
    return this.#ktProducer;
  }

  getAdmin() {
    return this.#ktProducer?.getAdmin()
  }

  async checkConnection(): Promise<void> {
    if (this.#ktProducer) {
      await this.#ktProducer.getAdmin().describeCluster();
    } else if (this.#ktConsumer) {
      await this.#ktConsumer.checkConnection();
    } else {
      throw new Error("Kafka is not initialized");
    }
  }

  #requireConsumer(): KTKafkaConsumer {
    if (!this.#ktConsumer) {
      throw new Error("Consumer is not initialized");
    }

    return this.#ktConsumer;
  }

  #requireProducer(): KTKafkaProducer {
    if (!this.#ktProducer) {
      throw new ProducerNotInitializedError();
    }

    return this.#ktProducer;
  }

  #extractErrorMessage(err: unknown): string {
    if (err instanceof Error) {
      this.#ctx.logger.error(err)

      return err.message
    }

    return ''
  }

  async #publishToDlq(params: KTPublishToDlqParams<Ctx>) {
    const Topic = DLQKTTopic(params.handler.topic.topicSettings)
    const Payload = Topic({
      originalOffset: params.originalOffset,
      originalTopic: params.originalTopic,
      originalPartition: params.originalPartition,
      key: params.key,
      value: params.value,
      errorMessage: params.errorMessage,
      failedAt: Date.now(),
    }, {
      messageKey: KafkaMessageKey.NULL,
      meta: {},
    })

    await this.#publisher.publishSingleMessage(Payload)
  }

  async #runHandlerWithTracing(params: KTRunHandlerWithTracingParams<Ctx>) {
    const attributes = createHandlerTraceAttributes({
      topicName: params.topicName,
      partition: params.partition,
      lastOffset: params.lastOffset,
      batchedValues: params.batchedValues,
      payloadContentLength: params.payloadContentLength,
      opts: {
        addPayloadToTrace: this.#tracing.addPayloadToTrace,
      },
    })

    try {
      await this.#observability.withSpan({ system: "kafka", name: params.topicName, operation: "process", attributes }, async () => {
        await params.handler.run(params.batchedValues, this.#ctx, this.#publisher, params.kafkaTopicParams)
      })
    } catch (err) {
      const errorMessage = this.#extractErrorMessage(err)

      if (params.handler.topic.topicSettings.createDLQ) {
        await this.#publishToDlq({
          handler: params.handler,
          originalOffset: params.lastOffset,
          originalTopic: params.topicName,
          originalPartition: params.partition,
          key: params.failedKey,
          value: params.batchedValues,
          errorMessage,
        })
      } else {
        throw err
      }
    }
  }

  #getRawPayloadContentLength(value: Buffer | string | null | undefined): number {
    if (!value) {
      return 0
    }

    if (Buffer.isBuffer(value)) {
      return value.byteLength
    }

    return Buffer.byteLength(value, "utf8")
  }

  async initProducer(params: KafkaBrokerConfig) {
    const { kafkaSettings: { brokerUrls } } = params

    if(!brokerUrls || !brokerUrls.length) { throw new ArgumentIsRequired('brokerUrls'); }

    this.#ktProducer  = new KTKafkaProducer({ ...params, logger: this.#ctx.logger });
    await this.#ktProducer.init();
  }

  async initConsumer(params: KTKafkaConsumerConfig) {
    const {
      kafkaSettings: { brokerUrls, partitionsConsumedConcurrently = 1 },
    } = params;

    if (!brokerUrls || !brokerUrls.length) {
      throw new ArgumentIsRequired("brokerUrls");
    }

    const registeredHandlers = [...this.#registeredHandlers.values()]

    if (registeredHandlers.length === 0) {
      throw new NoHandlersError('subscribe to consumer');
    }

    const hasDlqHandlers = registeredHandlers.some((handler) => handler.topic.topicSettings.createDLQ)

    if (hasDlqHandlers && !this.#ktProducer) {
      throw new ProducerInitRequiredForDLQError();
    }

    this.#ktConsumer = new KTKafkaConsumer({ ...params, logger: this.#ctx.logger });
    await this.#ktConsumer.init();

    if (params.kafkaSettings.batchConsuming) {
      await this.#subscribeAll(partitionsConsumedConcurrently)
    } else {
      await this.#subscribeAllEachMessages(partitionsConsumedConcurrently)
    }
  }

  async destroyProducer() {
    if (this.#ktProducer) {
      await this.#ktProducer.destroy();
      this.#ktProducer = undefined;
    }
  }

  async destroyConsumer() {
    if (this.#ktConsumer) {
      await this.#ktConsumer.destroy();
      this.#ktConsumer = undefined;
    }
  }

  async #subscribeAllEachMessages(partitionsConsumedConcurrently: number){
    const topicNames = [...this.#registeredHandlers.values()].map(item => item.topic.topicSettings.topic)
    const consumer = this.#requireConsumer();
    await consumer.subscribeTopic(topicNames)
    await consumer.consumer.run({
      partitionsConsumedConcurrently,
      eachMessage: async (eachMessagePayload) => {
        await this.#tracing.withSpan(`kafka-trail: eachMessage`, {
          kind: this.#tracing.otel?.SpanKind.CONSUMER ?? 0,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination.name': eachMessagePayload.topic,
            'messaging.operation.name': 'receive',
          },
        }, async (eachMessageSpan) => {
          try {
            const { topic, message, partition }  = eachMessagePayload

            const topicName = KafkaTopicName.fromString(topic)

            const handler = this.#registeredHandlers.get(topicName)

            if (handler) {
              const batchedValues: object[] = [];
              let lastOffset: string | undefined = undefined
              let payloadContentLength = 0

              if (message.value) {
                payloadContentLength = this.#getRawPayloadContentLength(message.value)
                // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
                const decodedMessage: object = handler.topic.decode(message.value);
                batchedValues.push(decodedMessage);
                lastOffset = message.offset;
              }

              await this.#runHandlerWithTracing({
                handler,
                topicName,
                partition,
                lastOffset,
                batchedValues,
                payloadContentLength,
                kafkaTopicParams: {
                  partition,
                  lastOffset,
                  heartBeat: () => eachMessagePayload.heartbeat(),
                },
                failedKey: KafkaMessageKey.fromString(message.key?.toString()),
              })
              this.#observability.recordConsumed({ system: "kafka", name: topicName, count: 1, consumerGroup: consumer.consumerGroupId })
            }
          } finally {
            eachMessageSpan?.end()
          }
        })
      },
    })
  }

  async #subscribeAll(partitionsConsumedConcurrently: number) {
    const topicNames = [...this.#registeredHandlers.values()].map(item => item.topic.topicSettings.topic)
    const consumer = this.#requireConsumer();
    await consumer.subscribeTopic(topicNames)
    await consumer.consumer.run({
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently,
      eachBatch: async (eachBatchPayload) => {
        await this.#tracing.withSpan(`kafka-trail: eachBatch`, {
          kind: this.#tracing.otel?.SpanKind.CONSUMER ?? 0,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination.name': eachBatchPayload.batch.topic,
            'messaging.operation.name': 'receive',
          },
        }, async (eachBatchSpan) => {
          try {
            const { batch: { topic, messages, partition } } = eachBatchPayload

            const topicName = KafkaTopicName.fromString(topic)

            const handler = this.#registeredHandlers.get(topicName)

            if (handler) {
              const heartbeatIntervalMs =
                consumer.heartBeatInterval - Math.floor(consumer.heartBeatInterval * consumer.heartbeatEarlyFactor)
              const heartBeatInterval = setInterval(() => {
                void this.#tracing.withSpan(`kafka-trail: manual-heartbeat`, {
                  kind: this.#tracing.otel?.SpanKind.CONSUMER ?? 0,
                  attributes: {
                    'messaging.system': 'kafka',
                    'messaging.destination.name': topicName,
                    'messaging.operation.name': 'heartbeat',
                  },
                }, async (heartbeatSpan) => {
                  try {
                    await eachBatchPayload.heartbeat()
                  } catch (err) {
                    this.#ctx.logger.error(err)
                  } finally {
                    heartbeatSpan?.end()
                  }
                })
              }, heartbeatIntervalMs)

              try {
                const batchedValues: object[] = [];
                let messageCount = 0;
                let lastOffset: string | undefined = undefined
                let payloadContentLength = 0

                for (const message of messages) {
                  if (batchedValues.length < handler.topic.topicSettings.batchMessageSizeToConsume) {
                    if (message.value) {
                      payloadContentLength += this.#getRawPayloadContentLength(message.value)
                      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
                      const decodedMessage: object = handler.topic.decode(message.value);
                      batchedValues.push(decodedMessage);
                    }

                    lastOffset = message.offset;
                    messageCount++;
                  } else {
                    break;
                  }
                }

                await this.#runHandlerWithTracing({
                  handler,
                  topicName,
                  partition,
                  lastOffset,
                  batchedValues,
                  payloadContentLength,
                  kafkaTopicParams: {
                    partition,
                    lastOffset,
                    heartBeat: () => eachBatchPayload.heartbeat(),
                    resolveOffset: (offset: string) => eachBatchPayload.resolveOffset(offset),
                  },
                  failedKey: KafkaMessageKey.fromString(JSON.stringify(messages.map(m=>m.key?.toString()))),
                })

                if (lastOffset) {
                  eachBatchPayload.resolveOffset(lastOffset)
                }

                this.#observability.recordConsumed({ system: "kafka", name: topicName, count: messageCount, consumerGroup: consumer.consumerGroupId })
              } finally {
                clearInterval(heartBeatInterval)
              }
            }

            await eachBatchPayload.heartbeat()
          } finally {
            eachBatchSpan?.end()
          }
        })
      },
    })
  }

  async initTopics<T extends object>(topicEvents: KTTopicEvent<T>[]) {
    const producer = this.#requireProducer();

    for (const topicEvent of topicEvents) {
      if (!topicEvent) {
        throw new Error("Attemt to create topic that doesn't exists (null, instead of KTTopicEvent)")
      }

      await producer.createTopic(topicEvent.topicSettings);
    }
  }

  getRegisteredHandler(topic: KafkaTopicName) {
    return this.#registeredHandlers.get(topic)
  }

  registerHandlers<T extends object>(mqHandlers: KTHandler<T, Ctx & KafkaLogger>[]) {
    for (const handler of mqHandlers) {
      if (!this.#registeredHandlers.has(handler.topic.topicSettings.topic)) {
        this.#registeredHandlers.set(handler.topic.topicSettings.topic, handler);
      } else {
        this.#ctx.logger.warn(`Attempting to register an already registered handler ${handler.topic.topicSettings.topic}`);
      }
    }
  }

  publishSingleMessage(topic: KTTopicPayloadWithMeta) {
    const producer = this.#ktProducer;

    if (!producer) {
      return Promise.reject(new ProducerNotInitializedError());
    }

    return this.#observability.withSpan({
      system: "kafka",
      name: topic.topicName,
      operation: "publish",
      attributes: {
        "messaging.batch.message_count": 1,
        "messaging.message.body.size": Buffer.byteLength(topic.message),
        ...(topic.meta.traceId ? { "messaging.trace_id": topic.meta.traceId } : {}),
        ...(this.#tracing.addPayloadToTrace ? { "messaging.payload": topic.message } : {}),
      },
    }, () => producer.sendSingleMessage({
      topicName: topic.topicName,
      value: topic.message,
      messageKey: topic.messageKey,
      headers: topic.meta ?? {},
    }))
  }

  publishBatchMessages(topic: KTTopicBatchPayload) {
    const producer = this.#ktProducer;

    if (!producer) {
      return Promise.reject(new ProducerNotInitializedError());
    }

    return this.#observability.withSpan({
      system: "kafka",
      name: topic.topicName,
      operation: "publish",
      messageCount: topic.messages.length,
      attributes: {
        "messaging.batch.message_count": topic.messages.length,
        "messaging.message.body.size": topic.messages.reduce((total, message) => total + this.#getRawPayloadContentLength(message.value), 0),
      },
    }, () => producer.sendBatchMessages(topic))
  }
}

export { KafkaBackend };
