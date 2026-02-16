import { clearInterval } from "node:timers";

import { context, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { pino } from "pino";

import { ArgumentIsRequired, NoHandlersError, ProducerInitRequiredForDLQError, ProducerNotInitializedError } from "../custom-errors/kafka-errors.js";
import type { KTHandler } from "../kafka/consumer-handler.js";
import type { KafkaBrokerConfig, KafkaLogger } from "../kafka/kafka-broker.js";
import type { KTKafkaConsumerConfig } from "../kafka/kafka-consumer.js";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import type { KTTopicBatchPayload } from "../kafka/topic-batch.js";
import { DLQKTTopic, type KTTopicEvent, type KTTopicPayloadWithMeta } from "../kafka/topic.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createHandlerTraceAttributes } from "../libs/helpers/observability.js";

type KTHandlerKafkaParams = {
  heartBeat: () => void,
  partition: number,
  lastOffset: string | undefined,
  resolveOffset?: (offset: string) => void,
}

type KTPublishToDlqParams<Ctx extends object> = {
  handler: KTHandler<object, Ctx & KafkaLogger>,
  originalTopic: KafkaTopicName,
  originalOffset: string | undefined,
  oritinalPartition: number,
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
  kafkaTopicParams: KTHandlerKafkaParams,
  failedKey: KafkaMessageKey,
}

class KTMessageQueue<Ctx extends object> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #registeredHandlers: Map<KafkaTopicName, KTHandler<any, Ctx & KafkaLogger>> = new Map();
  #ktProducer?: KTKafkaProducer;
  #ktConsumer?: KTKafkaConsumer;
  #logger: Console | pino.Logger = console;
  #ctx: Ctx & KafkaLogger
  // Trace settings
  #addPayloadToTrace: boolean = false;

  constructor(params?: {
    ctx: () => Ctx & {
      logger?: pino.Logger
    },
    tracingSettings?: {
      addPayloadToTrace: boolean
    }
  }) {
    let ctx = params?.ctx()

    if (!ctx) {
      ctx = {} as Ctx & KafkaLogger
    }

    if (!ctx.logger) {
      ctx.logger = pino()
    }

    this.#ctx = ctx as Ctx & KafkaLogger
    this.#addPayloadToTrace = params?.tracingSettings?.addPayloadToTrace ?? false
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
      this.#logger.error(err)

      return err.message
    }

    return ''
  }

  async #publishToDlq(params: KTPublishToDlqParams<Ctx>) {
    const Topic = DLQKTTopic(params.handler.topic.topicSettings)
    const Payload = Topic({
      originalOffset: params.originalOffset,
      originalTopic: params.originalTopic,
      oritinalPartition: params.oritinalPartition,
      key: params.key,
      value: params.value,
      errorMessage: params.errorMessage,
      failedAt: Date.now(),
    }, {
      messageKey: KafkaMessageKey.NULL,
      meta: {},
    })

    await this.publishSingleMessage(Payload)
  }

  async #runHandlerWithTracing(params: KTRunHandlerWithTracingParams<Ctx>) {
    const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

    const attributes = createHandlerTraceAttributes({
      topicName: params.topicName,
      partition: params.partition,
      lastOffset: params.lastOffset,
      batchedValues: params.batchedValues,
      opts: {
        addPayloadToTrace: this.#addPayloadToTrace,
      },
    })

    const handlerSpan = tracer.startSpan(`kafka-trail: handler ${params.topicName}`, {
      kind: SpanKind.CONSUMER,
      attributes,
    })

    await context.with(trace.setSpan(context.active(), handlerSpan), async () => {
      try {
        await params.handler.run(params.batchedValues, this.#ctx, this, params.kafkaTopicParams)
      } catch (err) {
        const errorMessage = this.#extractErrorMessage(err)

        if (params.handler.topic.topicSettings.createDLQ) {
          await this.#publishToDlq({
            handler: params.handler,
            originalOffset: params.lastOffset,
            originalTopic: params.topicName,
            oritinalPartition: params.partition,
            key: params.failedKey,
            value: params.batchedValues,
            errorMessage,
          })
        } else {
          throw err
        }
      } finally {
        handlerSpan.end()
      }
    })
  }

  async initProducer(params: KafkaBrokerConfig) {
    const { kafkaSettings: { brokerUrls } } = params

    if(!brokerUrls || !brokerUrls.length) { throw new ArgumentIsRequired('brokerUrls'); }

    this.#ktProducer  = new KTKafkaProducer({ ...params, logger: this.#ctx.logger });
    await this.#ktProducer.init();
  }

  async initConsumer(params: KTKafkaConsumerConfig) {
    const registeredHandlers = [...this.#registeredHandlers.values()]

    if (registeredHandlers.length === 0) {
      throw new NoHandlersError('subscribe to consumer');
    }

    const hasDlqHandlers = registeredHandlers.some((handler) => handler.topic.topicSettings.createDLQ)

    if (hasDlqHandlers) {
      throw new ProducerInitRequiredForDLQError();
    }

    this.#ktConsumer = new KTKafkaConsumer({ ...params, logger: this.#ctx.logger });
    await this.#ktConsumer.init();

    if (params.kafkaSettings.batchConsuming) {
      await this.#subscribeAll()
    } else {
      await this.#subscribeAllEachMessages()
    }
  }

  async destroyAll() {
    await Promise.all([
      this.destroyProducer(),
      this.destroyConsumer(),
    ])
  }

  async destroyProducer() {
    if (this.#ktProducer) {
      await this.#ktProducer.destroy();
    }
  }

  async destroyConsumer() {
    if (this.#ktConsumer) {
      await this.#ktConsumer.destroy();
    }
  }

  async #subscribeAllEachMessages(){
    const topicNames = [...this.#registeredHandlers.values()].map(item => item.topic.topicSettings.topic)
    const consumer = this.#requireConsumer();
    await consumer.subscribeTopic(topicNames)
    await consumer.consumer.run({
      partitionsConsumedConcurrently: 1,
      eachMessage: async (eachMessagePayload) => {
        const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

        const eachMessageSpan = tracer.startSpan(`kafka-trail: eachMessage`, {
          kind: SpanKind.CONSUMER,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination': topicNames,
          },
        })

        await context.with(trace.setSpan(context.active(), eachMessageSpan), async () => {
          try {
            const { topic, message, partition }  = eachMessagePayload

            const topicName = KafkaTopicName.fromString(topic)

            const handler = this.#registeredHandlers.get(topicName)

            if (handler) {
              const batchedValues: object[] = [];
              let lastOffset: string | undefined = undefined

              if (message.value) {
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
                kafkaTopicParams: {
                  partition,
                  lastOffset,
                  heartBeat: () => eachMessagePayload.heartbeat(),
                },
                failedKey: KafkaMessageKey.fromString(message.key?.toString()),
              })
            }
          } finally {
            eachMessageSpan.end()
          }
        })
      },
    })
  }

  async #subscribeAll() {
    const topicNames = [...this.#registeredHandlers.values()].map(item => item.topic.topicSettings.topic)
    const consumer = this.#requireConsumer();
    await consumer.subscribeTopic(topicNames)
    await consumer.consumer.run({
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 1,
      eachBatch: async (eachBatchPayload) => {
        const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

        const eachBatchSpan = tracer.startSpan(`kafka-trail: eachBatch`, {
          kind: SpanKind.CONSUMER,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination': topicNames,
          },
        })

        await context.with(trace.setSpan(context.active(), eachBatchSpan), async () => {
          try {
            const { batch: { topic, messages, partition } } = eachBatchPayload

            const topicName = KafkaTopicName.fromString(topic)

            const handler = this.#registeredHandlers.get(topicName)

            if (handler) {
              const heartbeatIntervalMs =
                consumer.heartBeatInterval - Math.floor(consumer.heartBeatInterval * consumer.heartbeatEarlyFactor)
              const heartBeatInterval = setInterval(() => {
                const heartbeatTracer = trace.getTracer(`kafka-trail`, '1.0.0')
                const heartbeatSpan = heartbeatTracer.startSpan(`kafka-trail: manual-heartbeat`, {
                  kind: SpanKind.CONSUMER,
                  attributes: {
                    'messaging.system': 'kafka',
                    'messaging.destination': topicNames,
                  },
                })

                void context.with(trace.setSpan(context.active(), heartbeatSpan), async () => {
                  try {
                    await eachBatchPayload.heartbeat()
                  } catch (err) {
                    this.#logger.error(err)
                  } finally {
                    heartbeatSpan.end()
                  }
                })
              }, heartbeatIntervalMs)

              try {
                const batchedValues: object[] = [];
                let lastOffset: string | undefined = undefined

                for (const message of messages) {
                  if (batchedValues.length < handler.topic.topicSettings.batchMessageSizeToConsume) {
                    if (message.value) {
                      // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
                      const decodedMessage: object = handler.topic.decode(message.value);
                      batchedValues.push(decodedMessage);
                      lastOffset = message.offset;
                    }
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
              } finally {
                clearInterval(heartBeatInterval)
              }
            }

            await eachBatchPayload.heartbeat()
          } finally {
            eachBatchSpan.end()
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
        this.#logger.warn(`Attempting to register an already registered handler ${handler.topic.topicSettings.topic}`);
      }
    }
  }

  publishSingleMessage(topic: KTTopicPayloadWithMeta) {
    const producer = this.#ktProducer;

    if (!producer) {
      return Promise.reject(new ProducerNotInitializedError());
    }

    const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

    const span = tracer.startSpan(`kafka-trail: publishSingleMessage ${topic.topicName}`, {
      kind: SpanKind.PRODUCER,
    })

    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const res = await producer.sendSingleMessage({
          topicName: topic.topicName,
          value: topic.message,
          messageKey: topic.messageKey,
          headers: topic.meta ?? {},
        });
        span.end()

        return res
      } catch (error) {
        span.recordException(error as Error)
        span.setStatus({ code: SpanStatusCode.ERROR, message: String(error) })
        span.end()
        throw error
      }
    })
  }

  publishBatchMessages(topic: KTTopicBatchPayload) {
    const producer = this.#ktProducer;

    if (!producer) {
      return Promise.reject(new ProducerNotInitializedError());
    }

    const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

    const span = tracer.startSpan(`kafka-trail: publishBatchMessages ${topic.topicName}`, {
      kind: SpanKind.PRODUCER,
      attributes: {
        messageSize: topic.messages.length,
      },
    })

    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const res = await producer.sendBatchMessages(topic);
        span.end()

        return res
      } catch (error) {
        span.recordException(error as Error)
        span.setStatus({ code: SpanStatusCode.ERROR, message: String(error) })
        span.end()
        throw error
      }
    })
  }
}

export { KTMessageQueue };
