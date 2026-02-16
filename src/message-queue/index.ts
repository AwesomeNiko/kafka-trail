import { clearInterval } from "node:timers";

import { context, SpanKind, SpanStatusCode, trace } from "@opentelemetry/api";
import { pino } from "pino";

import { ArgumentIsRequired, NoHandlersError, ProducerNotInitializedError } from "../custom-errors/kafka-errors.js";
import type { KTHandler } from "../kafka/consumer-handler.js";
import type { KafkaBrokerConfig, KafkaLogger } from "../kafka/kafka-broker.js";
import type { KTKafkaConsumerConfig } from "../kafka/kafka-consumer.js";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import type { KTTopicBatchPayload } from "../kafka/topic-batch.ts";
import { DLQKTTopic, type KTTopicEvent, type KTTopicPayloadWithMeta } from "../kafka/topic.js";
import { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createHandlerTraceAttributes } from "../libs/helpers/observability.js";

class KTMessageQueue<Ctx extends object> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #registeredHandlers: Map<KafkaTopicName, KTHandler<any, Ctx & KafkaLogger>> = new Map();
  #ktProducer!: KTKafkaProducer;
  #ktConsumer!: KTKafkaConsumer;
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

  getConsumer() {
    return this.#ktConsumer;
  }

  getProducer() {
    return this.#ktProducer;
  }

  getAdmin() {
    return this.#ktProducer?.getAdmin()
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
    await this.#ktConsumer.subscribeTopic(topicNames)
    await this.#ktConsumer.consumer.run({
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

              const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

              const attributes = createHandlerTraceAttributes({
                topicName,
                partition,
                lastOffset,
                batchedValues,
                opts: {
                  addPayloadToTrace: this.#addPayloadToTrace,
                },
              })

              const handlerSpan = tracer.startSpan(`kafka-trail: handler ${topicName}`, {
                kind: SpanKind.CONSUMER,
                attributes,
              })

              await context.with(trace.setSpan(context.active(), handlerSpan), async () => {
                try {
                  await handler.run(batchedValues, this.#ctx, this, {
                    partition,
                    lastOffset,
                    heartBeat: () => eachMessagePayload.heartbeat(),
                  })
                } catch (err) {
                  let errorMessage: string = ''

                  if (err instanceof Error) {
                    errorMessage = err.message
                    this.#logger.error(err)
                  }

                  if (handler.topic.topicSettings.createDLQ) {
                    const Topic = DLQKTTopic(handler.topic.topicSettings)
                    const Payload = Topic({
                      originalOffset: lastOffset,
                      originalTopic: topicName,
                      oritinalPartition: partition,
                      key: KafkaMessageKey.fromString(message.key?.toString()),
                      value: batchedValues,
                      errorMessage,
                      failedAt: Date.now(),
                    }, {
                      messageKey: KafkaMessageKey.NULL,
                      meta: {},
                    })
                    await this.publishSingleMessage(Payload)
                  } else {
                    throw err
                  }
                } finally {
                  handlerSpan.end()
                }
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
    await this.#ktConsumer.subscribeTopic(topicNames)
    await this.#ktConsumer.consumer.run({
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
                this.#ktConsumer.heartBeatInterval - Math.floor(this.#ktConsumer.heartBeatInterval * this.#ktConsumer.heartbeatEarlyFactor)
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

                const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

                const attributes = createHandlerTraceAttributes({
                  topicName,
                  partition,
                  lastOffset,
                  batchedValues,
                  opts: {
                    addPayloadToTrace: this.#addPayloadToTrace,
                  },
                })

                const handlerSpan = tracer.startSpan(`kafka-trail: handler ${topicName}`, {
                  kind: SpanKind.CONSUMER,
                  attributes: attributes,
                })

                await context.with(trace.setSpan(context.active(), handlerSpan), async () => {
                  try {
                    await handler.run(batchedValues, this.#ctx, this, {
                      partition,
                      lastOffset,
                      heartBeat: () => eachBatchPayload.heartbeat(),
                      resolveOffset: (offset: string) => eachBatchPayload.resolveOffset(offset),
                    })
                  } catch (err) {
                    let errorMessage: string = ''

                    if (err instanceof Error) {
                      errorMessage = err.message
                      this.#logger.error(err)
                    }

                    if (handler.topic.topicSettings.createDLQ) {
                      const Topic = DLQKTTopic(handler.topic.topicSettings)
                      const Payload = Topic({
                        originalOffset: lastOffset,
                        originalTopic: topicName,
                        oritinalPartition: partition,
                        key: KafkaMessageKey.fromString(JSON.stringify(messages.map(m=>m.key?.toString()))),
                        value: batchedValues,
                        errorMessage,
                        failedAt: Date.now(),
                      }, {
                        messageKey: KafkaMessageKey.NULL,
                        meta: {},
                      })
                      await this.publishSingleMessage(Payload)
                    } else {
                      throw err
                    }
                  } finally {
                    handlerSpan.end()
                  }
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
    if (!this.#ktProducer) {
      throw new Error("Producer field is required");
    }

    for (const topicEvent of topicEvents) {
      if (!topicEvent) {
        throw new Error("Attemt to create topic that doesn't exists (null, instead of KTTopicEvent)")
      }

      await this.#ktProducer.createTopic(topicEvent.topicSettings);
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
    if (!this.#ktProducer) {
      return Promise.reject(new ProducerNotInitializedError());
    }

    const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

    const span = tracer.startSpan(`kafka-trail: publishSingleMessage ${topic.topicName}`, {
      kind: SpanKind.PRODUCER,
    })

    return context.with(trace.setSpan(context.active(), span), async () => {
      try {
        const res = await this.#ktProducer.sendSingleMessage({
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
    if (!this.#ktProducer) {
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
        const res = await this.#ktProducer.sendBatchMessages(topic);
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
