import { clearInterval } from "node:timers";

import { context, SpanKind, trace } from "@opentelemetry/api";
import { pino } from "pino";

import { ArgumentIsRequired, NoHandlersError } from "../custom-errors/kafka-errors.js";
import type { KTHandler } from "../kafka/consumer-handler.js";
import type { KafkaBrokerConfig, KafkaLogger } from "../kafka/kafka-broker.js";
import type { KTKafkaConsumerConfig } from "../kafka/kafka-consumer.js";
import { KTKafkaConsumer } from "../kafka/kafka-consumer.js";
import { KTKafkaProducer } from "../kafka/kafka-producer.js";
import type { KTTopicPayloadWithMeta, KTTopicEvent } from "../kafka/topic.js";
import { KafkaTopicName } from "../libs/branded-types/kafka/index.js";

class KTMessageQueue<Ctx extends object> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #registeredHandlers: Map<KafkaTopicName, KTHandler<any, Ctx & KafkaLogger>> = new Map();
  #ktProducer!: KTKafkaProducer;
  #ktConsumer!: KTKafkaConsumer;
  #logger: Console | pino.Logger = console;
  #ctx: Ctx & KafkaLogger

  constructor(params?: {ctx: () => Ctx & { logger?: pino.Logger }}) {
    let ctx = params?.ctx()

    if (!ctx) {
      ctx = {} as Ctx & KafkaLogger
    }

    if (!ctx.logger) {
      ctx.logger = pino()
    }

    this.#ctx = ctx as Ctx & KafkaLogger
  }

  getConsumer() {
    return this.#ktConsumer;
  }

  getProducer() {
    return this.#ktProducer;
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
    await this.#subscribeAll()
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

  async #subscribeAll() {
    const topicNames = [...this.#registeredHandlers.values()].map(item => item.topic.topicSettings.topic)
    await this.#ktConsumer.subscribeTopic(topicNames)
    await this.#ktConsumer.consumer.run({
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: 1,
      eachBatch: async (eachBatchPayload) => {
        const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

        const span = tracer.startSpan(`kafka-trail: eachBatch`, {
          kind: SpanKind.CONSUMER,
          attributes: {
            'messaging.system': 'kafka',
            'messaging.destination': topicNames,
          },
        })

        await context.with(trace.setSpan(context.active(), span), async () => {

          const { batch: { topic, messages, partition } } = eachBatchPayload

          const topicName = KafkaTopicName.fromString(topic)

          const handler = this.#registeredHandlers.get(topicName)

          if (handler) {

            const heartBeatInterval = setInterval(async () => {
              await eachBatchPayload.heartbeat()
            }, this.#ktConsumer.heartBeatInterval - Math.floor(this.#ktConsumer.heartBeatInterval * this.#ktConsumer.heartbeatEarlyFactor))

            const batchedValues = [];
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

            await handler.run(batchedValues, this.#ctx, this, {
              partition,
              lastOffset,
              heartBeat: () => eachBatchPayload.heartbeat(),
              resolveOffset: (offset: string) => eachBatchPayload.resolveOffset(offset),
            })

            clearInterval(heartBeatInterval)

            if (lastOffset) {
              eachBatchPayload.resolveOffset(lastOffset)
            }
          }

          await eachBatchPayload.heartbeat()
          span.end()
        })
      },
    })
  }

  async initTopics<T extends object>(topicEvents: KTTopicEvent<T>[]) {
    if (!this.#ktProducer) {
      throw new Error("Producer field is required");
    }

    for (const topicEvent of topicEvents) {
      await this.#ktProducer.createTopic(
        topicEvent.topicSettings.topic,
        topicEvent.topicSettings.numPartitions,
        topicEvent.topicSettings.configEntries,
      );
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
    return this.#ktProducer.sendSingleMessage({
      topicName: topic.topicName,
      message: topic.message,
      messageKey: topic.messageKey,
    }, topic.meta);
  }
}

export { KTMessageQueue };
