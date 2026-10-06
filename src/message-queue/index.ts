import pino from "pino";
import type { Logger } from "pino";

import type { KTHandler } from "../kafka/consumer-handler.js";
import { KafkaBackend } from "../kafka/index.js";
import type { KafkaBrokerConfig, KafkaLogger } from "../kafka/kafka-broker.js";
import type { KTKafkaConsumerConfig } from "../kafka/kafka-consumer.js";
import type { KTTopicBatchPayload } from "../kafka/topic-batch.js";
import type { KTTopicEvent, KTTopicPayloadWithMeta } from "../kafka/topic.js";
import type { KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { KTTracing, type KTTracingSettings } from "../libs/helpers/tracing.js";

class KTMessageQueue<Ctx extends object> {
  #kafkaBackend: KafkaBackend<Ctx>;
  #ctx: Ctx & KafkaLogger;
  #tracing: KTTracing;

  constructor(params?: {
    ctx: () => Ctx & {
      logger?: Logger
    },
    tracingSettings?: KTTracingSettings
  }) {
    let ctx = params?.ctx()

    if (!ctx) {
      ctx = {} as Ctx & KafkaLogger
    }

    if (!ctx.logger) {
      ctx.logger = pino()
    }

    this.#ctx = ctx as Ctx & KafkaLogger
    this.#tracing = new KTTracing(params?.tracingSettings)
    this.#kafkaBackend = new KafkaBackend({
      ctx: this.#ctx,
      publisher: this,
      tracing: this.#tracing,
    })
  }

  getConsumer() {
    return this.#kafkaBackend.getConsumer();
  }

  getProducer() {
    return this.#kafkaBackend.getProducer();
  }

  getAdmin() {
    return this.#kafkaBackend.getAdmin();
  }

  initProducer(params: KafkaBrokerConfig) {
    return this.#kafkaBackend.initProducer(params);
  }

  initConsumer(params: KTKafkaConsumerConfig) {
    return this.#kafkaBackend.initConsumer(params);
  }

  async destroyAll() {
    await Promise.all([
      this.destroyProducer(),
      this.destroyConsumer(),
    ])
  }

  destroyProducer() {
    return this.#kafkaBackend.destroyProducer();
  }

  destroyConsumer() {
    return this.#kafkaBackend.destroyConsumer();
  }

  initTopics<T extends object>(topicEvents: KTTopicEvent<T>[]) {
    return this.#kafkaBackend.initTopics(topicEvents);
  }

  getRegisteredHandler(topic: KafkaTopicName) {
    return this.#kafkaBackend.getRegisteredHandler(topic);
  }

  registerHandlers<T extends object>(mqHandlers: KTHandler<T, Ctx & KafkaLogger>[]) {
    this.#kafkaBackend.registerHandlers(mqHandlers);
  }

  publishSingleMessage(topic: KTTopicPayloadWithMeta) {
    return this.#kafkaBackend.publishSingleMessage(topic);
  }

  publishBatchMessages(topic: KTTopicBatchPayload) {
    return this.#kafkaBackend.publishBatchMessages(topic);
  }
}

export { KTMessageQueue };
