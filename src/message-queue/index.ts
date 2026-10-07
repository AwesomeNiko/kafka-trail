import type { Meter } from "@opentelemetry/api";
import pino from "pino";
import type { Logger } from "pino";

import type { KTJobHandler } from "../bullmq/consumer-handler.js";
import { BullMQBackend, type KTBullMQConsumerConfig, type KTBullMQProducerConfig, type KTBullMQShutdownOptions } from "../bullmq/index.js";
import type { KTJobPayload, KTJobScheduler } from "../bullmq/job.js";
import type { KTHandler } from "../kafka/consumer-handler.js";
import { KafkaBackend } from "../kafka/index.js";
import type { KafkaBrokerConfig } from "../kafka/kafka-broker.js";
import type { KTKafkaConsumerConfig } from "../kafka/kafka-consumer.js";
import type { KTTopicBatchPayload } from "../kafka/topic-batch.js";
import type { KTTopicEvent, KTTopicPayloadWithMeta } from "../kafka/topic.js";
import type { KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import type { KTLogger } from "../libs/helpers/logger.js";
import { KTTracing, type KTTracingSettings } from "../libs/helpers/tracing.js";

class KTMessageQueue<Ctx extends object> {
  #kafkaBackend: KafkaBackend<Ctx>;
  #bullMQBackend: BullMQBackend<Ctx>;
  #ctx: Ctx & KTLogger;
  #tracing: KTTracing;

  constructor(params?: {
    ctx: () => Ctx & {
      logger?: Logger
    },
    tracingSettings?: KTTracingSettings
    meter?: Meter
  }) {
    let ctx = params?.ctx()

    if (!ctx) {
      ctx = {} as Ctx & KTLogger
    }

    if (!ctx.logger) {
      ctx.logger = pino()
    }

    this.#ctx = ctx as Ctx & KTLogger
    this.#tracing = new KTTracing(params?.tracingSettings)
    this.#kafkaBackend = new KafkaBackend({
      ctx: this.#ctx,
      publisher: this,
      tracing: this.#tracing,
    })
    this.#bullMQBackend = new BullMQBackend({
      ctx: this.#ctx,
      publisher: this,
      tracing: this.#tracing,
      ...(params?.meter ? { meter: params.meter } : {}),
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

  checkKafkaConnection() {
    return this.#kafkaBackend.checkConnection();
  }

  async destroyAll(options?: KTBullMQShutdownOptions) {
    await Promise.all([
      this.destroyConsumer(),
      this.destroyBullMQConsumer(options),
    ])
    await Promise.all([
      this.destroyProducer(),
      this.destroyBullMQProducer(),
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

  registerHandlers<T extends object>(mqHandlers: KTHandler<T, Ctx & KTLogger>[]) {
    this.#kafkaBackend.registerHandlers(mqHandlers);
  }

  publishSingleMessage(topic: KTTopicPayloadWithMeta) {
    return this.#kafkaBackend.publishSingleMessage(topic);
  }

  publishBatchMessages(topic: KTTopicBatchPayload) {
    return this.#kafkaBackend.publishBatchMessages(topic);
  }

  initBullMQProducer(params: KTBullMQProducerConfig) {
    return this.#bullMQBackend.initProducer(params);
  }

  initBullMQConsumer(params: KTBullMQConsumerConfig) {
    return this.#bullMQBackend.initConsumer(params);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  registerJobHandlers(handlers: KTJobHandler<any, Ctx & KTLogger>[]) {
    this.#bullMQBackend.registerHandlers(handlers);
  }

  getRegisteredJobHandler(name: string) {
    return this.#bullMQBackend.getRegisteredHandler(name);
  }

  getBullMQQueue(name: string) {
    return this.#bullMQBackend.getQueue(name);
  }

  getBullMQWorker(name: string) {
    return this.#bullMQBackend.getWorker(name);
  }

  publishJob(job: KTJobPayload) {
    return this.#bullMQBackend.publishJob(job);
  }

  checkBullMQConnection() {
    return this.#bullMQBackend.checkConnection();
  }

  publishBatchJobs(jobs: KTJobPayload[]) {
    return this.#bullMQBackend.publishBatchJobs(jobs);
  }

  upsertJobScheduler(params: KTJobScheduler) {
    return this.#bullMQBackend.upsertJobScheduler(params);
  }

  removeJobScheduler(params: { jobName: string, schedulerId: string }) {
    return this.#bullMQBackend.removeJobScheduler(params);
  }

  destroyBullMQProducer() {
    return this.#bullMQBackend.destroyProducer();
  }

  destroyBullMQConsumer(options?: KTBullMQShutdownOptions) {
    return this.#bullMQBackend.destroyConsumer(options);
  }
}

export { KTMessageQueue };
