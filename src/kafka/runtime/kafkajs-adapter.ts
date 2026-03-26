import KafkaJS, { type CompressionTypes as KafkaCompressionTypes, type ICustomPartitioner, type IHeaders, type KafkaConfig } from "kafkajs";

import { lz4Codec } from "../../codec/lz4-codec.js";
import type { KafkaClientId } from "../../libs/branded-types/kafka/index.js";

import type { KTRuntimeAdmin, KTRuntimeClient, KTRuntimeConsumer, KTRuntimeConsumerRunConfig, KTRuntimePartitionAssigner, KTRuntimeProducer } from "./transport-types.js";

type KTKafkaSettings = {
  brokerUrls: string[],
  clientId: KafkaClientId,
  connectionTimeout: number
  compressionCodec?: {
    codecType: KafkaCompressionTypes.LZ4,
    codecFn?: lz4Codec
  } | {
    codecType: Exclude<KafkaCompressionTypes, KafkaCompressionTypes.LZ4>,
    codecFn?: never
  }
}

export type KTKafkaRuntimeConfig = {
  kafkaSettings: KTKafkaSettings
  pureConfig: Pick<KafkaConfig, "ssl" | "sasl" | "authenticationTimeout" | "reauthenticationThreshold" | "requestTimeout" | "enforceRequestTimeout" | "retry" | "socketFactory" | "logLevel" | "logCreator">
}

const { Kafka, CompressionTypes, CompressionCodecs } = KafkaJS

let registeredLz4CodecFn: lz4Codec | undefined;

const ensureLz4CodecRegistered = (codecFn?: lz4Codec) => {
  const resolvedCodecFn = codecFn ?? lz4Codec;

  if (!registeredLz4CodecFn) {
    CompressionCodecs[CompressionTypes.LZ4] = () => resolvedCodecFn;
    registeredLz4CodecFn = resolvedCodecFn;

    return;
  }

  if (registeredLz4CodecFn !== resolvedCodecFn) {
    throw new Error("LZ4 codec is already registered with a different implementation");
  }
};

class KTKafkaJSConsumerAdapter implements KTRuntimeConsumer {
  #consumer: KafkaJS.Consumer;

  constructor(consumer: KafkaJS.Consumer) {
    this.#consumer = consumer;
  }

  connect(): Promise<void> {
    return this.#consumer.connect();
  }

  disconnect(): Promise<void> {
    return this.#consumer.disconnect();
  }

  stop(): Promise<void> {
    return this.#consumer.stop();
  }

  subscribe(params: { topics: string[], fromBeginning: boolean }): Promise<void> {
    return this.#consumer.subscribe({
      topics: params.topics,
      fromBeginning: params.fromBeginning,
    });
  }

  async run(config: KTRuntimeConsumerRunConfig): Promise<void> {
    if (config.mode === "eachMessage") {
      await this.#consumer.run({
        partitionsConsumedConcurrently: config.partitionsConsumedConcurrently,
        eachMessage: async (payload) => {
          await config.eachMessage({
            topic: payload.topic,
            partition: payload.partition,
            message: {
              key: payload.message.key,
              value: payload.message.value,
              offset: payload.message.offset,
            },
            heartbeat: () => payload.heartbeat(),
          })
        },
      });

      return;
    }

    await this.#consumer.run({
      eachBatchAutoResolve: config.eachBatchAutoResolve,
      partitionsConsumedConcurrently: config.partitionsConsumedConcurrently,
      eachBatch: async (payload) => {
        await config.eachBatch({
          batch: {
            topic: payload.batch.topic,
            partition: payload.batch.partition,
            messages: payload.batch.messages.map((message) => ({
              key: message.key,
              value: message.value,
              offset: message.offset,
            })),
          },
          heartbeat: () => payload.heartbeat(),
          resolveOffset: (offset: string) => payload.resolveOffset(offset),
        })
      },
    });
  }
}

class KTKafkaJSProducerAdapter implements KTRuntimeProducer {
  #producer: KafkaJS.Producer;

  constructor(producer: KafkaJS.Producer) {
    this.#producer = producer;
  }

  connect(): Promise<void> {
    return this.#producer.connect();
  }

  disconnect(): Promise<void> {
    return this.#producer.disconnect();
  }

  send(params: {
    topic: string
    compression: unknown
    messages: Array<{
      key: string | null
      value: string
      headers?: IHeaders
    }>
  }): Promise<unknown> {
    return this.#producer.send({
      topic: params.topic,
      compression: params.compression as KafkaCompressionTypes,
      messages: params.messages,
    });
  }
}

class KTKafkaJSAdminAdapter implements KTRuntimeAdmin {
  #admin: KafkaJS.Admin;

  constructor(admin: KafkaJS.Admin) {
    this.#admin = admin;
  }

  connect(): Promise<void> {
    return this.#admin.connect();
  }

  disconnect(): Promise<void> {
    return this.#admin.disconnect();
  }

  fetchTopicMetadata(params: { topics: string[] }): Promise<{ topics: KafkaJS.ITopicMetadata[] }> {
    return this.#admin.fetchTopicMetadata(params);
  }

  createPartitions(params: { topicPartitions: Array<{ topic: string, count: number }> }): Promise<boolean> {
    return this.#admin.createPartitions(params);
  }

  createTopics(params: { topics: KafkaJS.ITopicConfig[], waitForLeaders?: boolean }): Promise<boolean> {
    return this.#admin.createTopics(params);
  }
}

class KTKafkaJSClientAdapter implements KTRuntimeClient {
  #kafka: KafkaJS.Kafka;

  constructor(params: KTKafkaRuntimeConfig) {
    const clientId = params.kafkaSettings.clientId ?? `consumer-${Date.now()}`
    const { brokerUrls, connectionTimeout } = params.kafkaSettings;
    const { compressionCodec } = params.kafkaSettings;

    if (!compressionCodec || compressionCodec.codecType === CompressionTypes.LZ4) {
      ensureLz4CodecRegistered(compressionCodec?.codecFn);
    }

    if (!Array.isArray(brokerUrls)) {
      throw new Error("KafkaBrokerUrls must be an array");
    }

    this.#kafka = new Kafka({
      clientId,
      brokers: brokerUrls,
      connectionTimeout,
      ...params.pureConfig,
    });
  }

  createAdmin(): KTRuntimeAdmin {
    return new KTKafkaJSAdminAdapter(this.#kafka.admin());
  }

  createProducer(params: { createPartitioner?: unknown, compression?: unknown }): KTRuntimeProducer {
    const producerConfig: {
      allowAutoTopicCreation: false
      createPartitioner?: ICustomPartitioner
    } = {
      allowAutoTopicCreation: false,
    }

    if (params.createPartitioner) {
      producerConfig.createPartitioner = params.createPartitioner as ICustomPartitioner
    }

    return new KTKafkaJSProducerAdapter(this.#kafka.producer(producerConfig));
  }

  createConsumer(params: {
    groupId: string
    allowAutoTopicCreation: boolean
    heartbeatInterval: number
    sessionTimeout: number
    maxWaitTimeInMs: number
    maxBytesPerPartition: number
    maxInFlightRequests: number
    rebalanceTimeout: number
    partitionAssigners: KTRuntimePartitionAssigner[]
    maxBytes: number
    fromBeginning: boolean
    batchConsuming: boolean
  }): KTRuntimeConsumer {
    return new KTKafkaJSConsumerAdapter(this.#kafka.consumer({
      groupId: params.groupId,
      allowAutoTopicCreation: params.allowAutoTopicCreation,
      heartbeatInterval: params.heartbeatInterval,
      sessionTimeout: params.sessionTimeout,
      maxWaitTimeInMs: params.maxWaitTimeInMs,
      maxBytesPerPartition: params.maxBytesPerPartition,
      maxInFlightRequests: params.maxInFlightRequests,
      rebalanceTimeout: params.rebalanceTimeout,
      partitionAssigners: params.partitionAssigners as KafkaJS.PartitionAssigner[],
      maxBytes: params.maxBytes,
    }));
  }
}

export {
  CompressionTypes,
  KafkaJS,
  KTKafkaJSClientAdapter,
}
