import { createRequire } from "node:module";

import ConfluentKafka from "@confluentinc/kafka-javascript";

import type { KafkaBrokerConfig } from "../kafka-broker.js";

import type { KTRuntimeAdmin, KTRuntimeClient, KTRuntimeConsumer, KTRuntimeConsumerRunConfig, KTRuntimePartitionAssigner, KTRuntimeProducer, KTRuntimeTopicConfig } from "./transport-types.js";

const { KafkaJS, AdminClient, CODES } = ConfluentKafka
const require = createRequire(import.meta.url)
const { kafkaJSToRdKafkaConfig } = require("@confluentinc/kafka-javascript/lib/kafkajs/_common.js") as {
  kafkaJSToRdKafkaConfig: (config: Record<string, unknown>) => Record<string, unknown>
}

type ConfluentCommonConstructorConfig = NonNullable<ConstructorParameters<typeof KafkaJS.Kafka>[0]>
type ConfluentConsumerConstructorConfig = NonNullable<Parameters<InstanceType<typeof KafkaJS.Kafka>["consumer"]>[0]>
type ConfluentKafkaJSConsumerConfig = NonNullable<ConfluentConsumerConstructorConfig["kafkaJS"]>
type ConfluentAdminLike = ReturnType<InstanceType<typeof KafkaJS.Kafka>["admin"]>
type ConfluentLowLevelAdminClient = {
  createPartitions: (topic: string, desiredPartitions: number, timeout?: number, cb?: (err?: Error) => void) => void
  disconnect: () => void
  name?: string
}

type ConfluentEachMessagePayload = {
  topic: string
  partition: number
  message: {
    key: Buffer | null
    value: Buffer | null
    offset: string
  }
  heartbeat: () => Promise<void>
}

type ConfluentEachBatchPayload = {
  batch: {
    topic: string
    partition: number
    messages: Array<{
      key: Buffer | null
      value: Buffer | null
      offset: string
    }>
  }
  heartbeat: () => Promise<void>
  resolveOffset: (offset: string) => void
}

type ConfluentConsumerLike = {
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  stop: () => Promise<void>
  subscribe: (params: { topics: string[] }) => Promise<void>
  run: (config: {
    partitionsConsumedConcurrently?: number
    eachBatchAutoResolve?: boolean
    eachMessage?: (payload: ConfluentEachMessagePayload) => Promise<void>
    eachBatch?: (payload: ConfluentEachBatchPayload) => Promise<void>
  }) => Promise<void>
}

type ConfluentHeaders = Record<string, Buffer | string | Array<Buffer | string> | undefined>

type ConfluentProducerLike = {
  connect: () => Promise<void>
  disconnect: () => Promise<void>
  dependentAdmin: () => ConfluentAdminLike
  _getInternalClient: () => unknown
  send: (params: {
    topic: string
    messages: Array<{
      key: string | null
      value: string
      headers?: ConfluentHeaders
    }>
  }) => Promise<unknown>
}

const asConfluentCompression = (
  compression: unknown,
): typeof KafkaJS.CompressionTypes[keyof typeof KafkaJS.CompressionTypes] | undefined => {
  if (compression === undefined || compression === null) {
    return undefined
  }

  if (compression === 0 || compression === "none") {
    return KafkaJS.CompressionTypes.None
  }

  if (compression === 1 || compression === "gzip") {
    return KafkaJS.CompressionTypes.GZIP
  }

  if (compression === 2 || compression === "snappy") {
    return KafkaJS.CompressionTypes.Snappy
  }

  if (compression === 3 || compression === "lz4") {
    return KafkaJS.CompressionTypes.LZ4
  }

  if (compression === 4 || compression === "zstd") {
    return KafkaJS.CompressionTypes.ZSTD
  }

  throw new Error("Unsupported compression codec for confluent-kafkajs runtime")
}

const ensureSupportedPartitionAssigners = (partitionAssigners: KTRuntimePartitionAssigner[]) => {
  const supportedAssigners = new Set(["roundrobin", "range", "cooperative-sticky"])

  for (const partitionAssigner of partitionAssigners) {
    if (typeof partitionAssigner !== "string" || !supportedAssigners.has(partitionAssigner)) {
      throw new Error("Custom partition assigners are not supported by the confluent-kafkajs runtime")
    }
  }
}

const toConfluentPartitionAssigners = (
  partitionAssigners: KTRuntimePartitionAssigner[],
): Array<
  typeof KafkaJS.PartitionAssigners.roundRobin
  | typeof KafkaJS.PartitionAssigners.range
  | typeof KafkaJS.PartitionAssigners.cooperativeSticky
> => {
  return partitionAssigners.map((partitionAssigner) => {
    if (partitionAssigner === "roundrobin") {
      return KafkaJS.PartitionAssigners.roundRobin
    }

    if (partitionAssigner === "range") {
      return KafkaJS.PartitionAssigners.range
    }

    return KafkaJS.PartitionAssigners.cooperativeSticky
  })
}

const toConfluentCommonConfig = (params: KafkaBrokerConfig): ConfluentCommonConstructorConfig => {
  if (params.pureConfig.ssl && typeof params.pureConfig.ssl !== "boolean") {
    throw new Error("confluent-kafkajs runtime currently supports only boolean ssl configuration")
  }

  const kafkaJSConfig: ConfluentCommonConstructorConfig["kafkaJS"] = {
    brokers: params.kafkaSettings.brokerUrls,
    clientId: params.kafkaSettings.clientId,
    connectionTimeout: params.kafkaSettings.connectionTimeout,
  }

  if (params.pureConfig.authenticationTimeout !== undefined) {
    kafkaJSConfig.authenticationTimeout = params.pureConfig.authenticationTimeout
  }

  if (params.pureConfig.requestTimeout !== undefined) {
    kafkaJSConfig.requestTimeout = params.pureConfig.requestTimeout
  }

  if (params.pureConfig.enforceRequestTimeout !== undefined) {
    kafkaJSConfig.enforceRequestTimeout = params.pureConfig.enforceRequestTimeout
  }

  if (params.pureConfig.retry !== undefined) {
    kafkaJSConfig.retry = params.pureConfig.retry
  }

  if (params.pureConfig.logLevel !== undefined) {
    Object.assign(kafkaJSConfig, {
      logLevel: params.pureConfig.logLevel as unknown,
    })
  }

  if (params.pureConfig.sasl !== undefined) {
    Object.assign(kafkaJSConfig, {
      sasl: params.pureConfig.sasl as unknown,
    })
  }

  if (typeof params.pureConfig.ssl === "boolean") {
    kafkaJSConfig.ssl = params.pureConfig.ssl
  }

  return {
    kafkaJS: kafkaJSConfig,
  }
}

const toConfluentAdminClientConfig = (params: KafkaBrokerConfig) => {
  const commonConfig = toConfluentCommonConfig(params)

  return kafkaJSToRdKafkaConfig(commonConfig as unknown as Record<string, unknown>)
}

const isConfluentUnknownTopicError = (err: unknown) => {
  if (!(err instanceof Error)) {
    return false
  }

  const errorCode = "code" in err ? err.code : undefined

  return errorCode === CODES.ERRORS.ERR__UNKNOWN_TOPIC
    || errorCode === CODES.ERRORS.ERR_UNKNOWN_TOPIC_OR_PART
}

const normalizeConfluentTopicMetadata = (
  metadata: unknown,
): { topics: Array<{ name: string, partitions: Array<{ partitionId: number }> }> } => {
  if (Array.isArray(metadata)) {
    return {
      topics: metadata as Array<{ name: string, partitions: Array<{ partitionId: number }> }>,
    }
  }

  return metadata as { topics: Array<{ name: string, partitions: Array<{ partitionId: number }> }> }
}

const connectConfluentLowLevelAdminClient = (params: KafkaBrokerConfig): Promise<ConfluentLowLevelAdminClient> => {
  const adminConfig = toConfluentAdminClientConfig(params)

  return Promise.resolve(AdminClient.create(adminConfig) as ConfluentLowLevelAdminClient)
}

const connectConfluentLowLevelAdminClientFromProducer = (
  producer: ConfluentProducerLike,
): Promise<ConfluentLowLevelAdminClient> => {
  return Promise.resolve(
    AdminClient.createFrom(producer._getInternalClient() as never) as ConfluentLowLevelAdminClient,
  )
}

class KTConfluentKafkaJSConsumerAdapter implements KTRuntimeConsumer {
  #consumer: ConfluentConsumerLike;
  #connected = false;

  constructor(consumer: ConfluentConsumerLike) {
    this.#consumer = consumer;
  }

  connect(): Promise<void> {
    return this.#consumer.connect().then(() => {
      this.#connected = true
    });
  }

  async disconnect(): Promise<void> {
    if (!this.#connected) {
      return;
    }

    await this.#consumer.disconnect();
    this.#connected = false
  }

  stop(): Promise<void> {
    return this.#consumer.stop().catch((err: unknown) => {
      if (err instanceof Error && err.message === "Not implemented") {
        return;
      }

      throw err;
    });
  }

  subscribe(params: { topics: string[], fromBeginning: boolean }): Promise<void> {
    return this.#consumer.subscribe({
      topics: params.topics,
    });
  }

  async run(config: KTRuntimeConsumerRunConfig): Promise<void> {
    if (config.mode === "eachMessage") {
      await this.#consumer.run({
        partitionsConsumedConcurrently: config.partitionsConsumedConcurrently,
        eachMessage: async (payload: ConfluentEachMessagePayload) => {
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
      eachBatch: async (payload: ConfluentEachBatchPayload) => {
        await config.eachBatch({
          batch: {
            topic: payload.batch.topic,
            partition: payload.batch.partition,
            messages: payload.batch.messages.map((message: ConfluentEachBatchPayload["batch"]["messages"][number]) => ({
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

class KTConfluentKafkaJSProducerAdapter implements KTRuntimeProducer {
  #producer: ConfluentProducerLike;
  #connected = false;

  constructor(producer: ConfluentProducerLike) {
    this.#producer = producer;
  }

  connect(): Promise<void> {
    return this.#producer.connect().then(() => {
      this.#connected = true
    });
  }

  async disconnect(): Promise<void> {
    if (!this.#connected) {
      return;
    }

    await this.#producer.disconnect();
    this.#connected = false
  }

  createDependentAdmin(): KTRuntimeAdmin {
    return new KTConfluentKafkaJSAdminAdapter(
      this.#producer.dependentAdmin(),
      () => connectConfluentLowLevelAdminClientFromProducer(this.#producer),
    )
  }

  send(params: {
    topic: string
    compression: unknown
    messages: Array<{
      key: string | null
      value: string
      headers?: ConfluentHeaders
    }>
  }): Promise<unknown> {
    return this.#producer.send({
      topic: params.topic,
      messages: params.messages,
    });
  }
}

class KTConfluentKafkaJSAdminAdapter implements KTRuntimeAdmin {
  #admin: ConfluentAdminLike;
  #createLowLevelAdmin: () => Promise<ConfluentLowLevelAdminClient>;
  #lowLevelAdmin: ConfluentLowLevelAdminClient | null = null;

  constructor(admin: ConfluentAdminLike, createLowLevelAdmin: () => Promise<ConfluentLowLevelAdminClient>) {
    this.#admin = admin;
    this.#createLowLevelAdmin = createLowLevelAdmin;
  }

  async connect(): Promise<void> {
    await this.#admin.connect();
    this.#lowLevelAdmin = await this.#createLowLevelAdmin()
  }

  async disconnect(): Promise<void> {
    if (this.#lowLevelAdmin) {
      this.#lowLevelAdmin.disconnect()
      this.#lowLevelAdmin = null
    }

    await this.#admin.disconnect();
  }

  async fetchTopicMetadata(params: { topics: string[] }): Promise<{ topics: Array<{ name: string, partitions: Array<{ partitionId: number }> }> }> {
    try {
      const metadata = await this.#admin.fetchTopicMetadata(params);

      return normalizeConfluentTopicMetadata(metadata)
    } catch (err) {
      if (isConfluentUnknownTopicError(err)) {
        return { topics: [] }
      }

      throw err
    }
  }

  async createPartitions(params: { topicPartitions: Array<{ topic: string, count: number }> }): Promise<boolean> {
    if (!this.#lowLevelAdmin) {
      throw new Error("Confluent low-level admin client is not connected")
    }

    await Promise.all(params.topicPartitions.map((topicPartition) => {
      return new Promise<void>((resolve, reject) => {
        this.#lowLevelAdmin?.createPartitions(topicPartition.topic, topicPartition.count, 30_000, (err?: Error) => {
          if (err) {
            reject(err)

            return
          }

          resolve()
        })
      })
    }))

    return true
  }

  createTopics(params: { topics: KTRuntimeTopicConfig[], waitForLeaders?: boolean }): Promise<boolean> {
    return this.#admin.createTopics({
      topics: params.topics,
    });
  }
}

class KTConfluentKafkaJSClientAdapter implements KTRuntimeClient {
  #kafka: InstanceType<typeof KafkaJS.Kafka>;
  #params: KafkaBrokerConfig;

  constructor(params: KafkaBrokerConfig) {
    this.#params = params
    this.#kafka = new KafkaJS.Kafka(toConfluentCommonConfig(params));
  }

  createAdmin(): KTRuntimeAdmin {
    return new KTConfluentKafkaJSAdminAdapter(
      this.#kafka.admin(),
      () => connectConfluentLowLevelAdminClient(this.#params),
    );
  }

  createProducer(params: { createPartitioner?: unknown, compression?: unknown }): KTRuntimeProducer {
    if (params.createPartitioner) {
      throw new Error("Custom partitioners are not supported by the confluent-kafkajs runtime")
    }

    const producerConfig: {
      kafkaJS: {
        allowAutoTopicCreation: false
        compression?: typeof KafkaJS.CompressionTypes[keyof typeof KafkaJS.CompressionTypes]
      }
    } = {
      kafkaJS: {
        allowAutoTopicCreation: false,
      },
    }

    const confluentCompression = asConfluentCompression(params.compression)

    if (confluentCompression) {
      producerConfig.kafkaJS.compression = confluentCompression
    }

    return new KTConfluentKafkaJSProducerAdapter(this.#kafka.producer(producerConfig) as unknown as ConfluentProducerLike);
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
    ensureSupportedPartitionAssigners(params.partitionAssigners)

    const kafkaJSConsumerConfig: ConfluentKafkaJSConsumerConfig = {
      groupId: params.groupId,
      allowAutoTopicCreation: params.allowAutoTopicCreation,
      heartbeatInterval: params.heartbeatInterval,
      sessionTimeout: params.sessionTimeout,
      maxWaitTimeInMs: params.maxWaitTimeInMs,
      maxBytesPerPartition: params.maxBytesPerPartition,
      maxInFlightRequests: params.maxInFlightRequests,
      rebalanceTimeout: params.rebalanceTimeout,
      partitionAssigners: toConfluentPartitionAssigners(params.partitionAssigners),
      maxBytes: params.maxBytes,
      fromBeginning: params.fromBeginning,
    }

    const consumerConfig: ConfluentConsumerConstructorConfig = {
      kafkaJS: kafkaJSConsumerConfig,
    }

    if (params.batchConsuming) {
      consumerConfig["js.consumer.max.batch.size"] = -1

    }

    return new KTConfluentKafkaJSConsumerAdapter(this.#kafka.consumer(consumerConfig) as ConfluentConsumerLike);
  }
}

export { KTConfluentKafkaJSClientAdapter };
