import type pino from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import type { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { CODES, KTKafkaBroker, rdKafkaFactories, type Producer, type KafkaBrokerConfig, type KafkaWithLogger, type RdKafkaGlobalConfig } from "./kafka-broker.js";
import { KTCompressionTypes, type KTCustomPartitioner, type KTHeaders, type KTTopicConfig, type KTTopicMetadata } from "./kafka-types.js";
import type { KTTopicBatchPayload } from "./topic-batch.js";

type KTKafkaProducerConfig = {
  createPartitioner?: KTCustomPartitioner
} & KafkaBrokerConfig

type RdKafkaAdminClient = {
  disconnect: () => void
  createPartitions: (topic: string, desiredPartitions: number, timeout?: number, cb?: (err?: unknown) => void) => void
  createTopic: (topic: {
    topic: string
    num_partitions: number
    replication_factor: number
    config: Record<string, string>
  }, timeout?: number, cb?: (err?: unknown) => void) => void
}

type RdKafkaMessageHeader = Record<string, string | Buffer>

type ProducerDeliveryOpaque = {
  resolve: () => void
  reject: (error: Error) => void
}

const asRdKafkaCompression = (compression: unknown): string | undefined => {
  if (compression === undefined || compression === null) {
    return undefined
  }

  if (compression === KTCompressionTypes.None || compression === "none") {
    return "none"
  }

  if (compression === KTCompressionTypes.GZIP || compression === "gzip") {
    return "gzip"
  }

  if (compression === KTCompressionTypes.Snappy || compression === "snappy") {
    return "snappy"
  }

  if (compression === KTCompressionTypes.LZ4 || compression === "lz4") {
    return "lz4"
  }

  if (compression === KTCompressionTypes.ZSTD || compression === "zstd") {
    return "zstd"
  }

  throw new Error("Unsupported compression codec for the confluent runtime")
}

const toRdKafkaHeaders = (headers: KTHeaders): RdKafkaMessageHeader[] | undefined => {
  const mappedHeaders: RdKafkaMessageHeader[] = []

  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) {
      continue
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        mappedHeaders.push({ [key]: item })
      }

      continue
    }

    mappedHeaders.push({ [key]: value })
  }

  if (mappedHeaders.length === 0) {
    return undefined
  }

  return mappedHeaders
}

const connectProducer = async (producer: InstanceType<typeof Producer>) => {
  await new Promise<void>((resolve, reject) => {
    producer.connect(undefined, (err) => {
      if (err) {
        reject(err as unknown as Error)

        return
      }

      resolve()
    })
  })
}

const disconnectProducer = async (producer: InstanceType<typeof Producer>) => {
  await new Promise<void>((resolve, reject) => {
    producer.disconnect((err) => {
      if (err) {
        reject(err as unknown as Error)

        return
      }

      resolve()
    })
  })
}

const fetchTopicMetadata = async (
  producer: InstanceType<typeof Producer>,
  topic: string,
): Promise<KTTopicMetadata | null> => {
  return await new Promise<KTTopicMetadata | null>((resolve, reject) => {
    producer.getMetadata({
      topic,
      timeout: 30_000,
    }, (err, metadata) => {
      if (err) {
        const errorCode = (err as { code?: unknown }).code

        if (errorCode === CODES.ERRORS.ERR__UNKNOWN_TOPIC || errorCode === CODES.ERRORS.ERR_UNKNOWN_TOPIC_OR_PART) {
          resolve(null)

          return
        }

        reject(err as unknown as Error)

        return
      }

      const topicMetadata = metadata.topics.find((item) => item.name === topic)

      if (!topicMetadata) {
        resolve(null)

        return
      }

      resolve({
        name: topicMetadata.name,
        partitions: topicMetadata.partitions.map((partition) => ({
          partitionId: partition.id,
        })),
      })
    })
  })
}

class KTKafkaProducer extends KTKafkaBroker {
  #producer: InstanceType<typeof Producer> | null = null;
  #admin: RdKafkaAdminClient | null = null;
  #logger: pino.Logger;
  #producerConfig: RdKafkaGlobalConfig;

  constructor(params: KafkaWithLogger<KTKafkaProducerConfig>) {
    super(params);

    const { createPartitioner, logger } = params;

    if (createPartitioner) {
      throw new Error("Custom partitioners are not supported by the confluent runtime")
    }

    this.#producerConfig = {
      ...this._globalConfig,
      "dr_cb": true,
      "compression.codec": asRdKafkaCompression(params.kafkaSettings.compressionCodec?.codecType ?? KTCompressionTypes.LZ4),
    }
    this.#logger = logger
  }

  getAdmin() {
    return this.#admin
  }

  async init() {
    this.#producer = rdKafkaFactories.createProducer(this.#producerConfig)
    this.#producer.setPollInterval(50)
    this.#producer.on("delivery-report", (err, report) => {
      const opaque = report.opaque as ProducerDeliveryOpaque | undefined

      if (!opaque) {
        return
      }

      if (err) {
        opaque.reject(err as unknown as Error)

        return
      }

      opaque.resolve()
    })
    this.#admin = rdKafkaFactories.createAdminClient(this._globalConfig) as RdKafkaAdminClient

    await connectProducer(this.#producer)
  }

  async destroy() {
    this.#admin?.disconnect()
    this.#admin = null

    if (this.#producer) {
      await disconnectProducer(this.#producer)
      this.#producer = null
    }
  }

  async createTopic(topicConfig: KTTopicConfig): Promise<void> {
    const producer = this.#producer
    const admin = this.#admin

    if (!producer || !admin) {
      throw new Error("Producer is not initialized")
    }

    this.#logger.info({
      topicName: topicConfig.topic,
      topicConfig,
    }, "Resolving topics...");

    const currentTopic = await fetchTopicMetadata(producer, topicConfig.topic)

    if (!currentTopic) {
      await new Promise<void>((resolve, reject) => {
        admin.createTopic({
          topic: topicConfig.topic,
          num_partitions: topicConfig.numPartitions || 1,
          replication_factor: topicConfig.replicationFactor || 1,
          config: Object.fromEntries((topicConfig.configEntries || []).map((entry) => [entry.name, entry.value])),
        }, 30_000, (err: unknown) => {
          if (err) {
            reject(err as unknown as Error)

            return
          }

          resolve()
        })
      })

      return
    }

    if (topicConfig.numPartitions === currentTopic.partitions.length) {
      return;
    }

    if ((topicConfig.numPartitions || 0) > currentTopic.partitions.length) {
      await this.#createPartitions({
        topicPartitions: [
          {
            topic: topicConfig.topic,
            count: topicConfig.numPartitions || 0,
          },
        ],
      });
      this.#logger.info(`Expanded partitions for ${topicConfig.topic} topic`);
    } else {
      throw new UnableDecreasePartitionsError();
    }

    this.#logger.info("Topics resolved successful");
  }

  async sendSingleMessage(params: { topicName: KafkaTopicName, value: string, messageKey: KafkaMessageKey, headers: KTHeaders  }) {
    const { topicName, messageKey, value, headers } = params;

    await this.#produceMessage({
      topicName,
      value,
      messageKey,
      headers,
    })
  }

  async sendBatchMessages(params: KTTopicBatchPayload) {
    const { topicName, messages } = params;

    await Promise.all(messages.map(async (message) => {
      await this.#produceMessage({
        topicName,
        value: message.value,
        messageKey: message.key,
        headers: message.headers || {},
      })
    }))
  }

  async #produceMessage(params: {
    topicName: KafkaTopicName
    value: string
    messageKey: KafkaMessageKey
    headers: KTHeaders
  }) {
    const { topicName, messageKey, value, headers } = params
    const producer = this.#producer

    if (!producer) {
      throw new Error("Producer is not initialized")
    }

    await new Promise<void>((resolve, reject) => {
      const opaque: ProducerDeliveryOpaque = {
        resolve,
        reject,
      }

      try {
        producer.produce(
          topicName,
          null,
          Buffer.from(value),
          messageKey ?? null,
          null,
          opaque,
          toRdKafkaHeaders(headers),
        )
      } catch (err) {
        reject(err as unknown as Error)
      }
    })
  }

  async #createPartitions(params: { topicPartitions: Array<{ topic: string, count: number }> }): Promise<boolean> {
    const admin = this.#admin

    if (!admin) {
      throw new Error("Producer admin is not initialized")
    }

    await Promise.all(params.topicPartitions.map((topicPartition) => {
      return new Promise<void>((resolve, reject) => {
        admin.createPartitions(topicPartition.topic, topicPartition.count, 30_000, (err: unknown) => {
          if (err) {
            reject(err as unknown as Error)

            return
          }

          resolve()
        })
      })
    }))

    return true
  }
}

export { KTKafkaProducer };
