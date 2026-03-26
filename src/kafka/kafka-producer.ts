import type pino from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import type { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { CODES, KTKafkaBroker, type Producer, type KafkaBrokerConfig, type KafkaWithLogger, type RdKafkaGlobalConfig } from "./kafka-broker.js";
import { KTCompressionTypes, type KTHeaders, type KTTopicConfig, type KTTopicMetadata } from "./kafka-types.js";
import type { KTTopicBatchPayload } from "./topic-batch.js";

type KTKafkaProducerConfig = KafkaBrokerConfig

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
  reject: (error: unknown) => void
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
  const error = await new Promise<unknown>((resolve) => {
    producer.connect(undefined, (err) => {
      resolve(err ?? null)
    })
  })

  if (error) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw error
  }
}

const disconnectProducer = async (producer: InstanceType<typeof Producer>) => {
  const error = await new Promise<unknown>((resolve) => {
    producer.disconnect((err) => {
      resolve(err ?? null)
    })
  })

  if (error) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw error
  }
}

const fetchTopicMetadata = async (
  producer: InstanceType<typeof Producer>,
  topic: string,
): Promise<KTTopicMetadata | null> => {
  const result = await new Promise<{ error: unknown, metadata: KTTopicMetadata | null }>((resolve) => {
    producer.getMetadata({
      topic,
      timeout: 30_000,
    }, (err, metadata) => {
      if (err) {
        const errorCode = (err as { code?: unknown }).code

        if (errorCode === CODES.ERRORS.ERR__UNKNOWN_TOPIC || errorCode === CODES.ERRORS.ERR_UNKNOWN_TOPIC_OR_PART) {
          resolve({
            error: null,
            metadata: null,
          })

          return
        }

        resolve({
          error: err,
          metadata: null,
        })

        return
      }

      const topicMetadata = metadata.topics.find((item) => item.name === topic)

      if (!topicMetadata) {
        resolve({
          error: null,
          metadata: null,
        })

        return
      }

      resolve({
        error: null,
        metadata: {
          name: topicMetadata.name,
          partitions: topicMetadata.partitions.map((partition) => ({
            partitionId: partition.id,
          })),
        },
      })
    })
  })

  if (result.error) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw result.error
  }

  return result.metadata
}

class KTKafkaProducer extends KTKafkaBroker {
  #producer: InstanceType<typeof Producer> | null = null;
  #admin: RdKafkaAdminClient | null = null;
  #logger: pino.Logger;
  #producerConfig: RdKafkaGlobalConfig;

  constructor(params: KafkaWithLogger<KTKafkaProducerConfig>) {
    super(params);

    const { logger } = params;

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
    this.#producer = this.createProducer(this.#producerConfig)
    this.#producer.setPollInterval(50)
    this.#producer.on("delivery-report", (err, report) => {
      const opaque = report.opaque as ProducerDeliveryOpaque | undefined

      if (!opaque) {
        return
      }

      if (err) {
        opaque.reject(err)

        return
      }

      opaque.resolve()
    })
    this.#admin = this.createAdminClient(this._globalConfig) as RdKafkaAdminClient

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
      const error = await new Promise<unknown>((resolve) => {
        admin.createTopic({
          topic: topicConfig.topic,
          num_partitions: topicConfig.numPartitions || 1,
          replication_factor: topicConfig.replicationFactor || 1,
          config: Object.fromEntries((topicConfig.configEntries || []).map((entry) => [entry.name, entry.value])),
        }, 30_000, (err: unknown) => {
          resolve(err ?? null)
        })
      })

      if (error) {
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw error
      }

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

    const deliveryError = await new Promise<unknown>((resolve) => {
      const opaque: ProducerDeliveryOpaque = {
        resolve: () => resolve(null),
        reject: (error) => resolve(error),
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
        resolve(err)
      }
    })

    if (deliveryError) {
      // eslint-disable-next-line @typescript-eslint/only-throw-error
      throw deliveryError
    }
  }

  async #createPartitions(params: { topicPartitions: Array<{ topic: string, count: number }> }): Promise<boolean> {
    const admin = this.#admin

    if (!admin) {
      throw new Error("Producer admin is not initialized")
    }

    await Promise.all(params.topicPartitions.map(async (topicPartition) => {
      const error = await new Promise<unknown>((resolve) => {
        admin.createPartitions(topicPartition.topic, topicPartition.count, 30_000, (err: unknown) => {
          resolve(err ?? null)
        })
      })

      if (error) {
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw error
      }
    }))

    return true
  }
}

export { KTKafkaProducer };
