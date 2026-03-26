import type pino from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import type { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import type { KafkaBrokerConfig, KafkaWithLogger } from "./kafka-broker.js";
import { createLowLevelAdminClient, createLowLevelAdminClientFromProducer, KafkaJS, KTKafkaBroker, isUnknownTopicError } from "./kafka-broker.js";
import { KTCompressionTypes, type KTCustomPartitioner, type KTHeaders, type KTTopicConfig, type KTTopicMetadata } from "./kafka-types.js";
import type { KTTopicBatchPayload } from "./topic-batch.js";

type ConfluentAdminLike = ReturnType<InstanceType<typeof KafkaJS.Kafka>["admin"]>
type ConfluentLowLevelAdminClient = {
  createPartitions: (topic: string, desiredPartitions: number, timeout?: number, cb?: (err?: Error) => void) => void
  disconnect: () => void
}
type ConfluentProducerLike = ReturnType<InstanceType<typeof KafkaJS.Kafka>["producer"]> & {
  dependentAdmin: () => ConfluentAdminLike
  _getInternalClient: () => unknown
}

type KTKafkaProducerConfig = {
  createPartitioner?: KTCustomPartitioner
} & KafkaBrokerConfig

const asConfluentCompression = (
  compression: unknown,
): typeof KafkaJS.CompressionTypes[keyof typeof KafkaJS.CompressionTypes] | undefined => {
  if (compression === undefined || compression === null) {
    return undefined
  }

  if (compression === KTCompressionTypes.None || compression === "none") {
    return KafkaJS.CompressionTypes.None
  }

  if (compression === KTCompressionTypes.GZIP || compression === "gzip") {
    return KafkaJS.CompressionTypes.GZIP
  }

  if (compression === KTCompressionTypes.Snappy || compression === "snappy") {
    return KafkaJS.CompressionTypes.Snappy
  }

  if (compression === KTCompressionTypes.LZ4 || compression === "lz4") {
    return KafkaJS.CompressionTypes.LZ4
  }

  if (compression === KTCompressionTypes.ZSTD || compression === "zstd") {
    return KafkaJS.CompressionTypes.ZSTD
  }

  throw new Error("Unsupported compression codec for the confluent runtime")
}

class KTKafkaProducer extends KTKafkaBroker {
  #producer: ConfluentProducerLike;
  #admin: ConfluentAdminLike;
  #lowLevelAdmin: ConfluentLowLevelAdminClient | null = null;
  #logger: pino.Logger;
  #adminDependsOnProducer: boolean;
  #params: KafkaBrokerConfig;

  constructor(params: KafkaWithLogger<KTKafkaProducerConfig>) {
    super(params);

    const { createPartitioner, logger } = params;
    this.#params = params

    if (createPartitioner) {
      throw new Error("Custom partitioners are not supported by the confluent runtime")
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
    const confluentCompression = asConfluentCompression(params.kafkaSettings.compressionCodec?.codecType ?? KTCompressionTypes.LZ4)

    if (confluentCompression) {
      producerConfig.kafkaJS.compression = confluentCompression
    }

    this.#producer = this._kafka.producer(producerConfig) as unknown as ConfluentProducerLike;
    const dependentAdmin = this.#producer.dependentAdmin?.()

    this.#admin = dependentAdmin ?? this._kafka.admin();
    this.#logger = logger;
    this.#adminDependsOnProducer = Boolean(dependentAdmin)
  }

  getAdmin() {
    return this.#admin
  }

  async init() {
    if (this.#adminDependsOnProducer) {
      await this.#producer.connect()
      await this.#admin.connect()
      this.#lowLevelAdmin = createLowLevelAdminClientFromProducer(this.#producer) as ConfluentLowLevelAdminClient

      return
    }

    await Promise.all([this.#admin.connect(), this.#producer.connect()]);
    this.#lowLevelAdmin = createLowLevelAdminClient(this.#params) as ConfluentLowLevelAdminClient
  }

  async destroy() {
    if (this.#adminDependsOnProducer) {
      this.#lowLevelAdmin?.disconnect()
      this.#lowLevelAdmin = null
      await this.#admin.disconnect()
      await this.#producer.disconnect()

      return
    }

    this.#lowLevelAdmin?.disconnect()
    this.#lowLevelAdmin = null

    await Promise.all([this.#admin.disconnect(), this.#producer.disconnect()]);
  }

  async createTopic(topicConfig: KTTopicConfig): Promise<void> {
    this.#logger.info({
      topicName: topicConfig.topic,
      topicConfig,
    }, "Resolving topics...");

    const topicMetadata = await this.#fetchTopicMetadata({ topics: [topicConfig.topic] });

    const currentTopic = topicMetadata.topics.find(
      (topicMetadata) => topicMetadata.name === topicConfig.topic,
    );

    if (!currentTopic) {
      await this.#admin.createTopics({
        topics: [topicConfig],
      });

      return;
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

    await this.#producer.send({
      topic: topicName,
      messages: [
        {
          key: messageKey ?? null,
          value,
          headers,
        },
      ],
    });
  }

  async sendBatchMessages(params: KTTopicBatchPayload) {
    const { topicName, messages } = params;

    await this.#producer.send({
      topic: topicName,
      messages,
    });
  }

  async #fetchTopicMetadata(params: { topics: string[] }): Promise<{ topics: KTTopicMetadata[] }> {
    try {
      const metadata = await this.#admin.fetchTopicMetadata(params);

      if (Array.isArray(metadata)) {
        return {
          topics: metadata as KTTopicMetadata[],
        }
      }

      return metadata as { topics: KTTopicMetadata[] }
    } catch (err) {
      if (isUnknownTopicError(err)) {
        return { topics: [] }
      }

      throw err
    }
  }

  async #createPartitions(params: { topicPartitions: Array<{ topic: string, count: number }> }): Promise<boolean> {
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
}

export { KTKafkaProducer };
