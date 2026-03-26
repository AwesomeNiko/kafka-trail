import type pino from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import type { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import type { KafkaBrokerConfig, KafkaWithLogger } from "./kafka-broker.js";
import { KTKafkaBroker } from "./kafka-broker.js";
import { KTCompressionTypes, type KTCompressionType, type KTCustomPartitioner, type KTHeaders, type KTTopicConfig } from "./kafka-types.js";
import type { KTRuntimeAdmin, KTRuntimeProducer } from "./runtime/transport-types.js";
import type { KTTopicBatchPayload } from "./topic-batch.js";

type KTKafkaProducerConfig = {
  createPartitioner?: KTCustomPartitioner
} & KafkaBrokerConfig

class KTKafkaProducer extends KTKafkaBroker {
  #producer: KTRuntimeProducer;
  #admin: KTRuntimeAdmin;
  #logger: pino.Logger;
  #compressionType: KTCompressionType;
  #adminDependsOnProducer: boolean;

  constructor(params: KafkaWithLogger<KTKafkaProducerConfig>) {
    super(params);

    const { createPartitioner, logger } = params;

    if (createPartitioner) {
      throw new Error("Custom partitioners are not supported by the confluent runtime")
    }

    this.#producer = this._runtime.createProducer({
      createPartitioner,
      compression: params.kafkaSettings.compressionCodec?.codecType ?? KTCompressionTypes.LZ4,
    });
    const dependentAdmin = this.#producer.createDependentAdmin?.()

    this.#admin = dependentAdmin ?? this._runtime.createAdmin();
    this.#logger = logger;
    this.#compressionType = params.kafkaSettings.compressionCodec?.codecType ?? KTCompressionTypes.LZ4;
    this.#adminDependsOnProducer = Boolean(dependentAdmin)
  }

  getAdmin() {
    return this.#admin
  }

  async init() {
    if (this.#adminDependsOnProducer) {
      await this.#producer.connect()
      await this.#admin.connect()

      return
    }

    await Promise.all([this.#admin.connect(), this.#producer.connect()]);
  }

  async destroy() {
    if (this.#adminDependsOnProducer) {
      await this.#admin.disconnect()
      await this.#producer.disconnect()

      return
    }

    await Promise.all([this.#admin.disconnect(), this.#producer.disconnect()]);
  }

  async createTopic(topicConfig: KTTopicConfig): Promise<void> {
    this.#logger.info({
      topicName: topicConfig.topic,
      topicConfig,
    }, "Resolving topics...");

    const topicMetadata = await this.#admin.fetchTopicMetadata({ topics: [topicConfig.topic] });

    const currentTopic = topicMetadata.topics.find(
      (topicMetadata) => topicMetadata.name === topicConfig.topic,
    );

    if (!currentTopic) {
      await this.#admin.createTopics({
        topics: [topicConfig],
        waitForLeaders: true,
      });

      return;
    }

    if (topicConfig.numPartitions === currentTopic.partitions.length) {
      return;
    }

    if ((topicConfig.numPartitions || 0) > currentTopic.partitions.length) {
      await this.#admin.createPartitions({
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
      compression: this.#compressionType,
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
      compression: this.#compressionType,
      messages,
    });
  }
}

export { KTKafkaProducer };
