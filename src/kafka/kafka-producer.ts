import type { ICustomPartitioner, IHeaders } from "kafkajs";
import Kafka, { CompressionTypes } from "kafkajs";
import type pino from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import type { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { CustomPartitioner } from "./custom-partitioner.js";
import type { KafkaBrokerConfig, KafkaWithLogger } from "./kafka-broker.js";
import { KTKafkaBroker } from "./kafka-broker.js";
import type { KTRuntimeAdmin, KTRuntimeProducer } from "./runtime/transport-types.js";
import type { KTTopicBatchPayload } from "./topic-batch.js";

type KTKafkaProducerConfig = {
  createPartitioner?: ICustomPartitioner
} & KafkaBrokerConfig

class KTKafkaProducer extends KTKafkaBroker {
  #producer: KTRuntimeProducer;
  #admin: KTRuntimeAdmin;
  #logger: pino.Logger;
  #compressionType: CompressionTypes;
  #adminDependsOnProducer: boolean;

  constructor(params: KafkaWithLogger<KTKafkaProducerConfig>) {
    super(params);

    const { createPartitioner, logger } = params;
    const runtime = params.kafkaSettings.runtime ?? "confluent-kafkajs"

    let customPartitioner = createPartitioner

    if (!customPartitioner && runtime === "kafkajs") {
      customPartitioner = CustomPartitioner.roundRobin
    }

    if (runtime === "confluent-kafkajs" && createPartitioner) {
      throw new Error("Custom partitioners are not supported by the confluent-kafkajs runtime")
    }

    this.#producer = this._runtime.createProducer({
      createPartitioner: customPartitioner,
      compression: params.kafkaSettings.compressionCodec?.codecType ?? CompressionTypes.LZ4,
    });
    const dependentAdmin = this.#producer.createDependentAdmin?.()

    this.#admin = dependentAdmin ?? this._runtime.createAdmin();
    this.#logger = logger;
    this.#compressionType = params.kafkaSettings.compressionCodec?.codecType ?? CompressionTypes.LZ4;
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

  async createTopic(topicConfig: Kafka.ITopicConfig): Promise<void> {
    this.#logger.info({
      topicName: topicConfig.topic,
      topicConfig,
    }, "Resolving topics...");

    try {
      const topicMetadata = await this.#admin.fetchTopicMetadata({ topics: [topicConfig.topic] });

      const currentTopic = topicMetadata.topics.find(
        (topicMetadata) => topicMetadata.name === topicConfig.topic,
      );

      if (!currentTopic) {
        throw new Kafka.KafkaJSProtocolError('Topic not found')
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
    } catch (e) {
      if (e instanceof Kafka.KafkaJSProtocolError) {
        await this.#admin.createTopics({
          topics: [topicConfig],
          waitForLeaders: true,
        });
      } else {
        this.#logger.error(e, "Error from createTopic");
        throw e;
      }
    }
  }

  async sendSingleMessage(params: { topicName: KafkaTopicName, value: string, messageKey: KafkaMessageKey, headers: IHeaders  }) {
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
