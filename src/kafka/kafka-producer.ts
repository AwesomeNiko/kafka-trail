import type { ICustomPartitioner, IHeaders, IResourceConfigEntry } from "kafkajs";
import Kafka, { CompressionTypes } from "kafkajs";
import type pino from "pino";

import { UnableDecreasePartitionsError } from "../custom-errors/kafka-errors.js";
import type { KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";

import { CustomPartitioner } from "./custom-partitioner.js";
import type { KafkaBrokerConfig, KafkaWithLogger } from "./kafka-broker.js";
import { KTKafkaBroker } from "./kafka-broker.js";

type KTKafkaProducerConfig = {
  createPartitioner?: ICustomPartitioner
} & KafkaBrokerConfig

class KTKafkaProducer extends KTKafkaBroker {
  #producer: Kafka.Producer;
  #admin: Kafka.Admin;
  #logger: pino.Logger;

  constructor(params: KafkaWithLogger<KTKafkaProducerConfig>) {
    super(params);

    const { createPartitioner, logger } = params;

    let customPartitioner = createPartitioner

    if(!customPartitioner) {
      customPartitioner = CustomPartitioner.roundRobin
    }

    this.#producer = this._kafka.producer({
      createPartitioner: customPartitioner,
      allowAutoTopicCreation: false,
    });
    this.#admin = this._kafka.admin();
    this.#logger = logger;
  }

  init() {
    return Promise.all([this.#admin.connect(), this.#producer.connect()]);
  }

  destroy() {
    return Promise.all([this.#admin.disconnect(), this.#producer.disconnect()]);
  }

  async createTopic(topicName: string, partitions = 1, customConfigArray: IResourceConfigEntry[] = []): Promise<void> {
    this.#logger.info({
      topicName,
      partitions,
      customConfigArray,
    }, "Resolving topics...");

    try {
      const topicMetadata = await this.#admin.fetchTopicMetadata({ topics: [topicName] });
      
      const currentTopic = topicMetadata.topics.find(
        (topicMetadata) => topicMetadata.name === topicName,
      );

      if (!currentTopic) {
        throw new Kafka.KafkaJSProtocolError('Topic not found')
      }

      if (partitions === currentTopic.partitions.length) {
        return;
      }

      if (partitions > currentTopic.partitions.length) {
        await this.#admin.createPartitions({
          topicPartitions: [
            {
              topic: topicName,
              count: partitions,
            },
          ],
        });
        this.#logger.info(`Expanded partitions for ${topicName} topic`);
      } else {
        throw new UnableDecreasePartitionsError();
      }

      this.#logger.info("Topics resolved successful");
    } catch (e) {
      if (e instanceof Kafka.KafkaJSProtocolError) {
        await this.#admin.createTopics({
          topics: [
            {
              topic: topicName,
              numPartitions: partitions,
              configEntries: customConfigArray,
            },
          ],
          waitForLeaders: true,
        });
      } else {
        this.#logger.error(e, "Error from createTopic");
        throw e;
      }
    }
  }

  async sendSingleMessage(params: { topicName: KafkaTopicName, message: string, messageKey: KafkaMessageKey  }, headers: IHeaders = {}) {
    const { topicName, messageKey, message } = params;

    await this.#producer.send({
      topic: topicName,
      compression: CompressionTypes.LZ4,
      messages: [
        {
          key: messageKey ?? null,
          value: message,
          headers,
        },
      ],
    });
  }

  // async sendBatchMessage<T extends object>(params: { topicName: KafkaTopicName, batchMessage: KTTopicPayload<T>[] }, headers: IHeaders = {}) {
  //   const { topicName, batchMessage } = params
  //
  //   const msgToSend = batchMessage.map((item) => ({
  //     key: item.messageKey,
  //     value: item.message,
  //     headers,
  //   }));
  //
  //   await this.#producer.send({
  //     topic: topicName,
  //     compression: CompressionTypes.LZ4,
  //     messages: msgToSend,
  //   });
  // }
}

export { KTKafkaProducer };
