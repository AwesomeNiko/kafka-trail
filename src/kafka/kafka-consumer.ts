import type Kafka from "kafkajs";
import type pino from "pino";

import { ConsumerSubscribeError } from "../custom-errors/kafka-errors.js";
import type { KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { ifNanUseDefaultNumber } from "../libs/helpers/castings.js";
import { retry } from "../libs/helpers/retry.js";

import type { KafkaWithLogger , KafkaBrokerConfig } from "./kafka-broker.js";
import { KTKafkaBroker } from "./kafka-broker.js";

export type KTKafkaConsumerConfig = {
  kafkaSettings: {
    consumerGroupId: string;
    subscribeRetries?: number;
    subscribeRetryInterval?: number;
    heartbeatInterval?: number;
    partitionsConsumedConcurrently?: number
  }
} & KafkaBrokerConfig

class KTKafkaConsumer extends KTKafkaBroker {
  #isConnected = false;
  #logger: pino.Logger;

  consumer: Kafka.Consumer

  #subscribeRetry = {
    interval: 2000,
    retries: 50,
  };

  constructor(params: KafkaWithLogger<KTKafkaConsumerConfig>) {
    super(params);

    const {
      consumerGroupId,
      subscribeRetries,
      subscribeRetryInterval,
      heartbeatInterval,
    } = params.kafkaSettings;

    if (!consumerGroupId) {
      throw new Error("group id must be provided");
    }

    const { logger } = params
    this.#logger = logger;

    this.#subscribeRetry.retries = ifNanUseDefaultNumber(subscribeRetries, 30);
    this.#subscribeRetry.interval = ifNanUseDefaultNumber(subscribeRetryInterval, 2000)
    const hbInterval = ifNanUseDefaultNumber(heartbeatInterval, 30)

    this.consumer = this._kafka.consumer({
      groupId: consumerGroupId,
      allowAutoTopicCreation: false,
      heartbeatInterval: hbInterval,
    });
  }

  isConnected() {
    return this.#isConnected;
  }

  async init() {
    await this.consumer.connect();
    this.#isConnected = true;

    return this.isConnected();
  }

  async destroy() {
    await this.consumer.stop();
    this.#isConnected = false;

    await this.consumer.disconnect();
  }

  async subscribeTopic(topics: KafkaTopicName[]) {
    const isSubscribed = await retry(
      async () =>
        await this.consumer.subscribe({
          topics,
          fromBeginning: true,
        }),
      this.#logger,
      {
        maxRetries: this.#subscribeRetry.retries,
        interval: this.#subscribeRetry.interval,
      },
    );

    if (!isSubscribed) {
      throw new ConsumerSubscribeError();
    }
  }
}

export { KTKafkaConsumer };
