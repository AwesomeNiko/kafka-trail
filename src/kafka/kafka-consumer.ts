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
    heartbeatEarlyFactor?: number
    sessionTimeout?: number;
    maxWaitTimeInMs?: number;
    maxBytesPerPartition?: number;
    maxInFlightRequests?: number;
  }
} & KafkaBrokerConfig

class KTKafkaConsumer extends KTKafkaBroker {
  #isConnected = false;
  #logger: pino.Logger;
  heartBeatInterval: number;

  consumer: Kafka.Consumer

  #subscribeRetry = {
    interval: 2000,
    retries: 50,
  };

  heartbeatEarlyFactor: number = 0.1

  constructor(params: KafkaWithLogger<KTKafkaConsumerConfig>) {
    super(params);

    const {
      consumerGroupId,
      subscribeRetries,
      subscribeRetryInterval,
      heartbeatInterval,
      heartbeatEarlyFactor,
      sessionTimeout,
      maxWaitTimeInMs,
      maxBytesPerPartition,
      maxInFlightRequests,
    } = params.kafkaSettings;

    if (!consumerGroupId) {
      throw new Error("group id must be provided");
    }

    const { logger } = params
    this.#logger = logger;

    this.#subscribeRetry.retries = ifNanUseDefaultNumber(subscribeRetries, 30);
    this.#subscribeRetry.interval = ifNanUseDefaultNumber(subscribeRetryInterval, 2000)
    this.heartBeatInterval = ifNanUseDefaultNumber(heartbeatInterval, 5000)
    this.heartbeatEarlyFactor = ifNanUseDefaultNumber(heartbeatEarlyFactor, 0.1)

    const maxWaitTimeInMsParam = ifNanUseDefaultNumber(maxWaitTimeInMs, 5000)
    const sessionTimeoutParam = ifNanUseDefaultNumber(sessionTimeout, 60000)
    const maxBytesPerPartitionParam = ifNanUseDefaultNumber(maxBytesPerPartition, 10485 * 2)
    const maxInFlightRequestsParam = ifNanUseDefaultNumber(maxInFlightRequests, 1)

    this.consumer = this._kafka.consumer({
      groupId: consumerGroupId,
      allowAutoTopicCreation: false,
      heartbeatInterval: this.heartBeatInterval,
      sessionTimeout: sessionTimeoutParam,
      maxWaitTimeInMs: maxWaitTimeInMsParam,
      maxBytesPerPartition:maxBytesPerPartitionParam,
      maxInFlightRequests: maxInFlightRequestsParam,
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
