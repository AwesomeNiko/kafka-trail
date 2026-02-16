import type Kafka from "kafkajs";
import type { PartitionAssigner } from "kafkajs";
import { PartitionAssigners } from "kafkajs";
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
    subscribeFromBeginning?: boolean;
    subscribeRetries?: number;
    subscribeRetryInterval?: number;
    heartbeatInterval?: number;
    partitionsConsumedConcurrently?: number
    heartbeatEarlyFactor?: number
    sessionTimeout?: number;
    maxWaitTimeInMs?: number;
    maxBytesPerPartition?: number;
    maxInFlightRequests?: number;
    batchConsuming?: boolean
    rebalanceTimeout?: number;
    maxBytes?: number;
    partitionAssignerFn? : PartitionAssigner
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
  #subscribeFromBeginning = false;

  heartbeatEarlyFactor: number = 0.1

  constructor(params: KafkaWithLogger<KTKafkaConsumerConfig>) {
    super(params);

    const {
      consumerGroupId,
      subscribeFromBeginning,
      subscribeRetries,
      subscribeRetryInterval,
      heartbeatInterval,
      heartbeatEarlyFactor,
      sessionTimeout,
      maxWaitTimeInMs,
      maxBytesPerPartition,
      maxInFlightRequests,
      rebalanceTimeout,
      maxBytes,
      partitionAssignerFn,
    } = params.kafkaSettings;

    if (!consumerGroupId) {
      throw new Error("group id must be provided");
    }

    const { logger } = params
    this.#logger = logger;

    this.#subscribeRetry.retries = ifNanUseDefaultNumber(subscribeRetries, 30);
    this.#subscribeRetry.interval = ifNanUseDefaultNumber(subscribeRetryInterval, 2000)
    this.#subscribeFromBeginning = subscribeFromBeginning ?? true;
    this.heartBeatInterval = ifNanUseDefaultNumber(heartbeatInterval, 5000)
    this.heartbeatEarlyFactor = ifNanUseDefaultNumber(heartbeatEarlyFactor, 0.1)

    const maxWaitTimeInMsParam = ifNanUseDefaultNumber(maxWaitTimeInMs, 5000)
    const sessionTimeoutParam = ifNanUseDefaultNumber(sessionTimeout, 60000)
    const maxBytesPerPartitionParam = ifNanUseDefaultNumber(maxBytesPerPartition, 10_485_760)
    const maxInFlightRequestsParam = ifNanUseDefaultNumber(maxInFlightRequests, 1)
    const rebalanceTimeoutParam = ifNanUseDefaultNumber(rebalanceTimeout, 60_000)
    const maxBytesParam = ifNanUseDefaultNumber(maxBytes, 10_485_760)

    const partitionsAssignersFunctions = [PartitionAssigners.roundRobin]

    if (partitionAssignerFn) {
      partitionsAssignersFunctions.unshift(partitionAssignerFn)
    }

    this.consumer = this._kafka.consumer({
      groupId: consumerGroupId,
      allowAutoTopicCreation: false,
      heartbeatInterval: this.heartBeatInterval,
      sessionTimeout: sessionTimeoutParam,
      maxWaitTimeInMs: maxWaitTimeInMsParam,
      maxBytesPerPartition:maxBytesPerPartitionParam,
      maxInFlightRequests: maxInFlightRequestsParam,
      rebalanceTimeout: rebalanceTimeoutParam,
      partitionAssigners: partitionsAssignersFunctions,
      maxBytes: maxBytesParam,
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
          fromBeginning: this.#subscribeFromBeginning,
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
