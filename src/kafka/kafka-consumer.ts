import type pino from "pino";

import { ConsumerSubscribeError } from "../custom-errors/kafka-errors.js";
import type { KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { ifNanUseDefaultNumber } from "../libs/helpers/castings.js";
import { retry } from "../libs/helpers/retry.js";

import type { KafkaWithLogger , KafkaBrokerConfig } from "./kafka-broker.js";
import { KafkaJS, KTKafkaBroker } from "./kafka-broker.js";
import type { KTConsumerRunConfig, KTPartitionAssigner } from "./kafka-types.js";

type ConfluentConsumerConstructorConfig = NonNullable<Parameters<InstanceType<typeof KafkaJS.Kafka>["consumer"]>[0]>
type ConfluentKafkaJSConsumerConfig = NonNullable<ConfluentConsumerConstructorConfig["kafkaJS"]>
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
type ConfluentConsumerLike = ReturnType<InstanceType<typeof KafkaJS.Kafka>["consumer"]> & {
  subscribe: (params: { topics: string[] }) => Promise<void>
  run: (config: {
    partitionsConsumedConcurrently?: number
    eachBatchAutoResolve?: boolean
    eachMessage?: (payload: ConfluentEachMessagePayload) => Promise<void>
    eachBatch?: (payload: ConfluentEachBatchPayload) => Promise<void>
  }) => Promise<void>
}

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
    partitionAssignerFn? : KTPartitionAssigner
  }
} & KafkaBrokerConfig

class KTKafkaConsumer extends KTKafkaBroker {
  #isConnected = false;
  #logger: pino.Logger;
  heartBeatInterval: number;

  consumer: ConfluentConsumerLike

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

    if (partitionAssignerFn) {
      throw new Error("Custom partition assigners are not supported by the confluent runtime")
    }

    const kafkaJSConsumerConfig: ConfluentKafkaJSConsumerConfig = {
      groupId: consumerGroupId,
      allowAutoTopicCreation: false,
      heartbeatInterval: this.heartBeatInterval,
      sessionTimeout: sessionTimeoutParam,
      maxWaitTimeInMs: maxWaitTimeInMsParam,
      maxBytesPerPartition: maxBytesPerPartitionParam,
      maxInFlightRequests: maxInFlightRequestsParam,
      rebalanceTimeout: rebalanceTimeoutParam,
      partitionAssigners: [KafkaJS.PartitionAssigners.roundRobin],
      maxBytes: maxBytesParam,
      fromBeginning: this.#subscribeFromBeginning,
    }

    const consumerConfig: ConfluentConsumerConstructorConfig = {
      kafkaJS: kafkaJSConsumerConfig,
    }

    if (params.kafkaSettings.batchConsuming) {
      consumerConfig["js.consumer.max.batch.size"] = -1
    }

    this.consumer = this._kafka.consumer(consumerConfig) as ConfluentConsumerLike;
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
    await this.consumer.stop().catch((err: unknown) => {
      if (err instanceof Error && err.message === "Not implemented") {
        return;
      }

      throw err;
    });
    this.#isConnected = false;

    await this.consumer.disconnect();
  }

  async subscribeTopic(topics: KafkaTopicName[]) {
    const isSubscribed = await retry(
      async () =>
        await this.consumer.subscribe({
          topics,
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

  async run(config: KTConsumerRunConfig) {
    if (config.mode === "eachMessage") {
      await this.consumer.run({
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
      })

      return
    }

    await this.consumer.run({
      eachBatchAutoResolve: config.eachBatchAutoResolve,
      partitionsConsumedConcurrently: config.partitionsConsumedConcurrently,
      eachBatch: async (payload: ConfluentEachBatchPayload) => {
        await config.eachBatch({
          batch: {
            topic: payload.batch.topic,
            partition: payload.batch.partition,
            messages: payload.batch.messages.map((message) => ({
              key: message.key,
              value: message.value,
              offset: message.offset,
            })),
          },
          heartbeat: () => payload.heartbeat(),
          resolveOffset: (offset: string) => payload.resolveOffset(offset),
        })
      },
    })
  }
}

export { KTKafkaConsumer };
