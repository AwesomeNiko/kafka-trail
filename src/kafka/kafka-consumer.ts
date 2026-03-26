import type pino from "pino";

import { ConsumerSubscribeError } from "../custom-errors/kafka-errors.js";
import type { KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { ifNanUseDefaultNumber } from "../libs/helpers/castings.js";
import { retry } from "../libs/helpers/retry.js";

import { KTKafkaBroker, type KafkaConsumer, type KafkaWithLogger, type KafkaBrokerConfig, type RdKafkaGlobalConfig } from "./kafka-broker.js";
import type { KTConsumerRunConfig } from "./kafka-types.js";

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

type RdKafkaMessage = {
  topic: string
  partition: number
  offset: number
  key?: Buffer | string | null | undefined
  value: Buffer | null
}

type RdKafkaTopicPartitionOffset = {
  topic: string
  partition: number
  offset: number
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
  }
} & KafkaBrokerConfig

const connectConsumer = async (consumer: InstanceType<typeof KafkaConsumer>) => {
  const error = await new Promise<unknown>((resolve) => {
    consumer.connect(undefined, (err) => {
      resolve(err ?? null)
    })
  })

  if (error) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw error
  }
}

const disconnectConsumer = async (consumer: InstanceType<typeof KafkaConsumer>) => {
  const error = await new Promise<unknown>((resolve) => {
    consumer.disconnect((err) => {
      resolve(err ?? null)
    })
  })

  if (error) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw error
  }
}

const consumeMessages = async (
  consumer: InstanceType<typeof KafkaConsumer>,
  batchSize: number,
): Promise<RdKafkaMessage[]> => {
  const result = await new Promise<{ error: unknown, messages: RdKafkaMessage[] }>((resolve) => {
    consumer.consume(batchSize, (err, messages) => {
      resolve({
        error: err ?? null,
        messages: messages || [],
      })
    })
  })

  if (result.error) {
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    throw result.error
  }

  return result.messages
}

const commitOffset = (consumer: InstanceType<typeof KafkaConsumer>, offset: RdKafkaTopicPartitionOffset) => {
  consumer.commitSync(offset)
}

class KTKafkaConsumer extends KTKafkaBroker {
  #isConnected = false;
  #logger: pino.Logger;
  #shouldStop = false;
  #runPromise: Promise<void> | null = null;
  heartBeatInterval: number;

  consumer: InstanceType<typeof KafkaConsumer> | null = null

  #subscribeRetry = {
    interval: 2000,
    retries: 50,
  };
  #subscribeFromBeginning = false;
  #consumeBatchSize = 32
  #consumerConfig: RdKafkaGlobalConfig;

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
      batchConsuming,
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

    this.#consumerConfig = {
      ...this._globalConfig,
      "group.id": consumerGroupId,
      "allow.auto.create.topics": false,
      "enable.auto.commit": false,
      "enable.auto.offset.store": false,
      "auto.offset.reset": this.#subscribeFromBeginning ? "earliest" : "latest",
      "session.timeout.ms": ifNanUseDefaultNumber(sessionTimeout, 60000),
      "fetch.wait.max.ms": ifNanUseDefaultNumber(maxWaitTimeInMs, 5000),
      "max.partition.fetch.bytes": ifNanUseDefaultNumber(maxBytesPerPartition, 10_485_760),
      "max.in.flight": ifNanUseDefaultNumber(maxInFlightRequests, 1),
      "partition.assignment.strategy": "roundrobin",
    }

    const maxBytesParam = ifNanUseDefaultNumber(maxBytes, 10_485_760)
    this.#consumerConfig["fetch.message.max.bytes"] = maxBytesParam

    if (rebalanceTimeout !== undefined) {
      this.#consumerConfig["max.poll.interval.ms"] = rebalanceTimeout
    }

    if (batchConsuming) {
      this.#consumeBatchSize = 128
    }

  }

  isConnected() {
    return this.#isConnected;
  }

  async init() {
    this.consumer = this.createConsumer(this.#consumerConfig)
    this.consumer.setDefaultConsumeTimeout(100)

    await connectConsumer(this.consumer);
    this.#isConnected = true;

    return this.isConnected();
  }

  async destroy() {
    this.#shouldStop = true

    if (this.#runPromise) {
      await this.#runPromise.catch((err: unknown) => {
        if (err instanceof Error && err.message === "Consumer destroyed") {
          return
        }

        throw err
      })
    }

    this.#isConnected = false;

    if (this.consumer && this.consumer.subscription().length > 0) {
      this.consumer.unsubscribe()
    }

    if (this.consumer) {
      await disconnectConsumer(this.consumer);
      this.consumer = null
    }
  }

  async subscribeTopic(topics: KafkaTopicName[]) {
    const consumer = this.consumer

    if (!consumer) {
      throw new Error("Consumer is not initialized")
    }

    const isSubscribed = await retry(
      () => {
        consumer.subscribe(topics)

        return Promise.resolve()
      },
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

  run(config: KTConsumerRunConfig) {
    if (this.#runPromise) {
      return Promise.resolve()
    }

    this.#shouldStop = false
    this.#runPromise = this.#runLoop(config).catch((err: unknown) => {
      this.#logger.error(err)
      throw err
    }).finally(() => {
      this.#runPromise = null
    })

    return Promise.resolve()
  }

  async #runLoop(config: KTConsumerRunConfig) {
    const consumer = this.consumer

    if (!consumer) {
      throw new Error("Consumer is not initialized")
    }

    while (!this.#shouldStop) {
      const messages = await consumeMessages(consumer, config.mode === "eachMessage" ? 1 : this.#consumeBatchSize)

      if (messages.length === 0) {
        continue
      }

      if (config.mode === "eachMessage") {
        for (const message of messages) {
          await this.#runEachMessage(message, config)
        }

        continue
      }

      await this.#runEachBatch(messages, config)
    }
  }

  async #runEachMessage(message: RdKafkaMessage, config: Extract<KTConsumerRunConfig, { mode: "eachMessage" }>) {
    const consumer = this.consumer

    if (!consumer) {
      throw new Error("Consumer is not initialized")
    }

    const payload: ConfluentEachMessagePayload = {
      topic: message.topic,
      partition: message.partition,
      message: {
        key: Buffer.isBuffer(message.key)
          ? message.key
          : message.key
            ? Buffer.from(String(message.key))
            : null,
        value: message.value,
        offset: String(message.offset),
      },
      heartbeat: () => Promise.resolve(),
    }

    await config.eachMessage(payload)
    consumer.commitMessageSync(message)
  }

  async #runEachBatch(messages: RdKafkaMessage[], config: Extract<KTConsumerRunConfig, { mode: "eachBatch" }>) {
    const consumer = this.consumer

    if (!consumer) {
      throw new Error("Consumer is not initialized")
    }

    const firstMessage = messages[0]

    if (!firstMessage) {
      return
    }

    let resolvedOffset: string | null = null

    const payload: ConfluentEachBatchPayload = {
      batch: {
        topic: firstMessage.topic,
        partition: firstMessage.partition,
        messages: messages.map((message) => ({
          key: Buffer.isBuffer(message.key)
            ? message.key
            : message.key
              ? Buffer.from(String(message.key))
              : null,
          value: message.value,
          offset: String(message.offset),
        })),
      },
      heartbeat: () => Promise.resolve(),
      resolveOffset: (offset: string) => {
        resolvedOffset = offset
      },
    }

    await config.eachBatch(payload)

    if (config.eachBatchAutoResolve && resolvedOffset === null) {
      const lastMessage = messages[messages.length - 1]

      if (lastMessage) {
        resolvedOffset = String(lastMessage.offset)
      }
    }

    if (resolvedOffset !== null) {
      commitOffset(consumer, {
        topic: firstMessage.topic,
        partition: firstMessage.partition,
        offset: Number(resolvedOffset) + 1,
      })
    }
  }
}

export { KTKafkaConsumer };
