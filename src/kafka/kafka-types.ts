import type { KafkaTopicName } from "../libs/branded-types/kafka/index.js";

export const KTCompressionTypes = {
  None: 0,
  GZIP: 1,
  Snappy: 2,
  LZ4: 3,
  ZSTD: 4,
} as const;

export type KTCompressionType = typeof KTCompressionTypes[keyof typeof KTCompressionTypes]
export type KTLz4CompressionType = typeof KTCompressionTypes.LZ4

export type KTHeaderValue = string | Buffer | Array<string | Buffer> | undefined
export type KTHeaders = Record<string, KTHeaderValue>

export type KTTopicConfigEntry = {
  name: string
  value: string
}

export type KTTopicConfig = {
  topic: KafkaTopicName
  numPartitions?: number
  replicationFactor?: number
  configEntries?: KTTopicConfigEntry[]
}

export type KTRetryConfig = {
  initialRetryTime?: number
  retries?: number
  factor?: number
  multiplier?: number
  maxRetryTime?: number
}

export type KTSaslConfig = {
  mechanism: string
  username?: string
  password?: string
}

export type KTPureKafkaConfig = {
  ssl?: boolean
  sasl?: KTSaslConfig
  requestTimeout?: number
  retry?: KTRetryConfig
}

export type KTTopicPartitionConfig = {
  topic: string
  count: number
}

export type KTTopicMetadata = {
  name: string
  partitions: Array<{
    partitionId: number
  }>
}

export type KTReceivedMessage = {
  key: Buffer | string | null | undefined
  value: Buffer | string | null | undefined
  offset: string
}

export type KTEachMessagePayload = {
  topic: string
  partition: number
  message: KTReceivedMessage
  heartbeat: () => Promise<void>
}

export type KTBatch = {
  topic: string
  partition: number
  messages: KTReceivedMessage[]
}

export type KTEachBatchPayload = {
  batch: KTBatch
  heartbeat: () => Promise<void>
  resolveOffset: (offset: string) => void
}

export type KTEachMessageHandler = (payload: KTEachMessagePayload) => Promise<void>
export type KTEachBatchHandler = (payload: KTEachBatchPayload) => Promise<void>

export type KTConsumerRunConfig =
  | {
    mode: "eachMessage"
    partitionsConsumedConcurrently: number
    eachMessage: KTEachMessageHandler
  }
  | {
    mode: "eachBatch"
    eachBatchAutoResolve: boolean
    partitionsConsumedConcurrently: number
    eachBatch: KTEachBatchHandler
  }
