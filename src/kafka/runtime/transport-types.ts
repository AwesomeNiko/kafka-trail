import type { KafkaMessageKey, KafkaTopicName } from "../../libs/branded-types/kafka/index.js";
import type { KTHeaders, KTPartitionAssigner, KTTopicConfig } from "../kafka-types.js";

export type KTRuntimeTopicConfig = KTTopicConfig
export type KTRuntimeHeaders = KTHeaders
export type KTRuntimePartitionAssigner = KTPartitionAssigner

export type KTRuntimeTopicPartitionConfig = {
  topic: string
  count: number
}

export type KTRuntimeTopicMetadata = {
  name: string
  partitions: Array<{
    partitionId: number
  }>
}

export type KTRuntimeProducerMessage = {
  key: KafkaMessageKey
  value: string
  headers?: KTRuntimeHeaders
}

export type KTRuntimeReceivedMessage = {
  key: Buffer | string | null | undefined
  value: Buffer | string | null | undefined
  offset: string
}

export type KTRuntimeEachMessagePayload = {
  topic: string
  partition: number
  message: KTRuntimeReceivedMessage
  heartbeat: () => Promise<void>
}

export type KTRuntimeBatch = {
  topic: string
  partition: number
  messages: KTRuntimeReceivedMessage[]
}

export type KTRuntimeEachBatchPayload = {
  batch: KTRuntimeBatch
  heartbeat: () => Promise<void>
  resolveOffset: (offset: string) => void
}

export type KTRuntimeEachMessageHandler = (payload: KTRuntimeEachMessagePayload) => Promise<void>
export type KTRuntimeEachBatchHandler = (payload: KTRuntimeEachBatchPayload) => Promise<void>

export type KTRuntimeConsumerRunConfig =
  | {
    mode: "eachMessage"
    partitionsConsumedConcurrently: number
    eachMessage: KTRuntimeEachMessageHandler
  }
  | {
    mode: "eachBatch"
    eachBatchAutoResolve: boolean
    partitionsConsumedConcurrently: number
    eachBatch: KTRuntimeEachBatchHandler
  }

export interface KTRuntimeAdmin {
  connect(): Promise<void>
  disconnect(): Promise<void>
  fetchTopicMetadata(params: { topics: string[] }): Promise<{ topics: KTRuntimeTopicMetadata[] }>
  createPartitions(params: { topicPartitions: KTRuntimeTopicPartitionConfig[] }): Promise<boolean>
  createTopics(params: { topics: KTRuntimeTopicConfig[], waitForLeaders?: boolean }): Promise<boolean>
}

export interface KTRuntimeProducer {
  connect(): Promise<void>
  disconnect(): Promise<void>
  createDependentAdmin?(): KTRuntimeAdmin
  send(params: {
    topic: KafkaTopicName
    compression: unknown
    messages: KTRuntimeProducerMessage[]
  }): Promise<unknown>
}

export interface KTRuntimeConsumer {
  connect(): Promise<void>
  disconnect(): Promise<void>
  stop(): Promise<void>
  subscribe(params: { topics: KafkaTopicName[], fromBeginning: boolean }): Promise<void>
  run(config: KTRuntimeConsumerRunConfig): Promise<void>
}

export interface KTRuntimeClient {
  createAdmin(): KTRuntimeAdmin
  createProducer(params: {
    createPartitioner?: unknown
    compression?: unknown
  }): KTRuntimeProducer
  createConsumer(params: {
    groupId: string
    allowAutoTopicCreation: boolean
    heartbeatInterval: number
    sessionTimeout: number
    maxWaitTimeInMs: number
    maxBytesPerPartition: number
    maxInFlightRequests: number
    rebalanceTimeout: number
    partitionAssigners: KTRuntimePartitionAssigner[]
    maxBytes: number
    fromBeginning: boolean
    batchConsuming: boolean
  }): KTRuntimeConsumer
}
