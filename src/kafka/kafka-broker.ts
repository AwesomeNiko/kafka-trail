import { createRequire } from "node:module";

import ConfluentKafka from "@confluentinc/kafka-javascript";
import type pino from "pino";

import type { lz4Codec } from "../codec/lz4-codec.js";
import type { KafkaClientId } from "../libs/branded-types/kafka/index.js";

import type { KTCompressionType, KTLz4CompressionType, KTPureKafkaConfig } from "./kafka-types.js";

type KTKafkaSettings = {
  brokerUrls: string[],
  clientId: KafkaClientId,
  connectionTimeout: number
  compressionCodec?: {
    codecType: KTLz4CompressionType,
    codecFn?: lz4Codec
  } | {
    codecType: Exclude<KTCompressionType, KTLz4CompressionType>,
    codecFn?: never
  }
}

export type KafkaBrokerConfig = {
  kafkaSettings: KTKafkaSettings
  pureConfig: KTPureKafkaConfig
}

export type KafkaLogger = {
  logger: pino.Logger;
}

export type KafkaWithLogger<T extends KafkaBrokerConfig> = T & KafkaLogger

const { KafkaJS, AdminClient, CODES } = ConfluentKafka
const require = createRequire(import.meta.url)
const { kafkaJSToRdKafkaConfig } = require("@confluentinc/kafka-javascript/lib/kafkajs/_common.js") as {
  kafkaJSToRdKafkaConfig: (config: Record<string, unknown>) => Record<string, unknown>
}

type ConfluentCommonConstructorConfig = NonNullable<ConstructorParameters<typeof KafkaJS.Kafka>[0]>

type CreateKafkaClientFn = (params: KafkaBrokerConfig) => InstanceType<typeof KafkaJS.Kafka>
type CreateLowLevelAdminClientFn = (params: KafkaBrokerConfig) => unknown
type CreateLowLevelAdminClientFromProducerFn = (producer: { _getInternalClient: () => unknown }) => unknown

let createKafkaClientImpl: CreateKafkaClientFn = (params) => {
  return new KafkaJS.Kafka(toConfluentCommonConfig(params))
}

let createLowLevelAdminClientImpl: CreateLowLevelAdminClientFn = (params) => {
  const adminConfig = toConfluentAdminClientConfig(params)

  return AdminClient.create(adminConfig)
}

let createLowLevelAdminClientFromProducerImpl: CreateLowLevelAdminClientFromProducerFn = (producer) => {
  return AdminClient.createFrom(producer._getInternalClient() as never)
}

const setKafkaClientFactoryForTests = (fn: CreateKafkaClientFn) => {
  createKafkaClientImpl = fn
}

const setLowLevelAdminFactoryForTests = (fn: CreateLowLevelAdminClientFn) => {
  createLowLevelAdminClientImpl = fn
}

const setLowLevelAdminFromProducerFactoryForTests = (fn: CreateLowLevelAdminClientFromProducerFn) => {
  createLowLevelAdminClientFromProducerImpl = fn
}

const resetKafkaClientFactoryForTests = () => {
  createKafkaClientImpl = (params) => {
    return new KafkaJS.Kafka(toConfluentCommonConfig(params))
  }

  createLowLevelAdminClientImpl = (params) => {
    const adminConfig = toConfluentAdminClientConfig(params)

    return AdminClient.create(adminConfig)
  }

  createLowLevelAdminClientFromProducerImpl = (producer) => {
    return AdminClient.createFrom(producer._getInternalClient() as never)
  }
}

const toConfluentCommonConfig = (params: KafkaBrokerConfig): ConfluentCommonConstructorConfig => {
  if (params.pureConfig.ssl && typeof params.pureConfig.ssl !== "boolean") {
    throw new Error("Confluent runtime currently supports only boolean ssl configuration")
  }

  const kafkaJSConfig: ConfluentCommonConstructorConfig["kafkaJS"] = {
    brokers: params.kafkaSettings.brokerUrls,
    clientId: params.kafkaSettings.clientId,
    connectionTimeout: params.kafkaSettings.connectionTimeout,
  }

  if (params.pureConfig.authenticationTimeout !== undefined) {
    kafkaJSConfig.authenticationTimeout = params.pureConfig.authenticationTimeout
  }

  if (params.pureConfig.requestTimeout !== undefined) {
    kafkaJSConfig.requestTimeout = params.pureConfig.requestTimeout
  }

  if (params.pureConfig.enforceRequestTimeout !== undefined) {
    kafkaJSConfig.enforceRequestTimeout = params.pureConfig.enforceRequestTimeout
  }

  if (params.pureConfig.retry !== undefined) {
    kafkaJSConfig.retry = params.pureConfig.retry
  }

  if (params.pureConfig.logLevel !== undefined) {
    Object.assign(kafkaJSConfig, {
      logLevel: params.pureConfig.logLevel as unknown,
    })
  }

  if (params.pureConfig.sasl !== undefined) {
    Object.assign(kafkaJSConfig, {
      sasl: params.pureConfig.sasl as unknown,
    })
  }

  if (typeof params.pureConfig.ssl === "boolean") {
    kafkaJSConfig.ssl = params.pureConfig.ssl
  }

  return {
    kafkaJS: kafkaJSConfig,
  }
}

const toConfluentAdminClientConfig = (params: KafkaBrokerConfig) => {
  const commonConfig = toConfluentCommonConfig(params)

  return kafkaJSToRdKafkaConfig(commonConfig as unknown as Record<string, unknown>)
}

const createLowLevelAdminClient = (params: KafkaBrokerConfig) => {
  return createLowLevelAdminClientImpl(params)
}

const createLowLevelAdminClientFromProducer = (producer: { _getInternalClient: () => unknown }) => {
  return createLowLevelAdminClientFromProducerImpl(producer)
}

const isUnknownTopicError = (err: unknown) => {
  if (!(err instanceof Error)) {
    return false
  }

  const errorCode = "code" in err ? err.code : undefined

  return errorCode === CODES.ERRORS.ERR__UNKNOWN_TOPIC
    || errorCode === CODES.ERRORS.ERR_UNKNOWN_TOPIC_OR_PART
}

class KTKafkaBroker {
  _kafka: InstanceType<typeof KafkaJS.Kafka>;

  constructor(params: KafkaBrokerConfig) {
    this._kafka = createKafkaClientImpl(params);
  }

  encode(message: object) {
    return JSON.stringify(message);
  }
  decode<T>(message: string) {
    return JSON.parse(message) as T;
  }
}

export {
  createLowLevelAdminClient,
  createLowLevelAdminClientFromProducer,
  isUnknownTopicError,
  KafkaJS,
  KTKafkaBroker,
  resetKafkaClientFactoryForTests,
  setKafkaClientFactoryForTests,
  setLowLevelAdminFactoryForTests,
  setLowLevelAdminFromProducerFactoryForTests,
};
