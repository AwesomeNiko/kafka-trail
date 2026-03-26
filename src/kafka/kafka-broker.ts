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

export type RdKafkaGlobalConfig = Record<string, unknown>

const { CODES, Producer, KafkaConsumer, AdminClient } = ConfluentKafka

const toSaslConfig = (sasl: Record<string, unknown>): RdKafkaGlobalConfig => {
  const mechanism = typeof sasl.mechanism === "string"
    ? sasl.mechanism.toUpperCase()
    : undefined

  if (!mechanism) {
    throw new Error("SASL mechanism must be provided for the low-level confluent runtime")
  }

  const config: RdKafkaGlobalConfig = {
    "sasl.mechanism": mechanism,
  }

  if (typeof sasl.username === "string") {
    config["sasl.username"] = sasl.username
  }

  if (typeof sasl.password === "string") {
    config["sasl.password"] = sasl.password
  }

  return config
}

const toRdKafkaCommonConfig = (params: KafkaBrokerConfig): RdKafkaGlobalConfig => {
  if (params.pureConfig.ssl && typeof params.pureConfig.ssl !== "boolean") {
    throw new Error("Confluent runtime currently supports only boolean ssl configuration")
  }

  if (params.pureConfig.socketFactory !== undefined) {
    throw new Error("Custom socketFactory is not supported by the low-level confluent runtime")
  }

  if (params.pureConfig.reauthenticationThreshold !== undefined) {
    throw new Error("reauthenticationThreshold is not supported by the low-level confluent runtime")
  }

  const config: RdKafkaGlobalConfig = {
    "bootstrap.servers": params.kafkaSettings.brokerUrls.join(","),
    "client.id": params.kafkaSettings.clientId,
    "socket.connection.setup.timeout.ms": params.kafkaSettings.connectionTimeout,
    "allow.auto.create.topics": false,
  }

  if (params.pureConfig.requestTimeout !== undefined) {
    config["socket.timeout.ms"] = params.pureConfig.requestTimeout
  }

  if (params.pureConfig.retry?.initialRetryTime !== undefined) {
    config["retry.backoff.ms"] = params.pureConfig.retry.initialRetryTime
    config["reconnect.backoff.ms"] = params.pureConfig.retry.initialRetryTime
  }

  if (params.pureConfig.retry?.maxRetryTime !== undefined) {
    config["retry.backoff.max.ms"] = params.pureConfig.retry.maxRetryTime
    config["reconnect.backoff.max.ms"] = params.pureConfig.retry.maxRetryTime
  }

  if (typeof params.pureConfig.retry?.retries === "number") {
    config["message.send.max.retries"] = params.pureConfig.retry.retries
  }

  if (params.pureConfig.sasl !== undefined) {
    Object.assign(config, toSaslConfig(params.pureConfig.sasl))

    if (params.pureConfig.ssl) {
      config["security.protocol"] = "sasl_ssl"
    } else {
      config["security.protocol"] = "sasl_plaintext"
    }
  } else if (params.pureConfig.ssl) {
    config["security.protocol"] = "ssl"
  }

  return config
}

const rdKafkaFactories = {
  createProducer(config: RdKafkaGlobalConfig) {
    return new Producer(config)
  },
  createConsumer(config: RdKafkaGlobalConfig) {
    return new KafkaConsumer(config)
  },
  createAdminClient(config: RdKafkaGlobalConfig) {
    return AdminClient.create(config)
  },
}

class KTKafkaBroker {
  protected _globalConfig: RdKafkaGlobalConfig;

  constructor(params: KafkaBrokerConfig) {
    this._globalConfig = toRdKafkaCommonConfig(params);
  }

  encode(message: object) {
    return JSON.stringify(message);
  }

  decode<T>(message: string) {
    return JSON.parse(message) as T;
  }
}

export {
  KTKafkaBroker,
  Producer,
  KafkaConsumer,
  AdminClient,
  CODES,
  toRdKafkaCommonConfig,
  rdKafkaFactories,
};
