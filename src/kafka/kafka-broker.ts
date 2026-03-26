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

const { KafkaJS } = ConfluentKafka

type ConfluentCommonConstructorConfig = NonNullable<ConstructorParameters<typeof KafkaJS.Kafka>[0]>

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

class KTKafkaBroker {
  _kafka: InstanceType<typeof KafkaJS.Kafka>;

  constructor(params: KafkaBrokerConfig) {
    this._kafka = new KafkaJS.Kafka(toConfluentCommonConfig(params));
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
  KafkaJS,
  toConfluentCommonConfig,
};
