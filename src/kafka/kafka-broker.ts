import type { CompressionTypes as KafkaCompressionTypes, KafkaConfig } from "kafkajs";
import type pino from "pino";

import type { lz4Codec } from "../codec/lz4-codec.js";
import type { KafkaClientId } from "../libs/branded-types/kafka/index.js";

import { createKafkaRuntime } from "./runtime/runtime-factory.js";
import type { KTRuntimeClient } from "./runtime/transport-types.js";
type KTKafkaSettings = {
  brokerUrls: string[],
  clientId: KafkaClientId,
  connectionTimeout: number
  compressionCodec?: {
    codecType: KafkaCompressionTypes.LZ4,
    codecFn?: lz4Codec
  } | {
    codecType: Exclude<KafkaCompressionTypes, KafkaCompressionTypes.LZ4>,
    codecFn?: never
  }
}

export type KafkaBrokerConfig = {
  kafkaSettings: KTKafkaSettings
  pureConfig: Pick<KafkaConfig, "ssl" | "sasl" | "authenticationTimeout" | "reauthenticationThreshold" | "requestTimeout" | "enforceRequestTimeout" | "retry" | "socketFactory" | "logLevel" | "logCreator">
}

export type KafkaLogger = {
  logger: pino.Logger;
}

export type KafkaWithLogger<T extends KafkaBrokerConfig> = T & KafkaLogger

class KTKafkaBroker {
  _runtime: KTRuntimeClient;

  constructor(params: KafkaBrokerConfig) {
    this._runtime = createKafkaRuntime(params);
  }

  encode(message: object) {
    return JSON.stringify(message);
  }
  decode<T>(message: string) {
    return JSON.parse(message) as T;
  }
}

export { KTKafkaBroker };
