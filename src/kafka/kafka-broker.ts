import type { CompressionTypes as KafkaCompressionTypes, KafkaConfig } from "kafkajs";
import KafkaJS from "kafkajs";
import type pino from "pino";

import { lz4Codec } from "../codec/lz4-codec.js";
import type { KafkaClientId } from "../libs/branded-types/kafka/index.js";

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

const { Kafka , CompressionTypes, CompressionCodecs } = KafkaJS

let registeredLz4CodecFn: lz4Codec | undefined;

const ensureLz4CodecRegistered = (codecFn?: lz4Codec) => {
  const resolvedCodecFn = codecFn ?? lz4Codec;

  if (!registeredLz4CodecFn) {
    CompressionCodecs[CompressionTypes.LZ4] = () => resolvedCodecFn;
    registeredLz4CodecFn = resolvedCodecFn;

    return;
  }

  if (registeredLz4CodecFn !== resolvedCodecFn) {
    throw new Error("LZ4 codec is already registered with a different implementation");
  }
};

class KTKafkaBroker {
  _kafka: KafkaJS.Kafka;

  constructor(params: KafkaBrokerConfig) {

    const clientId = params.kafkaSettings.clientId ?? `consumer-${Date.now()}`
    const { brokerUrls, connectionTimeout } = params.kafkaSettings;
    const { compressionCodec } = params.kafkaSettings;

    if (!compressionCodec || compressionCodec.codecType === CompressionTypes.LZ4) {
      ensureLz4CodecRegistered(compressionCodec?.codecFn);
    }

    if (!Array.isArray(brokerUrls)) {
      throw new Error("KafkaBrokerUrls must be an array");
    }

    this._kafka = new Kafka({
      clientId,
      brokers: brokerUrls,
      connectionTimeout,
      ...params.pureConfig,
    });
  }

  encode(message: object) {
    return JSON.stringify(message);
  }
  decode<T>(message: string) {
    return JSON.parse(message) as T;
  }
}

export { KTKafkaBroker };
