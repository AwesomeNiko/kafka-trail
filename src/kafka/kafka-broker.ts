import KafkaJS from "kafkajs";
import type pino from "pino";

import { lz4Codec } from "../codec/lz4-codec.js";
import type { KafkaClientId } from "../libs/branded-types/kafka/index.js";

type KTKafkaSettings = {
  brokerUrls: string[],
  clientId: KafkaClientId,
  connectionTimeout: number
  compressionCodec?: {
    codecType: keyof typeof CompressionCodecs,
    codecFn: lz4Codec
  }
}

export type KafkaBrokerConfig = {
  kafkaSettings: KTKafkaSettings
}

export type KafkaLogger = {
  logger: pino.Logger;
}

export type KafkaWithLogger<T extends KafkaBrokerConfig> = T & KafkaLogger

const { Kafka , CompressionTypes, CompressionCodecs } = KafkaJS

class KTKafkaBroker {
  _kafka: KafkaJS.Kafka;

  constructor(params: KafkaBrokerConfig) {

    const clientId = params.kafkaSettings.clientId ?? `consumer-${Date.now()}`
    const { brokerUrls, connectionTimeout } = params.kafkaSettings;

    CompressionCodecs[params.kafkaSettings.compressionCodec?.codecType ?? CompressionTypes.LZ4] = () => params.kafkaSettings.compressionCodec?.codecFn ?? lz4Codec;

    if (!Array.isArray(brokerUrls)) {
      throw new Error("KafkaBrokerUrls must be an array");
    }

    this._kafka = new Kafka({
      clientId,
      brokers: brokerUrls,
      connectionTimeout,
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
