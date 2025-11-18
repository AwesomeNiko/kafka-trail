import type { IHeaders } from "kafkajs";

import type { KafkaTopicName , KafkaMessageKey } from "../libs/branded-types/kafka/index.js";
import { ktDecode, ktEncode, type KTTopicPayloadParser } from "../libs/helpers/default-data-parser.js";

import type { KTTopicSettings } from "./topic.ts";

export type KTTopicBatchRawMessage = Array<Omit<KTTopicBatchMessage, 'value'> & {value: object}>

export type KTTopicBatchMessage = {
  value: string
  key: KafkaMessageKey
  headers?: IHeaders & {
    traceId?: string
  }
}

export type KTTopicBatchPayload = {
  topicName: KafkaTopicName
  messages: KTTopicBatchMessage[]
}

export type KTTopicBatchEvent<Payload extends object> = {
  (payload: Payload): KTTopicBatchPayload;
  topicSettings: KTTopicSettings
  decode: KTTopicPayloadParser<Payload>['decode']
};

export type KTTopicBatch<T extends KTTopicBatchRawMessage>= typeof KTTopicBatch<T>
export type KTPayloadFromTopicBatch<T> = T extends KTTopicBatchEvent<infer P> ? P : never;

export const KTTopicBatch = <Payload extends KTTopicBatchRawMessage> (settings: KTTopicSettings): KTTopicBatchEvent<Payload>  => {
  const fn = (payload: KTTopicBatchRawMessage): KTTopicBatchPayload=> {
    const topicBatchMessages: KTTopicBatchMessage[] = []

    for (const data of payload) {
      const payloadToSend = ktEncode(data.value)

      topicBatchMessages.push({
        value: payloadToSend,
        key: data.key,
        headers: data.headers || {},
      })
    }

    return {
      topicName: settings.topic,
      messages: topicBatchMessages,
    }
  }

  fn.topicSettings = settings
  fn.decode = ktDecode

  return fn
}