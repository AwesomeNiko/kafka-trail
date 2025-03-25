import type { IHeaders, ITopicConfig } from "kafkajs";
import { v4 } from "uuid";

import type { KafkaTopicName , KafkaMessageKey } from "../libs/branded-types/kafka/index.js";
import type { KTTopicPayloadParser } from "../libs/helpers/default-data-parser.js";
import { ktEncode , ktDecode  } from "../libs/helpers/default-data-parser.js";

export type KTTopicSettings = ITopicConfig & {
  topic: KafkaTopicName
  batchMessageSizeToConsume: number
}

export type KTTopicPayload = {
  topicName: KafkaTopicName
  message: string
  messageKey: KafkaMessageKey
}

type KTTopicMeta = {
  meta?: {
    traceId?: string
  } & object & IHeaders
}

export type KTTopicPayloadWithMeta = KTTopicPayload & KTTopicMeta

export type KTTopicEvent<Payload extends object> = {
  (payload: Payload, { messageKey, meta }: KTTopicMeta & { messageKey: KafkaMessageKey }): KTTopicPayloadWithMeta;
  topicSettings: KTTopicSettings
  decode: KTTopicPayloadParser<Payload>['decode']
};

export type KTTopic<T extends object>= typeof KTTopic<T>

export const KTTopic = <Payload extends object> (settings: KTTopicSettings, validatorFn?:  KTTopicPayloadParser<Payload>): KTTopicEvent<Payload>  => {
  const fn = (payload: Payload,
    { messageKey, meta }: KTTopicMeta & { messageKey: KafkaMessageKey }): KTTopicPayloadWithMeta=> {
    let payloadToSend: string

    if (validatorFn) {
      payloadToSend = validatorFn.encode(payload)
    } else {
      payloadToSend = ktEncode(payload)
    }

    return {
      topicName: settings.topic,
      message: payloadToSend,
      messageKey: messageKey,
      meta: {
        ...meta,
        traceId: meta?.traceId ?? v4(),
      },
    }
  }

  fn.topicSettings = settings
  fn.decode = validatorFn?.decode ?? ktDecode

  return fn
}