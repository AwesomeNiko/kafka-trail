import type { IHeaders, ITopicConfig } from "kafkajs";
import { v4 } from "uuid";

import type { KafkaTopicName , KafkaMessageKey } from "../libs/branded-types/kafka/index.js";
import type { KTTopicPayloadParser } from "../libs/helpers/default-data-parser.js";
import { ktEncode , ktDecode  } from "../libs/helpers/default-data-parser.js";
import { CreateDlqTopicName } from "../libs/helpers/topic-name.js";

export type KTTopicSettings = ITopicConfig & {
  topic: KafkaTopicName
  batchMessageSizeToConsume: number
  numPartitions: number
  createDLQ: boolean
}

export type KTTopicPayload = {
  topicName: KafkaTopicName
  message: string
  messageKey: KafkaMessageKey
}

type KTTopicMeta = {
  meta: IHeaders & {
    traceId?: string
  }
}

export type KTTopicPayloadWithMeta = KTTopicPayload & KTTopicMeta

export type KTTopicEvent<Payload extends object> = {
  (payload: Payload, { messageKey, meta }: KTTopicMeta & { messageKey: KafkaMessageKey }): KTTopicPayloadWithMeta;
  topicSettings: KTTopicSettings
  decode: KTTopicPayloadParser<Payload>['decode']
};

export type KTTopic<T extends object>= typeof KTTopic<T>
export type KTPayloadFromTopic<T> = T extends KTTopicEvent<infer P> ? P : never;
export type DLQPayload<T> = {
  originalTopic: KafkaTopicName;
  oritinalPartition: number;
  originalOffset: string | undefined;
  key: KafkaMessageKey | null;
  value: T;
  errorMessage: string;
  failedAt: number;
}

/**
 * @deprecated Use CreateKTTopic instead
 */
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

export const DLQKTTopic = <Payload extends object> (settings: KTTopicSettings, validatorFn?:  KTTopicPayloadParser<DLQPayload<Payload>>): KTTopicEvent<DLQPayload<Payload>> => {
  return KTTopic<DLQPayload<Payload>>({ ...settings, createDLQ: true, topic: CreateDlqTopicName(settings.topic) }, validatorFn)
}

export const CreateKTTopic = <Payload extends object> (settings: KTTopicSettings, validatorFn?:  KTTopicPayloadParser<Payload>): {
  BaseTopic: KTTopicEvent<Payload>,
  DLQTopic: KTTopicEvent<DLQPayload<Payload>> | null
} => {
  const BaseTopic = KTTopic<Payload>(settings, validatorFn)
  let DLQTopic: KTTopicEvent<DLQPayload<Payload>> | null = null

  if (settings.createDLQ) {
    DLQTopic = DLQKTTopic<Payload>(settings)
  }

  return { BaseTopic, DLQTopic }
}
