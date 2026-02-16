import type { IHeaders } from "kafkajs";

import type { KafkaTopicName , KafkaMessageKey } from "../libs/branded-types/kafka/index.js";
import { ktDecode, ktEncode, type KTTopicPayloadParser } from "../libs/helpers/default-data-parser.js";

import type { DLQPayload , KTTopicEvent,  KTTopicSettings } from "./topic.js";
import { DLQKTTopic  } from "./topic.js";

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
  decode: KTTopicPayloadParser<Payload extends KTTopicBatchRawMessage ? Payload[number]['value'] : object>['decode']
};

const createTopicBatchEvent = <Payload extends KTTopicBatchRawMessage>(
  settings: KTTopicSettings,
  validatorFn?: KTTopicPayloadParser<Payload[number]['value']>,
): KTTopicBatchEvent<Payload> => {
  const fn = (payload: KTTopicBatchRawMessage): KTTopicBatchPayload => {
    const topicBatchMessages: KTTopicBatchMessage[] = []

    for (const data of payload) {
      const payloadToSend = validatorFn
        ? validatorFn.encode(data.value as Payload[number]['value'])
        : ktEncode(data.value)

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
  fn.decode = (validatorFn?.decode ?? ktDecode) as KTTopicBatchEvent<Payload>['decode']

  return fn
}

/**
 * @deprecated Use CreateKTTopicBatch instead
 */
export const KTTopicBatch = <Payload extends KTTopicBatchRawMessage>(
  _settings: KTTopicSettings,
): KTTopicBatchEvent<Payload> => {
  throw new Error("Deprecated. use CreateKTTopicBatch(...)");
}

export const CreateKTTopicBatch = <Payload extends KTTopicBatchRawMessage>(
  settings: KTTopicSettings,
  validatorFn?: KTTopicPayloadParser<Payload[number]['value']>,
): {
  BaseTopic: KTTopicBatchEvent<Payload>,
  DLQTopic: KTTopicEvent<DLQPayload<Payload>> | null
} => {
  const BaseTopic = createTopicBatchEvent<Payload>(settings, validatorFn)
  let DLQTopic: KTTopicEvent<DLQPayload<Payload>> | null = null

  if (settings.createDLQ) {
    DLQTopic = DLQKTTopic<Payload>(settings)
  }

  return { BaseTopic, DLQTopic }
}
