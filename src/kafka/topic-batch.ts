import type { KafkaTopicName , KafkaMessageKey } from "../libs/branded-types/kafka/index.js";
import { ktDecode, ktEncode, type KTTopicPayloadParser } from "../libs/helpers/default-data-parser.js";

import type { KTHeaders } from "./kafka-types.js";
import type { DLQPayload , KTTopicEvent,  KTTopicSettings } from "./topic.js";
import { createDLQTopicEvent } from "./topic.js";

export type KTTopicBatchRawMessage = Array<Omit<KTTopicBatchMessage, 'value'> & {value: object}>

export type KTTopicBatchMessage = {
  value: string
  key: KafkaMessageKey
  headers?: KTHeaders & {
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
  const validatePayload = (payload: unknown): payload is Payload[number]["value"] => {
    validatorFn?.validate?.(payload)

    return true
  }

  const fn = (payload: KTTopicBatchRawMessage): KTTopicBatchPayload => {
    const topicBatchMessages: KTTopicBatchMessage[] = []

    for (const data of payload) {
      validatePayload(data.value)
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
  fn.decode = ((data: string | Buffer) => {
    const decoded = (validatorFn?.decode ?? ktDecode<Payload[number]['value']>)(data)
    validatePayload(decoded)

    return decoded
  }) as KTTopicBatchEvent<Payload>['decode']

  return fn
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
    DLQTopic = createDLQTopicEvent<Payload>(settings)
  }

  return { BaseTopic, DLQTopic }
}
