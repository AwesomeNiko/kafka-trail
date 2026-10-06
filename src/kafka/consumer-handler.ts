import type { KafkaLogger } from "./kafka-broker.js";
import type { KTTopicBatchEvent } from "./topic-batch.js";
import type { KTTopicEvent, KTTopicPayloadWithMeta } from "./topic.js";

export type KTHandlerPublisher = {
  publishSingleMessage: (topic: KTTopicPayloadWithMeta) => Promise<void>
}

export type KTRun<Payload extends object, Ctx extends object> = (
  payload: Payload[],
  ctx: Ctx,
  publisher: KTHandlerPublisher,
  kafkaTopicParams: {
    heartBeat: () => Promise<void>
    partition: number
    lastOffset: string | undefined
    resolveOffset?: (offset: string) => void
  }) => Promise<void>

export type KTHandler<Payload extends object, Ctx extends object> = {
  topic: KTTopicEvent<Payload> | KTTopicBatchEvent<Payload>
  run: KTRun<Payload, Ctx>
}

export type CtxWithKafka<T> = T & KafkaLogger

/**
 * @example
 * const TestExampleTopic = KTTopic<{
 *     field: number
 * }>({
 *   topic: KafkaTopicName.fromString('test.example'),
 *   numPartitions: 1,
 *   batchMessageSizeToConsume: 10,
 * })
 *
 * KTHandler({
 *   topic: TestExampleTopic,
 *   run: async (payload, ktMessageQueue) => {
 *     const data = payload[0]
 *
 *     if (!data) {
 *       return
 *     }
 *
 *     const newPayload = TestExampleTopic({
 *       field: data.field + 1,
 *     }, {
 *       messageKey: KafkaMessageKey.NULL,
 *     })
 *
 *     await ktMessageQueue.publishSingleMessage(newPayload)
 *   },
 * })
 */
export const KTHandler = <Payload extends object, Ctx extends object>(params: KTHandler<Payload, KafkaLogger & Ctx>): KTHandler<Payload, KafkaLogger & Ctx> => {
  return {
    topic: params.topic,
    run: params.run,
  }
}
