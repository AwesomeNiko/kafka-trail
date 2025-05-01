import type { KTMessageQueue } from "../message-queue/index.js";

import type { KTTopicEvent } from "./topic.js";

export type KTRun<Payload extends object, Ctx extends object> = (
  payload: Payload[],
  ctx: Ctx,
  publisher: Pick<KTMessageQueue<Ctx>, 'publishSingleMessage'>,
  kafkaTopicParams: {
    heartBeat: () => void
    partition: number
    lastOffset: string | undefined
    resolveOffset?: (offset: string) => void
  }) => Promise<void>

export type KTHandler<Payload extends object, Ctx extends object> = {
  topic: KTTopicEvent<Payload>
  run: KTRun<Payload, Ctx>
}

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
export const KTHandler = <Payload extends object, Ctx extends object>(params: KTHandler<Payload, Ctx>): KTHandler<Payload, Ctx> => {
  return {
    topic: params.topic,
    run: params.run,
  }
}