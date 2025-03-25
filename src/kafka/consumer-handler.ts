import type { KTMessageQueue } from "../message-queue/index.js";

import type { KTTopicEvent } from "./topic.js";

export type KTHandler<Payload extends object> = {
  topic: KTTopicEvent<Payload>
  run: (payload: Payload[], ktMessageQueue: KtMessageQueuePublisher) => Promise<void>
}
type KtMessageQueuePublisher = Pick<KTMessageQueue, 'publishSingleData'>

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
 *     await ktMessageQueue.publishSingleData(newPayload)
 *   },
 * })
 */
export const KTHandler = <Payload extends object>(params: {
    topic: KTTopicEvent<Payload>
    run: (payload: Payload[], ktMessageQueue: KtMessageQueuePublisher) => Promise<void>
}) => {
  return {
    run: params.run,
    topic: params.topic,
  }
}