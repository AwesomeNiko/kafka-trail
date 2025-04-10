import { context, SpanKind, trace } from "@opentelemetry/api";

import type { KTMessageQueue } from "../message-queue/index.js";

import type { KTTopicEvent } from "./topic.js";

export type KTRun<Payload extends object, Ctx extends object> = (
  payload: Payload[],
  ctx: Ctx,
  publisher: Pick<KTMessageQueue<Ctx>, 'publishSingleMessage'>,
  kafkaTopicParams: {
    partition?: number
    lastOffset?: string | undefined
    heartBeat?: () => void
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
    run: async (payload, ctx, publisher, kafkaTopicParams) => {
      const tracer = trace.getTracer(`kafka-trail`, '1.0.0')

      const { partition, lastOffset } = kafkaTopicParams

      const span = tracer.startSpan(`kafka-trail: handler ${params.topic.topicSettings.topic}`, {
        kind: SpanKind.CONSUMER,
        attributes: {
          'messaging.system': 'kafka',
          'messaging.destination': params.topic.topicSettings.topic,
          'messaging.kafka.partition': partition,
          'messaging.kafka.offset': lastOffset,
          'messaging.kafka.payload': JSON.stringify(payload),
        },
      })

      await context.with(trace.setSpan(context.active(), span), async () => {
        await params.run(payload, ctx, publisher, kafkaTopicParams)
        span.end()
      })
    },
  }
}