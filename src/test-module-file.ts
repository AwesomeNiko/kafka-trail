// import { type KafkaMessageKey, KafkaTopicName, KTHandler, KTTopic, KTTopicBatch } from "./index.ts";
//
// const createTestBatchTopic = () => KTTopicBatch<{
//   value: {
//     test: string
//   },
//   key: KafkaMessageKey
// }[]>({
//   topic: KafkaTopicName.fromString('test'),
//   numPartitions: 1,
//   batchMessageSizeToConsume: 1,
// })
//
// const createTestTopic = () => KTTopic<{
//   test: string
// }>({
//   topic: KafkaTopicName.fromString('test_1'),
//   numPartitions: 1,
//   batchMessageSizeToConsume: 1,
// })
//
// const createTestDataTopicHandler = () => {
//   const TestTopic = createTestBatchTopic()
//
//   const TestHandler  = KTHandler({
//     topic: TestTopic,
//     run: async (payload, ctx, publisher, kafkaTopicParams) => {
//     },
//   })
//
//   return { TestHandler }
// }
//
// export {
//   createTestTopic,
//   createTestDataTopicHandler,
//   createTestBatchTopic,
// }