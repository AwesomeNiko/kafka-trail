// // import { KafkaTopicName, KTHandler, KTMessageQueue, KTTopic } from "./index.ts";
// // import { createTestDataTopicHandler, createTestTopic } from "./test-module-file.ts";
// //
// import { KafkaMessageKey } from "./libs/branded-types/kafka/index.ts";
// import { KTMessageQueue } from "./message-queue/index.ts";
// import { createTestBatchTopic, createTestTopic } from "./test-module-file.ts";
//
// const TestTopic = createTestBatchTopic()
//
// const payload = TestTopic([{
//   value: {
//     test: "1",
//   },
//   key: KafkaMessageKey.NULL,
// }])
//
// const messageQueue = new KTMessageQueue()
// await messageQueue.publishBatchMessages(payload)
//
// //
// //
// // messageQueue.registerHandlers([TestHandler])
// // await messageQueue.initTopics([TestTopic])