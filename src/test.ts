// // import { KafkaTopicName, KTHandler, KTMessageQueue, KTTopic } from "./index.js";
// // import { createTestDataTopicHandler, createTestTopic } from "./test-module-file.js";
// //
// import { KafkaMessageKey } from "./libs/branded-types/kafka/index.js";
// import { KTMessageQueue } from "./message-queue/index.js";
// import { createTestBatchTopic, createTestTopic } from "./test-module-file.js";
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