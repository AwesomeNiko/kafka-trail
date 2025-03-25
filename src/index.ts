import { CompressionTypes } from "kafkajs";
import lz4 from "lz4";

import { KTTopic } from "./kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "./libs/branded-types/kafka/index.js";
import { KTMessageQueue } from "./message-queue/index.js";

const kafkaBrokerUrls = ["kafka://localhost:9092"];

// Create a MessageQueue instance
const messageQueue = new KTMessageQueue();

// Start producer
await messageQueue.initProducer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString('hostname'),
    connectionTimeout: 30_000,
    compressionCodec: {
      codecType: CompressionTypes.LZ4,
      codecFn: {
        compress(encoder: Buffer) {
          return lz4.encode(encoder);
        },

        decompress<T>(buffer: Buffer) {
          return lz4.decode(buffer) as T;
        },
      },
    },
  },
})

// Create topic fn
const TestExampleTopic = KTTopic<{
    fieldForPayload: number
  }>({
    topic: KafkaTopicName.fromString('test.example'),
    numPartitions: 1,
    batchMessageSizeToConsume: 10,
  })

// Create or use topic
await messageQueue.initTopics([
  TestExampleTopic,
])

// Use publishSingleData method to publish message
const payload = TestExampleTopic({
  fieldForPayload: 1,
}, {
  messageKey: KafkaMessageKey.NULL, //If you don't want to specify message key
})

await messageQueue.publishSingleData(payload)
