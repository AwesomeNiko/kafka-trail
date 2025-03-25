# Kafka-trail - MessageQueue Library

A Node.js library for managing message queues with Kafka, designed to simplify creating, using, and managing Kafka topics with producers and consumers.

### Based on [Kafkajs](https://kafka.js.org/)

---

## Features

- Fully in typescript
- Branded types
- Connect to Kafka brokers easily.
- Create or use existing Kafka topics with specified partitions.
- Initialize the message queue with minimal setup.
- Setup consumer handlers
- Compressing ([see](https://kafka.js.org/docs/1.12.0/producing#compression))
- Supports custom encoders/decoders.

---

## Installation

Install the library using npm or Yarn:

```bash
npm install kafka-trail
```
Or with Yarn:

```bash
yarn add kafka-trail
```

## Usage
Here’s an example of how to use the `kafka-trail` library in your project.
### For producer:

```typescript
// Define your Kafka broker URLs
import { KTTopic } from "kafka-trail";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "kafka-trail";
import { KTMessageQueue } from "kafka-trail";

const kafkaBrokerUrls = ["kafka://localhost:9092"];

// Create a MessageQueue instance
const messageQueue = new KTMessageQueue();

// Start producer
await messageQueue.initProducer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString('hostname'),
    connectionTimeout: 30_000,
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
```

### For consumer:
```typescript
import { KTHandler } from "kafka-trail";
import { KTTopic } from "kafka-trail";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "kafka-trail";
import { KTMessageQueue } from "kafka-trail";

const kafkaBrokerUrls = ["kafka://localhost:9092"];

// Create a MessageQueue instance
const messageQueue = new KTMessageQueue();

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

// Create topic handler
const testExampleTopicHandler = KTHandler({
  topic: TestExampleTopic,
  run: async (payload, ktMessageQueue) => {
    // Ts will show you right type for `payload` variable from `TestExampleTopic`

    const data = payload[0]

    if (!data) {
      return
    }

    const newPayload = TestExampleTopic({
      fieldForPayload: data.fieldForPayload + 1,
    }, {
      messageKey: KafkaMessageKey.NULL,
    })

    await ktMessageQueue.publishSingleData(newPayload)
  },
})

messageQueue.registerHandlers([
  testExampleTopicHandler,
])

// Start consumer
await messageQueue.initConsumer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString('hostname'),
    connectionTimeout: 30_000,
    consumerGroupId: 'consumer-group-id',
  },
})
```

### Destroying all will help you perform graceful shutdown
```javascript
const messageQueue = new MessageQueue();

process.on("SIGINT", async () => {
  await messageQueue.destroyAll()
});

process.on("SIGTERM", async () => {
  await messageQueue.destroyAll()
});
```

## Configurations

### Compression codec
By default, lib using LZ4 codec to compress and decompress data.
You can override it, by passing via `KTKafkaSettings` type. Be careful - producer and consumer should have same codec.
[Ref docs](https://kafka.js.org/docs/1.12.0/producing#compression). Example:

```typescript
import { KTMessageQueue } from "kafka-trail";
import { CompressionTypes } from "kafkajs";
import lz4 from "lz4";

// Instanciate messageQueue
const kafkaBrokerUrls = ["kafka://localhost:9092"];

const messageQueue = new KTMessageQueue();

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
```


### Data encoding / decoding
You can provide custom encoders / decoders for sending / receiving data. Example:

```typescript
type MyModel = {
  fieldForPayload: number
}

const TestExampleTopic = KTTopic<MyModel>({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
}, {
  encode: (data) => {
    return JSON.stringify(data)
  },
  decode: (data: string | Buffer) => {
    if (Buffer.isBuffer(data)) {
      data = data.toString()
    }

    return JSON.parse(data) as MyModel
  },
})
```



## Contributing
Contributions are welcome! If you’d like to improve this library:

1. Fork the repository.
2. Create a new branch.
3. Make your changes and submit a pull request.

## License
This library is open-source and licensed under the [MIT License](LICENSE).