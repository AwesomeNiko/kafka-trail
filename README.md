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
npm install @awesomeniko/kafka-trail
```
Or with Yarn:

```bash
yarn add @awesomeniko/kafka-trail
```

## Usage
Here’s an example of how to use the `@awesomeniko/kafka-trail` library in your project.
### If you want only producer:

```typescript
// Define your Kafka broker URLs
import { 
  CreateKTTopic, 
  KafkaClientId, 
  KafkaMessageKey, 
  KafkaTopicName, 
  KTMessageQueue 
} from "@awesomeniko/kafka-trail";

const kafkaBrokerUrls = ["localhost:19092"];

// Create a MessageQueue instance
const messageQueue = new KTMessageQueue();

// Start producer
await messageQueue.initProducer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString('hostname'),
    connectionTimeout: 30_000,
  },
  pureConfig: {},
})

// Create topic fn
const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
  fieldForPayload: number
}>({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10, // Works is batchConsuming = true
  createDLQ: false,
})

// Create or use topic
await messageQueue.initTopics([
  TestExampleTopic,
])

// Use publishSingleMessage method to publish message
const payload = TestExampleTopic({
  fieldForPayload: 1,
}, {
  messageKey: KafkaMessageKey.NULL, //If you don't want to specify message key
  meta: {},
})

await messageQueue.publishSingleMessage(payload)
```

### If you want consumer only:
```typescript
import type pino from "pino";

import { 
  KTHandler, 
  CreateKTTopic, 
  KafkaClientId, 
  KafkaMessageKey, 
  KafkaTopicName, 
  KTMessageQueue 
} from "@awesomeniko/kafka-trail";

// Another dependency example
class DatabaseClass {
  #client: string
  constructor () {
    this.#client = 'test-client'
  }

  getClient() {
    return this.#client
  }
}

const dbClass = new DatabaseClass()

const kafkaBrokerUrls = ["localhost:19092"];

// Create a MessageQueue instance
const messageQueue = new KTMessageQueue({
  // If you want pass context available in handler
  ctx: () => {
    return {
      dbClass,
    }
  },
});

export const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
  fieldForPayload: number
}>({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10, // Works is batchConsuming = true
  createDLQ: false,
})

// Create topic handler
const testExampleTopicHandler = KTHandler({
  topic: TestExampleTopic,
  run: async (payload, ctx: {logger: pino.Logger, dbClass: typeof dbClass}) => {
    // Ts will show you right type for `payload` variable from `TestExampleTopic`
    // Ctx passed from KTMessageQueue({ctx: () => {...}})

    const [data] = payload

    if (!data) {
      return Promise.resolve()
    }

    const logger = ctx.logger.child({
      payload: data.fieldForPayload,
    })

    logger.info(dbClass.getClient())

    return Promise.resolve()
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
    batchConsuming: true // default false
  },
  pureConfig: {},
})
```


### For both consumer and producer:
```typescript
import { 
  KTHandler, 
  CreateKTTopic, 
  KafkaClientId, 
  KafkaMessageKey, 
  KafkaTopicName, 
  KTMessageQueue 
} from "@awesomeniko/kafka-trail";

const kafkaBrokerUrls = ["localhost:19092"];

// Create a MessageQueue instance
const messageQueue = new KTMessageQueue();

// Create topic fn
const { BaseTopic: TestExampleTopic } = CreateKTTopic<{
  fieldForPayload: number
}>({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10, // Works is batchConsuming = true
  createDLQ: false,
})

// Required, because inside handler we are going to publish data
await messageQueue.initProducer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString('hostname'),
    connectionTimeout: 30_000,
  },
  pureConfig: {},
})

// Create or use topic
await messageQueue.initTopics([
  TestExampleTopic,
])

// Create topic handler
const testExampleTopicHandler = KTHandler({
  topic: TestExampleTopic,
  run: async (payload, _, publisher, { heartbeat, partition, lastOffset, resolveOffset }) => { // resolveOffset available for batchConsuming = true only
    // Ts will show you right type for `payload` variable from `TestExampleTopic`

    const [data] = payload

    if (!data) {
      return Promise.resolve()
    }

    const newPayload = TestExampleTopic({
      fieldForPayload: data.fieldForPayload + 1,
    }, {
      messageKey: KafkaMessageKey.NULL,
      meta: {},
    })

    await publisher.publishSingleMessage(newPayload)
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
    batchConsuming: true // default false
  },
  pureConfig: {},
})
```

### Destroying all will help you perform graceful shutdown
```javascript
const messageQueue = new KTMessageQueue();

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
import { KafkaClientId, KTMessageQueue } from "@awesomeniko/kafka-trail";
import { CompressionTypes } from "kafkajs";
import lz4 from "lz4";

// Instanciate messageQueue
const kafkaBrokerUrls = ["localhost:19092"];

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
  pureConfig: {},
})
```


### Data encoding / decoding
You can provide custom encoders / decoders for sending / receiving data. Example:

```typescript
type MyModel = {
  fieldForPayload: number
}

const { BaseTopic: TestExampleTopic } = CreateKTTopic<MyModel>({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10, // Works is batchConsuming = true
  createDLQ: false,
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

### Sending batch messages
You can send batch messages instead of sending one by one, but it required a little different usage. Example:

```javascript
// Create topic fn
const { BaseTopic: TestExampleTopic } = CreateKTTopicBatch({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ: false,
})

// Create or use topic
await messageQueue.initTopics([
  TestExampleTopic,
])

// Use publishBatchMessages method to publish message
const payload = TestExampleTopic([{
  value: {
    test: 1,
    test2: 2,
  },
  key: '1',
}, {
  value: {
    test: 3,
    test2: 4,
  },
  key: '2',
}, {
  value: {
    test: 5,
    test2: 6,
  },
  key: '3',
}])

await messageQueue.publishBatchMessages(payload)
```

### Dead Letter Queue (DLQ)
Automatically route failed messages to DLQ topics for later analysis and reprocessing.

`initProducer` must be called before `initConsumer` when at least one registered handler uses `createDLQ: true`, otherwise `ProducerInitRequiredForDLQError` is thrown.

```typescript
// DLQ topics are automatically created with 'dlq.' prefix
const { BaseTopic: TestExampleTopic, DLQTopic: DLQTestExampleTopic } = CreateKTTopic<MyPayload>({
  topic: KafkaTopicName.fromString('my.topic'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ: true, // Enables DLQ
})

// Create or use topic
await messageQueue.initTopics([
  TestExampleTopic,
  DLQTestExampleTopic
])

// Failed messages automatically sent to: dlq.my.topic with next model:
{
  originalOffset: "123",
  originalTopic: "user.events",
  originalPartition: 0,
  key: '["user123","user456"]',
  value: [
    { userId: "user123", action: "login" },
    { userId: "user456", action: "logout" }
  ],
  errorMessage: "Database connection failed",
  failedAt: 1703123456789
}
```

### Deprecated topic creators
`KTTopic(...)` and `KTTopicBatch(...)` are kept only for backward compatibility and throw runtime errors:
- `Deprecated. use CreateKTTopic(...)`
- `Deprecated. use CreateKTTopicBatch(...)`

## Contributing
Contributions are welcome! If you’d like to improve this library:

1. Fork the repository.
2. Create a new branch.
3. Make your changes and submit a pull request.

## License
This library is open-source and licensed under the [MIT License](LICENSE).
