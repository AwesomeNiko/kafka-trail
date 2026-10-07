# Kafka-trail - MessageQueue Library

A Node.js library for managing Kafka messages and BullMQ jobs through one facade, with typed definitions and handlers.

### Based on [KafkaJS](https://kafka.js.org/) and [BullMQ](https://docs.bullmq.io/)

---

## Features

- Fully in typescript
- Branded types
- Connect to Kafka brokers easily.
- Create or use existing Kafka topics with specified partitions.
- Initialize the message queue with minimal setup.
- Setup consumer handlers
- Compressing ([see](https://kafka.js.org/docs/producing#compression))
- Supports custom encoders/decoders.
- Publish and consume BullMQ jobs with the same application context and publisher as Kafka handlers.
- BullMQ retries, delayed jobs, concurrency and job schedulers.

---

## Installation

Install the library using npm or Bun:

```bash
npm install @awesomeniko/kafka-trail
```
Or with Bun:

```bash
bun add @awesomeniko/kafka-trail
```

### Native LZ4 codec

The default `LZ4` codec is now backed by an internal `Rust + napi-rs` native binding instead of the `lz4` npm package.

- Library consumers should use the prebuilt native artifact shipped with the package.
- If you are developing this repository from source, run `bun run build` to build both the native module and TypeScript, or `bun run build:native` before `bun run test`.
- The native module source lives in `native/lz4`.

### Native build requirements

If you are building this repository from source, you need:

- `Node.js` (version specified in `.nvmrc`)
- `Bun` (version specified in `package.json` under `packageManager`)
- `Rust` toolchain via `rustup` with `cargo` and `rustc`
- On macOS, `Xcode Command Line Tools`

Local development flow:

```bash
bun install
bun run build
```

This setup does not require `python`, `node-gyp`, or a C++ Node addon toolchain.

### OpenTelemetry observability

Kafka and BullMQ share the same tracing and metrics implementation. Pass `tracingSettings.otel` to enable tracing and `meter` to enable metrics; they can be enabled independently.

`KTMessageQueue` no longer relies on its own runtime copy of `@opentelemetry/api`.

- If you do not pass `otel`, the library works as usual, but without tracing.
- If you want tracing, pass your application's OpenTelemetry API instance through `tracingSettings.otel`.
- This is useful when your app already uses its own observability package and you want `kafka-trail` to join the same trace context.

Example:

```typescript
import * as otel from "@opentelemetry/api";
import { KafkaClientId, KTMessageQueue } from "@awesomeniko/kafka-trail";

const kafkaBrokerUrls = ["localhost:19092"];

const messageQueue = new KTMessageQueue({
  meter: otel.metrics.getMeter("my-service"),
  tracingSettings: {
    otel,
    addPayloadToTrace: false,
  },
});

await messageQueue.initProducer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString("hostname"),
    connectionTimeout: 30_000,
  },
  pureConfig: {},
});
```

If your application uses a wrapper package like `observability`, pass the OpenTelemetry API object from there instead of importing a separate copy directly.

Both backends create `PRODUCER` spans for publication and `CONSUMER` spans for processing, named `kafka-trail: <kafka|bullmq> <publish|process> <topic-or-queue>`. They share `messaging.system`, `messaging.destination.name`, `messaging.operation.name`, `messaging.batch.message_count` and `messaging.message.body.size` attributes. Payloads are attached as `messaging.payload` only when `addPayloadToTrace` is enabled. Kafka also records partition and offset; BullMQ records job ID and attempts made. Failed operations record their exception and set the span status to `ERROR`. Spans close on success and failure.

Passing a `meter` enables these shared instruments:

| Metric | Type | Meaning |
| --- | --- | --- |
| `message_handler_executions` | Counter | Processing attempts, including retries. |
| `message_handler_duration` | Histogram, seconds | Duration of a processing attempt. |
| `messages_produced` | Counter | Messages in successful publication calls. |
| `messages_consumed` | Counter | Kafka records whose processing finished, or BullMQ jobs confirmed completed. |

The handler metrics use `messaging.system` (`kafka` or `bullmq`), `messaging.destination.name` (topic or queue) and `messaging.handler.outcome` (`completed` or `failed`). Kafka batches count once per handler invocation. A Kafka handler failure counts as `failed` even when subsequent DLQ publication succeeds. Publication spans do not increment handler metrics.

The throughput counters use `messaging.system` and `messaging.destination.name`. Kafka `messages_consumed` also includes `messaging.consumer.group.name`, so independent consumer groups can be compared to production separately. IDs, payloads and errors are excluded from their attributes.

`messages_produced` counts one message per single publication and the actual batch size after a successful bulk publication. A bulk call across BullMQ queues is counted separately for each successful queue operation. `messages_consumed` counts processed Kafka records, including tombstones and records successfully routed to DLQ; failed processing attempts are excluded. In batch mode it counts only the records handled and resolved in that batch. BullMQ consumption is counted on the worker's `completed` event, after Redis confirms completion, rather than when the handler returns or retries.

For Grafana with Prometheus, use `rate()` for messages per second and `increase()` for messages over an interval. Assuming your exporter uses normalized labels and the `_total` counter suffix, these queries compare one Kafka topic and consumer group across application replicas:

```promql
# Produced messages/second
sum(rate(messages_produced_total{messaging_system="kafka", messaging_destination_name="events"}[5m])) or vector(0)

# Consumed messages/second for one consumer group
sum(rate(messages_consumed_total{messaging_system="kafka", messaging_destination_name="events", messaging_consumer_group_name="my-group"}[5m])) or vector(0)
```

Subtract consumed rate from produced rate in Grafana. For BullMQ, select `messaging_system="bullmq"` and the queue name, without a consumer-group filter. A positive difference indicates an imbalance in flow, rather than an exact queue depth: Kafka records can be redelivered, BullMQ deduplication can accept a publication without adding a job, and scheduler-generated jobs bypass these publication calls. Use Kafka consumer lag or BullMQ queue counts to measure the actual backlog.

These instruments replace the previous BullMQ-only `job_handler_executions` and `job_handler_duration` metrics. Transport-specific payload trace attributes are now unified as `messaging.payload`; payload size is recorded as `messaging.message.body.size`.

If you prefer, you can also pass only the required OpenTelemetry fields explicitly:

```typescript
import {
  context,
  trace,
  SpanKind,
  SpanStatusCode,
} from "@opentelemetry/api";
import { KafkaClientId, KTMessageQueue } from "@awesomeniko/kafka-trail";

const kafkaBrokerUrls = ["localhost:19092"];

const messageQueue = new KTMessageQueue({
  tracingSettings: {
    otel: {
      context,
      trace,
      SpanKind,
      SpanStatusCode,
    },
    addPayloadToTrace: false,
  },
});

await messageQueue.initProducer({
  kafkaSettings: {
    brokerUrls: kafkaBrokerUrls,
    clientId: KafkaClientId.fromString("hostname"),
    connectionTimeout: 30_000,
  },
  pureConfig: {},
});
```

### Publishing native binaries

For local development, building the native module from source is enough. For npm distribution, the better long-term setup is to publish prebuilt `napi-rs` binaries per platform so library consumers do not need Rust installed.

Recommended direction:

- keep the Rust source in `native/lz4`
- build platform-specific `.node` artifacts in CI
- publish them as optional platform packages
- let the main package depend on those optional native packages

That is the standard `napi-rs` distribution model and avoids local compilation for end users.

## Usage

### Health checks

After initializing Kafka, use `checkKafkaConnection()` in your application's `/health` handler. It uses the existing KafkaJS clients: the producer's `admin.describeCluster()` when a producer is initialized, otherwise `consumer.describeGroup()`. Producer-only and consumer-only applications can use the same method. If both are initialized, the producer check is used and its errors are propagated.

```typescript
await messageQueue.checkKafkaConnection();
```

The method returns `Promise<void>` and propagates connection or authentication errors. It throws `Kafka is not initialized` when no Kafka producer or consumer is initialized. It checks broker availability, not whether handlers are processing messages. KafkaJS connection, request timeout and retry settings apply to the check.

For BullMQ, use `checkBullMQConnection()`. If your application uses both backends, await both checks in `/health`.

### BullMQ jobs

Each job definition has its own BullMQ queue. Register handlers before initializing the consumer. Kafka and BullMQ can run independently or together on the same `KTMessageQueue` instance.

```typescript
import { CreateKTJob, KTJobHandler, KTMessageQueue } from "@awesomeniko/kafka-trail";

const SendEmail = CreateKTJob<{ email: string }>({
  name: "send.email",
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
    removeOnComplete: { age: 3600 },
    removeOnFail: { age: 86400 },
  },
});

const mq = new KTMessageQueue({
  ctx: () => ({ sendEmail: async (email: string) => { /* application code */ } }),
});

mq.registerJobHandlers([
  KTJobHandler({
    job: SendEmail,
    options: { concurrency: 5 },
    run: async ([payload], ctx, publisher, { job, signal }) => {
      if (!payload) return;
      await ctx.sendEmail(payload.email);
      await job.updateProgress(100);
      // publisher can publish both Kafka messages and BullMQ jobs.
      // signal is aborted on lock loss or forced shutdown.
    },
  }),
]);

const redis = { connection: { host: "localhost", port: 6379 }, prefix: "my-service" };
await mq.initBullMQProducer(redis);
await mq.initBullMQConsumer(redis);

const job = await mq.publishJob(SendEmail({ email: "user@example.com" }, {
  jobId: "email-42",
  delay: 1000,
  meta: { traceId: "request-42" },
}));

await mq.publishBatchJobs([
  SendEmail({ email: "first@example.com" }),
  SendEmail({ email: "second@example.com" }),
]);

await mq.destroyAll();
```

Like `KTHandler`, `KTJobHandler.run` receives `(payloads, ctx, publisher, params)`. BullMQ delivers one job at a time, so `payloads` contains one decoded payload. Its fourth argument exposes the native BullMQ `job` (ID, attempts, progress) and cancellation `signal`. Throwing from a handler lets BullMQ retry the job according to `attempts` and `backoff`; exhausted jobs remain failed unless `removeOnFail` is configured.

`CreateKTJob(settings, codec)` accepts the same JSON, Zod, AJV and custom codecs as Kafka topic definitions. Metadata includes an automatically generated `traceId` when none is supplied. Job options override definition defaults; definition defaults override producer defaults. `jobId` uses BullMQ's deduplication behavior while that ID remains in the queue. Bulk publication is atomic within each queue; publication across different queues is independent.

Both initializers accept BullMQ options with Redis connection settings. Producer and consumer must use the same Redis database and `prefix`. Worker options on a handler override consumer defaults. `getBullMQQueue(name)` returns a native queue after its first publication or scheduler operation; `getBullMQWorker(name)` returns the running worker. The library owns and closes its Redis connections. `checkBullMQConnection()` checks initialized producer and worker connections, including before the first publication.

### BullMQ final failures

Handlers can define an optional `onFinalFailure` callback with the same decoded payload, context, publisher and job parameters as `run`, plus `error` in its fourth argument. It runs after a job becomes terminally failed, including when `removeOnFail` deletes it. Callback errors are logged. Typed callbacks require a successfully decoded payload.

```typescript
import { UnrecoverableJobError } from "@awesomeniko/kafka-trail";

const ProcessTask = CreateKTJob<{ id: string }>({
  name: "process.task",
  defaultJobOptions: { attempts: 5, backoff: { type: "exponential", delay: 1000 } },
});

mq.registerJobHandlers([
  KTJobHandler({
    job: ProcessTask,
    run: async ([payload], _ctx, _publisher, { signal }) => {
      if (!payload) return;
      signal.throwIfAborted();
      if (!payload.id) throw new UnrecoverableJobError("Task ID is required");
      // Process the task and pass signal to cancellable operations.
    },
    onFinalFailure: async ([payload], _ctx, _publisher, { error }) => {
      // Application code can store failure state or send a notification.
    },
  }),
]);
```

`UnrecoverableJobError` stops automatic retries immediately. State storage, outbox reconciliation and domain-specific recovery remain application responsibilities.

`destroyBullMQConsumer({ graceful: true, timeout: 30_000 })` stops taking new jobs and waits for active handlers and final failure callbacks. On timeout, or with `graceful: false`, active signals are aborted and workers are force-closed. Handlers must cooperate with cancellation; JavaScript execution cannot be forcibly terminated. `destroyAll()` accepts the same BullMQ shutdown options and closes producers after consumers. Its timeout applies to BullMQ; Kafka shutdown behavior is unchanged.

BullMQ attempts are logged with job name, ID, result and duration. Tracing and metrics use the shared OpenTelemetry configuration described above.

### BullMQ schedulers

Schedulers use the same job definition and handler as ordinary jobs. Initialize the BullMQ producer to manage schedules and the consumer to execute them. Upserting the same scheduler ID within a queue updates that schedule.

```typescript
import { CreateKTJob, KTJobHandler, KTMessageQueue } from "@awesomeniko/kafka-trail";

const RefreshCache = CreateKTJob<{ scope: string }>({ name: "refresh.cache" });

const mq = new KTMessageQueue({
  ctx: () => ({ refreshCache: async (scope: string) => { /* application code */ } }),
});

mq.registerJobHandlers([
  KTJobHandler({
    job: RefreshCache,
    run: async ([payload], ctx) => {
      if (!payload) return;
      await ctx.refreshCache(payload.scope);
    },
  }),
]);

const redis = { connection: { host: "localhost", port: 6379 }, prefix: "my-service" };
await mq.initBullMQProducer(redis);
await mq.initBullMQConsumer(redis);

await mq.upsertJobScheduler({
  schedulerId: "daily-refresh",
  repeat: { pattern: "0 0 * * *", tz: "UTC" },
  job: RefreshCache({ scope: "all" }),
});

await mq.upsertJobScheduler({
  schedulerId: "frequent-refresh",
  repeat: { every: 60_000 },
  job: RefreshCache({ scope: "recent" }),
});

await mq.removeJobScheduler({
  jobName: RefreshCache.jobSettings.name,
  schedulerId: "daily-refresh",
});
```

Scheduler templates cannot use `jobId`, `delay` or `deduplication`; BullMQ controls scheduled job IDs and timing. Closing a queue does not remove persistent jobs or schedules from Redis.

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
  batchMessageSizeToConsume: 10, // Works if batchConsuming = true
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
  messageKey: KafkaMessageKey.NULL, // If you don't want to specify message key
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
  batchMessageSizeToConsume: 10, // Works if batchConsuming = true
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
  batchMessageSizeToConsume: 10, // Works if batchConsuming = true
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
  run: async (payload, _, publisher, { resolveOffset }) => { // resolveOffset available for batchConsuming = true only
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

    if (resolveOffset) {
      // optional manual offset control when needed
    }
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
[Ref docs](https://kafka.js.org/docs/producing#compression). Example:

```typescript
import { KafkaClientId, KTMessageQueue } from "@awesomeniko/kafka-trail";
import { CompressionTypes } from "kafkajs";

const customLz4Codec = {
  compress(encoder: Buffer) {
    return encoder;
  },

  decompress<T>(buffer: Buffer) {
    return buffer as T;
  },
};

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
      codecFn: customLz4Codec,
    },
  },
  pureConfig: {},
})
```

The example above shows the shape of a custom codec. In a real codec implementation, `compress` and `decompress` should perform matching transformations.


### Data encoding / decoding
You can provide custom encoders / decoders for sending / receiving data. Example:

```typescript
import { CreateKTTopic, KafkaTopicName } from "@awesomeniko/kafka-trail";

type MyModel = {
  fieldForPayload: number
}

const { BaseTopic: TestExampleTopic } = CreateKTTopic<MyModel>({
  topic: KafkaTopicName.fromString('test.example'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10, // Works if batchConsuming = true
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

### AJV schema adapter
Use `createAjvCodecFromSchema` when your payload contract is JSON Schema and you want runtime validation via AJV.

```typescript
import { Ajv } from "ajv";
import { CreateKTTopic, KafkaTopicName, createAjvCodecFromSchema } from "@awesomeniko/kafka-trail";

type UserEvent = {
  id: number
}

const ajv = new Ajv()

const codec = createAjvCodecFromSchema<UserEvent>({
  ajv,
  schema: {
    $id: "user-event-id",
    title: "user-event",
    type: "object",
    properties: {
      id: {
        type: "number",
      },
    },
    required: ["id"],
    additionalProperties: false,
  },
})

const { BaseTopic } = CreateKTTopic<UserEvent>({
  topic: KafkaTopicName.fromString('test.ajv.topic'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ: false,
}, codec)
```

### Zod schema adapter
Use `createZodCodec` when schema is defined in application code with Zod.

```typescript
import { z } from "zod";
import { CreateKTTopic, KafkaTopicName, createZodCodec } from "@awesomeniko/kafka-trail";

type UserEvent = {
  id: number
}

const userEventSchema = z.object({
  id: z.number(),
}).meta({
  id: "user-event",
  schemaVersion: "1",
})

const codec = createZodCodec<UserEvent>(userEventSchema)

const { BaseTopic } = CreateKTTopic<UserEvent>({
  topic: KafkaTopicName.fromString('test.zod.topic'),
  numPartitions: 1,
  batchMessageSizeToConsume: 10,
  createDLQ: false,
}, codec)
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

### AWS Glue Schema Registry (with in-memory cache)
You can create a codec from AWS Glue Schema Registry and reuse it in `CreateKTTopic` / `CreateKTTopicBatch`.
The codec is initialized asynchronously (schema is fetched before codec creation), then works synchronously at runtime.

1) Create native AWS Glue adapter (IAM/default credentials):

```typescript
import { Ajv } from "ajv";
import {
  createAwsGlueCodec,
  createAwsGlueSchemaRegistryAdapter,
  clearAwsGlueSchemaCache,
} from "@awesomeniko/kafka-trail";

type UserEvent = {
  id: number
}

const ajv = new Ajv()
const glueAdapter = await createAwsGlueSchemaRegistryAdapter({
  region: "eu-central-1",
  preload: {
    schemas: [{
      registryName: "my-registry",
      schemaName: "user-events",
      schemaVersionId: "schema-version-id",
    }],
  },
})

const codec = await createAwsGlueCodec<UserEvent>({
  ajv,
  glue: glueAdapter,
  schema: {
    registryName: "my-registry",
    schemaName: "user-events",
    schemaVersionId: "schema-version-id",
  },
})

// clearAwsGlueSchemaCache() // optional manual cache reset
```

2) Static AWS keys (instead of IAM/default chain):

```typescript
const glueAdapter = await createAwsGlueSchemaRegistryAdapter({
  region: "eu-central-1",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
    sessionToken: process.env.AWS_SESSION_TOKEN,
  },
})
```

3) Zod mode (same Glue adapter, no manual `getSchema`):

```typescript
import { z } from "zod";
import { createAwsGlueCodec, createAwsGlueSchemaRegistryAdapter } from "@awesomeniko/kafka-trail";

type UserEvent = {
  id: number
}

const glueAdapter = await createAwsGlueSchemaRegistryAdapter({
  region: "eu-central-1",
})

const codec = await createAwsGlueCodec<UserEvent>({
  validator: "zod",
  glue: glueAdapter,
  schema: {
    registryName: "my-registry",
    schemaName: "user-events",
  },
  zodSchemaFactory: ({ schema }) => {
    // Build your zod schema using Glue JSON schema payload
    return z.object({
      id: z.number(),
    })
  },
})
```

Notes:
- cache is in-memory and enabled by default per process;
- cache key is based on registry + schema identifiers and is shared for AJV/Zod modes;
- unsupported Glue data formats (for example AVRO/PROTOBUF) are rejected in this version;
- call `glueAdapter.destroy()` on shutdown if you want to close the AWS SDK client explicitly;
- call `clearAwsGlueSchemaCache()` if you need to invalidate cached schemas manually.

### Deprecated topic creators
`KTTopic(...)` and `KTTopicBatch(...)` were deprecated in previous version.
Current versions intentionally throw runtime errors if these APIs are invoked (for teams that have not migrated yet).
It's planned to be removed in the next version:
- `Deprecated. use CreateKTTopic(...)`
- `Deprecated. use CreateKTTopicBatch(...)`

## Testing

Tests use Jest. Run the full suite with `bun run test`; `bun test` starts Bun's built-in test runner and does not load the Jest configuration or integration setup.

Run unit tests with `bun run test:unit`.

Integration tests use [Testcontainers Redpanda](https://node.testcontainers.org/modules/redpanda/) and [Testcontainers Redis](https://node.testcontainers.org/modules/redis/).
They start one temporary broker and one Redis container on dynamically assigned ports for the test run and remove them afterward.
Docker must already be running. Separately started Kafka, Redpanda or Redis services are not required.

For Colima, set its existing Docker socket before running tests ([runtime setup](https://node.testcontainers.org/supported-container-runtimes/#colima)):

```bash
export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

```bash
bun run build:native
bun run test:int
```

Integration tests use the real native LZ4 codec. Unit tests use a mock codec.
BullMQ integration tests cover publication through handlers, bulk jobs, retries, final failure callbacks, unrecoverable failures, lock-loss cancellation, delays, schema validation, schedulers, graceful shutdown and Kafka → BullMQ → Kafka delivery.
The default timeout for each integration test is 30 seconds; set `KAFKA_INT_TEST_TIMEOUT_MS` to override it.
CI builds the native codec and runs both test suites.

## Contributing
Contributions are welcome! If you’d like to improve this library:

1. Fork the repository.
2. Create a new branch.
3. Make your changes and submit a pull request.

## License
This library is open-source and licensed under the [MIT License](LICENSE).
