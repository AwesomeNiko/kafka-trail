import { randomUUID } from "node:crypto";

import { describe, expect, it, jest } from "@jest/globals";
import pino from "pino";
import { z } from "zod";

import { KTJobHandler, type KTJobRun } from "../bullmq/consumer-handler.js";
import { CreateKTJob } from "../bullmq/job.js";
import { BullMQProducerNotInitializedError, NoJobHandlersError } from "../custom-errors/bullmq-errors.js";
import { KTHandler } from "../kafka/consumer-handler.js";
import { CreateKTTopic } from "../kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "../libs/branded-types/kafka/index.js";
import { createZodCodec } from "../libs/schema/adapters/zod-adapter.js";
import { KTMessageQueue } from "../message-queue/index.js";

import { getIntTestConfig, INTEGRATION_TEST_TIMEOUT_MS } from "./infrastructure/kafka.js";
import { getRedisTestConfig } from "./infrastructure/redis.js";

type Payload = { value: number }
type Context = { service: string, logger: pino.Logger }

const context: Context = { service: "integration", logger: pino({ level: "silent" }) };
const createQueue = () => new KTMessageQueue({ ctx: () => context });
const createJob = () => CreateKTJob<Payload>({ name: `test.job.${randomUUID()}` });

const waitFor = async (condition: () => boolean | Promise<boolean>): Promise<void> => {
  const deadline = Date.now() + 10_000;

  while (!await condition()) {
    if (Date.now() > deadline) {
      throw new Error("Timeout waiting for BullMQ integration result");
    }

    await new Promise(resolve => setTimeout(resolve, 20));
  }
};

describe("BullMQ through KTMessageQueue", () => {
  it("publishes and delivers a typed payload to a consumer with the same context and publisher", async () => {
    const producer = createQueue();
    const consumer = createQueue();
    const Job = createJob();
    const run = jest.fn<KTJobRun<Payload, Context>>().mockResolvedValue(undefined);
    const handler = KTJobHandler({ job: Job, run });
    consumer.registerJobHandlers([handler]);

    try {
      await producer.initBullMQProducer(getRedisTestConfig());
      await consumer.initBullMQConsumer(getRedisTestConfig());
      const published = await producer.publishJob(Job({ value: 42 }, { meta: { traceId: "trace-42" } }));
      await waitFor(async () => await published.getState() === "completed");

      expect(run).toHaveBeenCalledTimes(1);
      expect(run.mock.calls[0]?.[0]).toEqual([{ value: 42 }]);
      expect(run.mock.calls[0]?.[1]).toBe(context);
      expect(run.mock.calls[0]?.[2]).toBe(consumer);
      expect(run.mock.calls[0]?.[3].job.id).toBe(published.id);
      expect(run.mock.calls[0]?.[3].job.data.meta.traceId).toBe("trace-42");
      expect(consumer.getRegisteredJobHandler(Job.jobSettings.name)).toBe(handler);
      expect(consumer.getBullMQWorker(Job.jobSettings.name)).toBeDefined();
      expect(producer.getBullMQQueue(Job.jobSettings.name)).toBeDefined();
    } finally {
      await consumer.destroyAll();
      await producer.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("publishes bulk jobs across queues and processes each job separately", async () => {
    const queue = createQueue();
    const First = createJob();
    const Second = CreateKTJob<{ text: string }>({ name: `test.text.${randomUUID()}` });
    const received: number[] = [];
    const firstHandler = KTJobHandler({
      job: First,
      run: async (payload) => {
        received.push(...payload.map(value => value.value));
        await Promise.resolve();
      },
    });
    queue.registerJobHandlers([
      firstHandler,
      KTJobHandler({
        job: Second,
        run: async ([payload]) => {
          if (payload) received.push(Number(payload.text));

          await Promise.resolve();
        },
      }),
    ]);

    try {
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer({ ...getRedisTestConfig(), concurrency: 2 });
      await queue.publishBatchJobs([First({ value: 1 }), First({ value: 2 }), Second({ text: "3" })]);
      await waitFor(() => received.length === 3);
      expect(received.sort()).toEqual([1, 2, 3]);
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("respects delayed jobs and BullMQ jobId deduplication", async () => {
    const queue = createQueue();
    const Job = createJob();
    const run = jest.fn<KTJobRun<Payload, Context>>().mockResolvedValue(undefined);
    queue.registerJobHandlers([KTJobHandler({ job: Job, run })]);

    try {
      await queue.initBullMQProducer(getRedisTestConfig());
      const job = await queue.publishJob(Job({ value: 1 }, { jobId: "same-id", delay: 300 }));
      await queue.publishJob(Job({ value: 2 }, { jobId: "same-id", delay: 300 }));
      expect(await job.getState()).toBe("delayed");
      await queue.initBullMQConsumer(getRedisTestConfig());
      await waitFor(async () => await job.getState() === "completed");
      expect(run).toHaveBeenCalledTimes(1);
      expect(run.mock.calls[0]?.[0]).toEqual([{ value: 1 }]);
      expect(Date.now() - job.timestamp).toBeGreaterThanOrEqual(300);
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("retries handler failures and retains failed jobs after attempts are exhausted", async () => {
    const queue = createQueue();
    const Job = CreateKTJob<Payload>({
      name: `test.retry.${randomUUID()}`,
      defaultJobOptions: { attempts: 3, backoff: { type: "exponential", delay: 20 } },
    });
    const attempts: number[] = [];
    queue.registerJobHandlers([KTJobHandler({
      job: Job,
      run: async ([payload], _ctx, _publisher, { job }) => {
        await Promise.resolve();

        if (payload?.value === 1) {
          attempts.push(job.attemptsMade);
          if (job.attemptsMade === 0) throw new Error("temporary failure");
        } else {
          throw new Error("permanent failure");
        }
      },
    })]);

    try {
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer(getRedisTestConfig());
      const retry = await queue.publishJob(Job({ value: 1 }));
      const failure = await queue.publishJob(Job({ value: 2 }, { attempts: 2 }));
      await waitFor(async () => await retry.getState() === "completed" && await failure.getState() === "failed");
      expect(attempts).toEqual([0, 1]);
      const stored = await queue.getBullMQQueue(Job.jobSettings.name)?.getJob(failure.id ?? "");
      expect(stored?.attemptsMade).toBe(2);
      expect(stored?.failedReason).toBe("permanent failure");
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("validates received jobs with the configured schema before invoking the handler", async () => {
    const queue = createQueue();
    const Job = CreateKTJob({ name: `test.schema.${randomUUID()}` }, createZodCodec(z.object({ value: z.number() })));
    const run = jest.fn<KTJobRun<Payload, Context>>().mockResolvedValue(undefined);
    queue.registerJobHandlers([KTJobHandler({ job: Job, run })]);

    try {
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer(getRedisTestConfig());
      const valid = await queue.publishJob(Job({ value: 42 }));
      await waitFor(async () => await valid.getState() === "completed");
      const nativeQueue = queue.getBullMQQueue(Job.jobSettings.name);
      if (!nativeQueue) throw new Error("Native queue is required");
      const invalid = await nativeQueue.add(Job.jobSettings.name, { message: '{"value":"bad"}', meta: {} });
      await waitFor(async () => await invalid.getState() === "failed");
      expect(run).toHaveBeenCalledTimes(1);
      const stored = await nativeQueue.getJob(invalid.id ?? "");
      expect(stored?.failedReason).toContain("validation");
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("upserts interval and cron schedulers and executes scheduled jobs", async () => {
    const queue = createQueue();
    const Job = createJob();
    const received: number[] = [];
    queue.registerJobHandlers([KTJobHandler({
      job: Job,
      run: async ([payload]) => {
        if (payload) received.push(payload.value);
        await Promise.resolve();
      },
    })]);

    try {
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.upsertJobScheduler({ schedulerId: "interval", repeat: { every: 100 }, job: Job({ value: 1 }) });
      await queue.upsertJobScheduler({ schedulerId: "interval", repeat: { every: 100 }, job: Job({ value: 2 }) });
      await queue.upsertJobScheduler({ schedulerId: "cron", repeat: { pattern: "0 0 * * *", tz: "UTC" }, job: Job({ value: 3 }) });
      const nativeQueue = queue.getBullMQQueue(Job.jobSettings.name);
      expect(await nativeQueue?.getJobSchedulersCount()).toBe(2);
      await queue.initBullMQConsumer(getRedisTestConfig());
      await waitFor(() => received.filter(value => value === 2).length >= 2);
      expect(await queue.removeJobScheduler({ jobName: Job.jobSettings.name, schedulerId: "interval" })).toBe(true);
      expect(await queue.removeJobScheduler({ jobName: Job.jobSettings.name, schedulerId: "cron" })).toBe(true);
      expect(await nativeQueue?.getJobSchedulersCount()).toBe(0);
      await expect(queue.upsertJobScheduler({ schedulerId: "invalid", repeat: { every: 100 }, job: Job({ value: 1 }, { jobId: "custom" }) })).rejects.toThrow("scheduler templates");
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("runs Kafka and BullMQ together and lets handlers publish across backends", async () => {
    const queue = createQueue();
    const Job = createJob();
    const suffix = randomUUID();
    const { BaseTopic: Topic } = CreateKTTopic<Payload>({
      topic: KafkaTopicName.fromString(`test.bridge.${suffix}`),
      numPartitions: 1,
      batchMessageSizeToConsume: 1,
      createDLQ: false,
    });
    const received: number[] = [];
    queue.registerHandlers([KTHandler({
      topic: Topic,
      run: async ([payload], _ctx, publisher) => {
        if (payload?.value === 1) {
          await publisher.publishJob(Job({ value: 2 }));
        } else if (payload) {
          received.push(payload.value);
        }
      },
    })]);
    queue.registerJobHandlers([KTJobHandler({
      job: Job,
      run: async ([payload], _ctx, publisher) => {
        if (payload) {
          await publisher.publishSingleMessage(Topic({ value: payload.value + 1 }, { messageKey: KafkaMessageKey.NULL, meta: {} }));
        }
      },
    })]);
    const config = {
      kafkaSettings: {
        brokerUrls: [getIntTestConfig().brokerUrl],
        clientId: KafkaClientId.fromString(`bridge-${suffix}`),
        connectionTimeout: 10_000,
        consumerGroupId: `bridge-${suffix}`,
      },
      pureConfig: {},
    };

    try {
      await queue.initProducer(config);
      await queue.initTopics([Topic]);
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer(getRedisTestConfig());
      await queue.initConsumer(config);
      await queue.publishSingleMessage(Topic({ value: 1 }, { messageKey: KafkaMessageKey.NULL, meta: {} }));
      await waitFor(() => received.length === 1);
      expect(received).toEqual([3]);
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("keeps the producer available to active handlers during graceful shutdown", async () => {
    const queue = createQueue();
    const Job = createJob();
    let release: () => void = () => undefined;
    const gate = new Promise<void>(resolve => { release = resolve; });
    let started = false;
    let publishedFromHandler = false;
    queue.registerJobHandlers([KTJobHandler({
      job: Job,
      run: async ([payload], _ctx, publisher) => {
        if (payload?.value === 1) {
          started = true;
          await gate;
          await publisher.publishJob(Job({ value: 2 }));
          publishedFromHandler = true;
        }
      },
    })]);

    try {
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer(getRedisTestConfig());
      await queue.publishJob(Job({ value: 1 }));
      await waitFor(() => started);
      const closing = queue.destroyAll();
      release();
      await closing;
      expect(publishedFromHandler).toBe(true);
      expect(queue.getBullMQWorker(Job.jobSettings.name)).toBeUndefined();
      expect(queue.getBullMQQueue(Job.jobSettings.name)).toBeUndefined();
    } finally {
      release();
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);

  it("requires explicit initialization and allows reinitialization after shutdown", async () => {
    const queue = createQueue();
    const Job = createJob();

    try {
      await expect(queue.publishJob(Job({ value: 1 }))).rejects.toBeInstanceOf(BullMQProducerNotInitializedError);
      await expect(queue.initBullMQConsumer(getRedisTestConfig())).rejects.toBeInstanceOf(NoJobHandlersError);
      queue.registerJobHandlers([KTJobHandler({ job: Job, run: () => Promise.resolve() })]);
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer(getRedisTestConfig());
      await expect(queue.initBullMQProducer(getRedisTestConfig())).rejects.toThrow("already initialized");
      await expect(queue.initBullMQConsumer(getRedisTestConfig())).rejects.toThrow("already initialized");
      await queue.destroyAll();
      await queue.initBullMQProducer(getRedisTestConfig());
      await queue.initBullMQConsumer(getRedisTestConfig());
      const job = await queue.publishJob(Job({ value: 42 }));
      await waitFor(async () => await job.getState() === "completed");
    } finally {
      await queue.destroyAll();
    }
  }, INTEGRATION_TEST_TIMEOUT_MS);
});
