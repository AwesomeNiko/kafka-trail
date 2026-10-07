import { EventEmitter } from "node:events";

import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import type { Meter } from "@opentelemetry/api";
import type { Job, WorkerOptions } from "bullmq";
import pino from "pino";

import { KTJobHandler, type KTJobRun } from "../bullmq/consumer-handler.js";
import { CreateKTJob, type KTJobData } from "../bullmq/job.js";

type NativeJob = Job<KTJobData, void, string>
type Processor = (job: NativeJob, token?: string, signal?: AbortSignal) => Promise<void>
const redisInfo = jest.fn<() => Promise<string>>().mockResolvedValue("redis_version:7.4.2");
const workers: TestWorker[] = [];

class TestWorker extends EventEmitter {
  getBackend() {
    return { client: Promise.resolve({ info: redisInfo }) };
  }
  readonly run = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
  readonly waitUntilReady = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
  readonly pause = jest.fn<(doNotWaitActive?: boolean) => Promise<void>>().mockResolvedValue(undefined);
  readonly close = jest.fn<(force?: boolean) => Promise<void>>().mockResolvedValue(undefined);
  readonly cancelJob = jest.fn<(id: string, reason?: string) => boolean>().mockReturnValue(true);
  readonly cancelAllJobs = jest.fn<(reason?: string) => void>();

  constructor(readonly name: string, readonly processor: Processor, readonly options: WorkerOptions) {
    super();
    workers.push(this);
  }
}

class TestQueue extends EventEmitter {
  readonly close = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
}

class TestConnection extends EventEmitter {
  readonly client = Promise.resolve({ info: redisInfo });
  readonly close = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
}

jest.unstable_mockModule("bullmq", () => ({ Queue: TestQueue, Worker: TestWorker, RedisConnection: TestConnection }));
const { KTMessageQueue } = await import("../message-queue/index.js");

const logger = pino({ level: "silent" });
const context = { service: "unit", logger };
const config = { connection: { host: "localhost", port: 6379 } };
const Definition = CreateKTJob<{ value: number }>({ name: "unit.job", defaultJobOptions: { attempts: 3 } });
const createQueue = () => new KTMessageQueue({ ctx: () => context });

const createNativeJob = (state = "waiting") => {
  const payload = Definition({ value: 42 }, { jobId: "stable-id" });

  return {
    id: "stable-id",
    queueName: Definition.jobSettings.name,
    data: payload.data,
    opts: payload.options,
    attemptsMade: 1,
    getState: jest.fn<NativeJob["getState"]>().mockResolvedValue(state as Awaited<ReturnType<NativeJob["getState"]>>),
  } as unknown as jest.Mocked<NativeJob>;
};

const firstWorker = () => {
  const worker = workers[0];
  if (!worker) throw new Error("Worker was not initialized");

  return worker;
};

describe("BullMQ backend through KTMessageQueue", () => {
  beforeEach(() => {
    workers.length = 0;
    jest.clearAllMocks();
  });

  it("passes decoded payloads, context and the facade to the handler", async () => {
    const queue = createQueue();
    const run = jest.fn<KTJobRun<{ value: number }, typeof context>>().mockResolvedValue(undefined);
    const handler = KTJobHandler({ job: Definition, run });
    queue.registerJobHandlers([handler]);
    await queue.initBullMQConsumer(config);
    const job = createNativeJob();
    await firstWorker().processor(job);

    expect(run).toHaveBeenCalledTimes(1);
    expect(run).toHaveBeenCalledWith([{ value: 42 }], context, queue, { job, signal: expect.any(AbortSignal) });
    await queue.destroyAll();
  });

  it.each(["failed", "unknown"])("awaits final failure callbacks at shutdown for a %s job", async state => {
    const queue = createQueue();
    const entered = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    const onFinalFailure = jest.fn(async () => { entered.resolve(undefined); await release.promise; });
    const handler = KTJobHandler({ job: Definition, run: () => Promise.resolve(), onFinalFailure });
    queue.registerJobHandlers([handler]);
    await queue.initBullMQConsumer(config);
    const job = createNativeJob(state);
    firstWorker().emit("failed", job, new Error("terminal failure"));
    await new Promise(resolve => setImmediate(resolve));
    expect(onFinalFailure).toHaveBeenCalledTimes(1);
    await entered.promise;
    let stopped = false;
    const closing = queue.destroyAll().then(() => { stopped = true; });
    await new Promise(resolve => setImmediate(resolve));
    expect(stopped).toBe(false);
    release.resolve(undefined);
    await closing;
  });

  it("does not report final failure while a job is waiting for another attempt", async () => {
    const queue = createQueue();
    const onFinalFailure = jest.fn<() => Promise<void>>().mockResolvedValue(undefined);
    const handler = KTJobHandler({ job: Definition, run: () => Promise.resolve(), onFinalFailure });
    queue.registerJobHandlers([handler]);
    await queue.initBullMQConsumer(config);
    firstWorker().emit("failed", createNativeJob("delayed"), new Error("retrying"));
    await new Promise(resolve => setImmediate(resolve));

    expect(onFinalFailure).not.toHaveBeenCalled();
    await queue.destroyAll();
  });

  it("cancels only the jobs whose locks could not be renewed", async () => {
    const queue = createQueue();
    queue.registerJobHandlers([KTJobHandler({ job: Definition, run: () => Promise.resolve() })]);
    await queue.initBullMQConsumer(config);
    const worker = firstWorker();
    worker.emit("lockRenewalFailed", ["first", "second"]);

    expect(worker.cancelJob.mock.calls).toEqual([["first", "BullMQ lock renewal failed"], ["second", "BullMQ lock renewal failed"]]);
    await queue.destroyAll();
  });

  it("aborts active handlers and force-closes workers when the shutdown timeout expires", async () => {
    const queue = createQueue();
    const release = Promise.withResolvers<undefined>();
    let signal: AbortSignal | undefined;
    queue.registerJobHandlers([KTJobHandler({
      job: Definition,
      run: async (_payload, _ctx, _publisher, params) => {
        signal = params.signal;
        await release.promise;
      },
    })]);
    await queue.initBullMQConsumer(config);
    const worker = firstWorker();
    const processing = worker.processor(createNativeJob());
    await queue.destroyBullMQConsumer({ timeout: 10 });

    expect(signal?.aborted).toBe(true);
    expect(worker.close).toHaveBeenCalledWith(true);
    expect(worker.cancelAllJobs).toHaveBeenCalledTimes(1);
    release.resolve(undefined);
    await processing;
  });

  it("checks Redis without requiring a published job", async () => {
    const queue = createQueue();
    await queue.initBullMQProducer(config);
    await queue.checkBullMQConnection();

    expect(redisInfo).toHaveBeenCalledTimes(1);
    redisInfo.mockRejectedValueOnce(new Error("Redis unavailable"));
    await expect(queue.checkBullMQConnection()).rejects.toThrow("Redis unavailable");
    await queue.destroyAll();
  });

  it("propagates worker cancellation to the handler signal", async () => {
    const queue = createQueue();
    const controller = new AbortController();
    const reason = new Error("lock lost");
    const run = jest.fn<KTJobRun<{ value: number }, typeof context>>((_payload, _ctx, _publisher, params) => {
      controller.abort(reason);
      expect(params.signal.aborted).toBe(true);
      expect(params.signal.reason).toBe(reason);

      return Promise.resolve();
    });
    queue.registerJobHandlers([KTJobHandler({ job: Definition, run })]);
    await queue.initBullMQConsumer(config);
    await firstWorker().processor(createNativeJob(), undefined, controller.signal);
    await queue.destroyAll();
  });

  it("bounds shutdown even when a final failure callback does not finish", async () => {
    const queue = createQueue();
    const entered = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    let signal: AbortSignal | undefined;
    queue.registerJobHandlers([KTJobHandler({
      job: Definition,
      run: () => Promise.resolve(),
      onFinalFailure: async (_payload, _ctx, _publisher, params) => {
        signal = params.signal;
        entered.resolve(undefined);
        await release.promise;
      },
    })]);
    await queue.initBullMQConsumer(config);
    const worker = firstWorker();
    worker.emit("failed", createNativeJob("failed"), new Error("terminal"));
    await entered.promise;
    await queue.destroyAll({ timeout: 10 });

    expect(signal?.aborted).toBe(true);
    expect(worker.close).toHaveBeenCalledWith(true);
    release.resolve(undefined);
  });

  it("waits for final failure callbacks emitted while closing a worker", async () => {
    const queue = createQueue();
    const entered = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    queue.registerJobHandlers([KTJobHandler({
      job: Definition,
      run: () => Promise.resolve(),
      onFinalFailure: async () => {
        entered.resolve(undefined);
        await release.promise;
      },
    })]);
    await queue.initBullMQConsumer(config);
    const worker = firstWorker();
    worker.close.mockImplementation(() => {
      worker.emit("failed", createNativeJob("failed"), new Error("terminal"));

      return Promise.resolve();
    });
    let stopped = false;
    const stopping = queue.destroyAll().then(() => { stopped = true; });
    await entered.promise;
    await new Promise(resolve => setImmediate(resolve));

    expect(stopped).toBe(false);
    release.resolve(undefined);
    await stopping;
  });

  it("checks consumer-only Redis connections and rejects an uninitialized healthcheck", async () => {
    const queue = createQueue();
    await expect(queue.checkBullMQConnection()).rejects.toThrow("not initialized");
    queue.registerJobHandlers([KTJobHandler({ job: Definition, run: () => Promise.resolve() })]);
    await queue.initBullMQConsumer(config);
    await queue.checkBullMQConnection();

    expect(redisInfo).toHaveBeenCalledTimes(1);
    await queue.destroyAll();
  });

  it("records successful and failed attempts through the supplied meter and logger", async () => {
    const addMetric = jest.fn();
    const record = jest.fn();
    const meter = {
      createCounter: jest.fn(() => ({ add: addMetric })),
      createHistogram: jest.fn(() => ({ record })),
    } as unknown as Meter;
    const info = jest.spyOn(logger, "info");
    const errorLog = jest.spyOn(logger, "error");
    const queue = new KTMessageQueue({ ctx: () => context, meter });
    const error = new Error("failed attempt");
    let fail = false;
    queue.registerJobHandlers([KTJobHandler({ job: Definition, run: () => fail ? Promise.reject(error) : Promise.resolve() })]);
    await queue.initBullMQConsumer(config);
    await firstWorker().processor(createNativeJob());
    fail = true;
    await expect(firstWorker().processor(createNativeJob())).rejects.toBe(error);

    expect(addMetric.mock.calls).toEqual([
      [1, { "job.name": "unit.job", "job.outcome": "completed" }],
      [1, { "job.name": "unit.job", "job.outcome": "failed" }],
    ]);
    expect(record).toHaveBeenCalledTimes(2);
    expect(record).toHaveBeenCalledWith(expect.any(Number), { "job.name": "unit.job" });
    expect(info).toHaveBeenCalledWith(expect.objectContaining({ jobId: "stable-id", outcome: "completed" }), "BullMQ job attempt completed");
    expect(errorLog).toHaveBeenCalledWith(expect.objectContaining({ err: error, jobId: "stable-id" }), "BullMQ job attempt failed");
    await queue.destroyAll();
    info.mockRestore();
    errorLog.mockRestore();
  });
});
