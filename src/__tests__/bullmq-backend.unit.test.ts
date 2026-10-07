import { EventEmitter } from "node:events";

import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import type { Job, WorkerOptions } from "bullmq";
import pino from "pino";

import { KTJobHandler, type KTJobRun } from "../bullmq/consumer-handler.js";
import { CreateKTJob, type KTJobData } from "../bullmq/job.js";

import { createMeterMocks, createTracingMocks } from "./mocks/observability.js";

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
  readonly add = jest.fn<() => Promise<NativeJob>>().mockImplementation(() => Promise.resolve(createNativeJob()));
  readonly addBulk = jest.fn<() => Promise<NativeJob[]>>().mockResolvedValue([]);
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
    const { add: addMetric, record, meter } = createMeterMocks();
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
      [1, { "messaging.system": "bullmq", "messaging.destination.name": "unit.job", "messaging.handler.outcome": "completed" }],
      [1, { "messaging.system": "bullmq", "messaging.destination.name": "unit.job", "messaging.handler.outcome": "failed" }],
    ]);
    expect(record).toHaveBeenCalledTimes(2);
    expect(record).toHaveBeenCalledWith(expect.any(Number), { "messaging.system": "bullmq", "messaging.destination.name": "unit.job", "messaging.handler.outcome": "failed" });
    expect(info).toHaveBeenCalledWith(expect.objectContaining({ jobId: "stable-id", outcome: "completed" }), "BullMQ job attempt completed");
    expect(errorLog).toHaveBeenCalledWith(expect.objectContaining({ err: error, jobId: "stable-id" }), "BullMQ job attempt failed");
    await queue.destroyAll();
    info.mockRestore();
    errorLog.mockRestore();
  });

  it("counts BullMQ single and bulk publication and excludes rejected requests", async () => {
    const metrics = createMeterMocks();
    const queue = new KTMessageQueue({ ctx: () => context, meter: metrics.meter });
    const Other = CreateKTJob<{ value: number }>({ name: "other.job" });
    await queue.initBullMQProducer(config);
    await queue.publishJob(Definition({ value: 1 }));
    await queue.publishBatchJobs([Definition({ value: 2 }), Definition({ value: 3 }), Other({ value: 4 })]);
    await queue.publishBatchJobs([]);
    const nativeQueue = queue.getBullMQQueue(Definition.jobSettings.name);
    if (!nativeQueue) throw new Error("Queue was not initialized");
    const error = new Error("Redis unavailable");
    jest.spyOn(nativeQueue, "add").mockRejectedValueOnce(error);
    await expect(queue.publishJob(Definition({ value: 5 }))).rejects.toBe(error);
    jest.spyOn(nativeQueue, "addBulk").mockRejectedValueOnce(error);
    await expect(queue.publishBatchJobs([Definition({ value: 6 })])).rejects.toBe(error);

    expect(metrics.produced.mock.calls).toEqual([
      [1, { "messaging.system": "bullmq", "messaging.destination.name": "unit.job" }],
      [2, { "messaging.system": "bullmq", "messaging.destination.name": "unit.job" }],
      [1, { "messaging.system": "bullmq", "messaging.destination.name": "other.job" }],
    ]);
    expect(metrics.consumed).not.toHaveBeenCalled();
    await queue.destroyAll();
  });

  it("counts consumption on BullMQ completion rather than processor attempts", async () => {
    const metrics = createMeterMocks();
    const queue = new KTMessageQueue({ ctx: () => context, meter: metrics.meter });
    const error = new Error("retrying");
    const run = jest.fn<KTJobRun<{ value: number }, typeof context>>().mockRejectedValueOnce(error).mockResolvedValue(undefined);
    queue.registerJobHandlers([KTJobHandler({ job: Definition, run })]);
    await queue.initBullMQConsumer(config);
    const worker = firstWorker();
    const job = createNativeJob();
    await expect(worker.processor(job)).rejects.toBe(error);
    worker.emit("failed", job, error, "active");
    expect(metrics.consumed).not.toHaveBeenCalled();

    await worker.processor(job);
    expect(metrics.consumed).not.toHaveBeenCalled();
    worker.emit("completed", job, undefined, "active");
    expect(metrics.consumed.mock.calls).toEqual([[1, { "messaging.system": "bullmq", "messaging.destination.name": "unit.job" }]]);
    expect(metrics.add).toHaveBeenCalledTimes(2);
    await queue.destroyAll();
  });

  it.each([false, true])("traces BullMQ publication and processing with optional payloads (payload=%s)", async addPayloadToTrace => {
    const tracing = createTracingMocks();
    const metrics = createMeterMocks();
    const queue = new KTMessageQueue({ ctx: () => context, meter: metrics.meter, tracingSettings: { otel: tracing.otel, addPayloadToTrace } });
    const error = new Error("handler failed");
    queue.registerJobHandlers([KTJobHandler({ job: Definition, run: () => Promise.reject(error) })]);

    try {
      await queue.initBullMQProducer(config);
      await queue.initBullMQConsumer(config);
      const payload = Definition({ value: 42 });
      await queue.publishJob(payload);
      await queue.publishBatchJobs([payload, payload]);
      expect(metrics.add).not.toHaveBeenCalled();
      await expect(firstWorker().processor(createNativeJob())).rejects.toBe(error);
      expect(tracing.spans.map(span => span.name)).toEqual([
        "kafka-trail: bullmq publish unit.job",
        "kafka-trail: bullmq publish unit.job",
        "kafka-trail: bullmq process unit.job",
      ]);

      for (const [index, traced] of tracing.spans.entries()) {
        expect(traced.options?.attributes).toEqual(expect.objectContaining({
          "messaging.system": "bullmq",
          "messaging.destination.name": "unit.job",
          "messaging.operation.name": index < 2 ? "publish" : "process",
          "messaging.batch.message_count": index === 1 ? 2 : 1,
        }));
        expect(traced.options?.kind).toBe(index < 2 ? tracing.otel.SpanKind.PRODUCER : tracing.otel.SpanKind.CONSUMER);
        expect(traced.span.end).toHaveBeenCalledTimes(1);
      }

      const processing = tracing.spans[2]?.span;
      expect(processing?.recordException).toHaveBeenCalledWith(error);
      expect(processing?.setStatus).toHaveBeenCalledWith({ code: tracing.otel.SpanStatusCode.ERROR, message: String(error) });
      expect(processing?.setAttribute).toHaveBeenCalledWith("messaging.message.id", "stable-id");
      expect(processing?.setAttribute).toHaveBeenCalledWith("messaging.bullmq.attempts_made", 1);
      expect(processing?.setAttribute.mock.calls.some(([name]) => name === "messaging.payload")).toBe(addPayloadToTrace);
      expect(tracing.spans[0]?.options?.attributes?.["messaging.payload"]).toBe(addPayloadToTrace ? payload.data.message : undefined);
      await queue.destroyAll();
    } finally {
      tracing.restore();
    }
  });
});
