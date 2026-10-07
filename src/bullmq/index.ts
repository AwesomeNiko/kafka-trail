import type { Counter, Histogram, Meter, Span } from "@opentelemetry/api";
import { Queue, RedisConnection, Worker } from "bullmq";
import type { Job, QueueOptions, RedisOptions, WorkerOptions } from "bullmq";

import { BullMQProducerNotInitializedError, NoJobHandlersError } from "../custom-errors/bullmq-errors.js";
import type { KTLogger } from "../libs/helpers/logger.js";
import type { KTTracing } from "../libs/helpers/tracing.js";
import type { KTPublisher } from "../message-queue/publisher.js";

import type { KTJobHandler } from "./consumer-handler.js";
import type { KTJobData, KTJobPayload, KTJobScheduler } from "./job.js";

export type KTBullMQProducerConfig = Omit<QueueOptions, "connection"> & {
  connection: RedisOptions
}

export type KTBullMQConsumerConfig = Omit<WorkerOptions, "connection" | "autorun"> & {
  connection: RedisOptions
}

export type KTBullMQShutdownOptions = {
  graceful?: boolean
  timeout?: number
}

export class BullMQBackend<Ctx extends object> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #handlers = new Map<string, KTJobHandler<any, Ctx & KTLogger>>();
  #queues = new Map<string, Queue<KTJobData, void, string>>();
  #workers = new Map<string, Worker<KTJobData, void, string>>();
  #producerConnection: RedisConnection | undefined;
  #producerConfig: KTBullMQProducerConfig | undefined;
  #consumerConfig: KTBullMQConsumerConfig | undefined;
  #ctx: Ctx & KTLogger;
  #publisher: KTPublisher;
  #tracing: KTTracing;
  #activeControllers = new Set<AbortController>();
  #activeJobs = new Set<Promise<void>>();
  #activeFailureHandlers = new Set<Promise<void>>();
  #executions: Counter | undefined;
  #duration: Histogram | undefined;

  constructor(params: { ctx: Ctx & KTLogger, publisher: KTPublisher, tracing: KTTracing, meter?: Meter }) {
    this.#ctx = params.ctx;
    this.#publisher = params.publisher;
    this.#tracing = params.tracing;
    this.#executions = params.meter?.createCounter("job_handler_executions", { description: "Number of job execution attempts" });
    this.#duration = params.meter?.createHistogram("job_handler_duration", { description: "Job execution duration", unit: "s" });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  registerHandlers(handlers: KTJobHandler<any, Ctx & KTLogger>[]) {
    if (this.#consumerConfig) {
      throw new Error("Register BullMQ job handlers before initializing the consumer");
    }

    for (const handler of handlers) {
      const name = handler.job.jobSettings.name;

      if (this.#handlers.has(name)) {
        this.#ctx.logger.warn({ jobName: name }, "BullMQ job handler is already registered");
        continue;
      }

      this.#handlers.set(name, handler);
    }
  }

  getRegisteredHandler(name: string) {
    return this.#handlers.get(name);
  }

  getQueue(name: string) {
    return this.#queues.get(name);
  }

  getWorker(name: string) {
    return this.#workers.get(name);
  }

  async initProducer(config: KTBullMQProducerConfig): Promise<void> {
    if (this.#producerConnection) {
      throw new Error("BullMQ producer is already initialized");
    }

    const connection = new RedisConnection(config.connection, {
      blocking: false,
      skipVersionCheck: config.skipVersionCheck ?? false,
      skipWaitingForReady: config.skipWaitingForReady ?? false,
    });
    connection.on("error", (error: Error) => this.#ctx.logger.error({ err: error }, "BullMQ producer connection error"));
    this.#producerConnection = connection;

    try {
      await connection.client;
      this.#producerConfig = config;
    } catch (error) {
      await connection.close(true);
      this.#producerConnection = undefined;
      throw error;
    }
  }

  async initConsumer(config: KTBullMQConsumerConfig): Promise<void> {
    if (this.#consumerConfig) {
      throw new Error("BullMQ consumer is already initialized");
    }

    if (!this.#handlers.size) {
      throw new NoJobHandlersError();
    }

    this.#consumerConfig = config;

    try {
      for (const [name, handler] of this.#handlers) {
        const worker = new Worker<KTJobData, void, string>(name,
          (job, _token, signal) => this.#runHandler(handler, job, signal),
          { ...config, ...handler.options, autorun: false });
        worker.on("error", (error: Error) => this.#ctx.logger.error({ err: error, jobName: name }, "BullMQ worker error"));
        worker.on("lockRenewalFailed", (jobIds: string[]) => {
          for (const jobId of jobIds) worker.cancelJob(jobId, "BullMQ lock renewal failed");
        });
        worker.on("failed", (job, error) => {
          if (!job || !handler.onFinalFailure) return;

          const handling = this.#runFinalFailure(handler, job, error);
          this.#activeFailureHandlers.add(handling);
          void handling.finally(() => this.#activeFailureHandlers.delete(handling));
        });
        this.#workers.set(name, worker);
        await worker.waitUntilReady();
      }

      for (const [name, worker] of this.#workers) {
        void worker.run().catch((error: unknown) => {
          this.#ctx.logger.error({ err: error, jobName: name }, "BullMQ worker stopped unexpectedly");
        });
      }
    } catch (error) {
      await this.destroyConsumer();
      throw error;
    }
  }

  async #queue(name: string): Promise<Queue<KTJobData, void, string>> {
    const connection = this.#producerConnection;
    const config = this.#producerConfig;

    if (!connection || !config) {
      throw new BullMQProducerNotInitializedError();
    }

    const client = await connection.client;
    let queue = this.#queues.get(name);

    if (!queue) {
      queue = new Queue<KTJobData, void, string>(name, { ...config, connection: client });
      queue.on("error", (error: Error) => this.#ctx.logger.error({ err: error, jobName: name }, "BullMQ queue error"));
      this.#queues.set(name, queue);
    }

    return queue;
  }

  publishJob(payload: KTJobPayload): Promise<Job<KTJobData, void, string>> {
    return this.#withSpan("publish", payload.jobName, async () => {
      const queue = await this.#queue(payload.jobName);

      return queue.add(payload.jobName, payload.data, payload.options);
    });
  }

  async checkConnection(): Promise<void> {
    if (!this.#producerConnection && !this.#workers.size) throw new Error("BullMQ is not initialized");

    if (this.#producerConnection) {
      const client = await this.#producerConnection.client;
      await client.info();
    }

    await Promise.all([...this.#workers.values()].map(async worker => {
      const client = await worker.getBackend().client;
      await client.info();
    }));
  }

  async publishBatchJobs(payloads: KTJobPayload[]): Promise<void> {
    const groups = new Map<string, KTJobPayload[]>();

    for (const payload of payloads) {
      const group = groups.get(payload.jobName) ?? [];
      group.push(payload);
      groups.set(payload.jobName, group);
    }

    await Promise.all([...groups].map(([name, jobs]) => this.#withSpan("publish", name, async () => {
      const queue = await this.#queue(name);
      await queue.addBulk(jobs.map(job => ({ name, data: job.data, opts: job.options })));
    })));
  }

  async upsertJobScheduler({ schedulerId, repeat, job }: KTJobScheduler) {
    const { jobId, delay, deduplication, ...options } = job.options;

    if (jobId !== undefined || delay !== undefined || deduplication !== undefined) {
      throw new Error("BullMQ scheduler templates do not support jobId, delay or deduplication");
    }

    const queue = await this.#queue(job.jobName);

    return queue.upsertJobScheduler(schedulerId, repeat, { name: job.jobName, data: job.data, opts: options });
  }

  async removeJobScheduler(params: { jobName: string, schedulerId: string }): Promise<boolean> {
    const queue = await this.#queue(params.jobName);

    return queue.removeJobScheduler(params.schedulerId);
  }

  #runHandler<Payload extends object>(handler: KTJobHandler<Payload, Ctx & KTLogger>, job: Job<KTJobData, void, string>, workerSignal?: AbortSignal): Promise<void> {
    const controller = new AbortController();
    const abort = () => controller.abort(workerSignal?.reason);
    workerSignal?.addEventListener("abort", abort, { once: true });
    if (workerSignal?.aborted) abort();
    this.#activeControllers.add(controller);
    const startedAt = performance.now();
    const processing = this.#withSpan("process", job.queueName, async (span) => {
      span?.setAttribute("messaging.message.id", job.id ?? "");
      span?.setAttribute("messaging.bullmq.attempts_made", job.attemptsMade);

      if (job.data.meta?.traceId) {
        span?.setAttribute("messaging.trace_id", job.data.meta.traceId);
      }

      if (this.#tracing.addPayloadToTrace) {
        span?.setAttribute("messaging.bullmq.payload", job.data.message);
      }

      const params = { job, signal: controller.signal };
      let outcome = "failed";

      try {
        const payloads = [handler.job.decode(job.data.message)];
        await handler.run(payloads, this.#ctx, this.#publisher, params);
        outcome = "completed";
      } catch (error) {
        this.#ctx.logger.error({ err: error, jobName: job.queueName, jobId: job.id, durationMs: performance.now() - startedAt }, "BullMQ job attempt failed");
        throw error;
      } finally {
        this.#executions?.add(1, { "job.name": job.queueName, "job.outcome": outcome });
        this.#duration?.record((performance.now() - startedAt) / 1000, { "job.name": job.queueName });

        if (outcome !== "failed") {
          this.#ctx.logger.info({ jobName: job.queueName, jobId: job.id, outcome, durationMs: performance.now() - startedAt }, "BullMQ job attempt completed");
        }
      }
    });
    this.#activeJobs.add(processing);

    return processing.finally(() => {
      workerSignal?.removeEventListener("abort", abort);
      this.#activeControllers.delete(controller);
      this.#activeJobs.delete(processing);
    });
  }

  async #runFinalFailure<Payload extends object>(handler: KTJobHandler<Payload, Ctx & KTLogger>, job: Job<KTJobData, void, string>, error: Error): Promise<void> {
    const controller = new AbortController();
    this.#activeControllers.add(controller);

    try {
      const state = await job.getState();
      // removeOnFail can delete the job before BullMQ emits its failed event.
      if (state !== "failed" && state !== "unknown") return;

      const payloads = [handler.job.decode(job.data.message)];
      await handler.onFinalFailure?.(payloads, this.#ctx, this.#publisher, { job, signal: controller.signal, error });
    } catch (callbackError) {
      this.#ctx.logger.error({ err: callbackError, jobName: job.queueName, jobId: job.id }, "BullMQ onFinalFailure callback failed");
    } finally {
      this.#activeControllers.delete(controller);
    }
  }

  #withSpan<T>(operation: "publish" | "process", name: string, run: (span?: Span) => Promise<T>): Promise<T> {
    const otel = this.#tracing.otel;

    return this.#tracing.withSpan(`kafka-trail: bullmq ${operation} ${name}`, {
      ...(otel ? { kind: operation === "publish" ? otel.SpanKind.PRODUCER : otel.SpanKind.CONSUMER } : {}),
      attributes: { "messaging.system": "bullmq", "messaging.destination.name": name, "messaging.operation.name": operation },
    }, async (span) => {
      try {
        return await run(span);
      } catch (error) {
        if (error instanceof Error) {
          span?.recordException(error);
        }

        if (otel) {
          span?.setStatus({ code: otel.SpanStatusCode.ERROR, message: String(error) });
        }

        throw error;
      } finally {
        span?.end();
      }
    });
  }

  async destroyConsumer({ graceful = true, timeout = 30_000 }: KTBullMQShutdownOptions = {}): Promise<void> {
    const workers = [...this.#workers.values()];
    await Promise.all(workers.map(worker => worker.pause(true)));
    const deadline = performance.now() + timeout;
    const finished = graceful && await this.#waitForTasks([...this.#activeJobs, ...this.#activeFailureHandlers], timeout);
    if (!finished) this.#cancelActiveJobs();
    await Promise.all(workers.map(worker => worker.close(!finished)));

    if (finished) {
      const callbacksFinished = await this.#waitForTasks([...this.#activeFailureHandlers], Math.max(0, deadline - performance.now()));
      if (!callbacksFinished) this.#cancelActiveJobs();
    }

    this.#workers.clear();
    this.#consumerConfig = undefined;
  }

  #cancelActiveJobs() {
    const reason = new Error("BullMQ consumer stopped before active handlers finished");
    for (const controller of this.#activeControllers) controller.abort(reason);
    for (const worker of this.#workers.values()) worker.cancelAllJobs(reason.message);
  }

  async #waitForTasks(tasks: Promise<void>[], timeout: number): Promise<boolean> {
    if (!tasks.length) return true;

    let timer: ReturnType<typeof setTimeout> | undefined;

    try {
      return await Promise.race([
        Promise.allSettled(tasks).then(() => true),
        new Promise<boolean>(resolve => { timer = setTimeout(() => resolve(false), timeout); }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  async destroyProducer(): Promise<void> {
    await Promise.all([...this.#queues.values()].map(queue => queue.close()));
    this.#queues.clear();
    await this.#producerConnection?.close();
    this.#producerConnection = undefined;
    this.#producerConfig = undefined;
  }
}
