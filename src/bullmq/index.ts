import type { Span } from "@opentelemetry/api";
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

  constructor(params: { ctx: Ctx & KTLogger, publisher: KTPublisher, tracing: KTTracing }) {
    this.#ctx = params.ctx;
    this.#publisher = params.publisher;
    this.#tracing = params.tracing;
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

  #runHandler<Payload extends object>(handler: KTJobHandler<Payload, Ctx & KTLogger>, job: Job<KTJobData, void, string>, signal?: AbortSignal): Promise<void> {
    return this.#withSpan("process", job.queueName, async (span) => {
      span?.setAttribute("messaging.message.id", job.id ?? "");
      span?.setAttribute("messaging.bullmq.attempts_made", job.attemptsMade);

      if (job.data.meta?.traceId) {
        span?.setAttribute("messaging.trace_id", job.data.meta.traceId);
      }

      if (this.#tracing.addPayloadToTrace) {
        span?.setAttribute("messaging.bullmq.payload", job.data.message);
      }

      const payload = handler.job.decode(job.data.message);
      await handler.run([payload], this.#ctx, this.#publisher, { job, signal });
    });
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

  async destroyConsumer(): Promise<void> {
    await Promise.all([...this.#workers.values()].map(worker => worker.close()));
    this.#workers.clear();
    this.#consumerConfig = undefined;
  }

  async destroyProducer(): Promise<void> {
    await Promise.all([...this.#queues.values()].map(queue => queue.close()));
    this.#queues.clear();
    await this.#producerConnection?.close();
    this.#producerConnection = undefined;
    this.#producerConfig = undefined;
  }
}
