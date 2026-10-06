import type { Job, WorkerOptions } from "bullmq";

import type { KTLogger } from "../libs/helpers/logger.js";
import type { KTPublisher } from "../message-queue/publisher.js";

import type { KTJobData, KTJobEvent } from "./job.js";

export type KTJobHandlerOptions = Omit<WorkerOptions, "connection" | "prefix" | "autorun">

export type KTJobHandlerParams = {
  job: Job<KTJobData, void, string>
  signal: AbortSignal | undefined
}

export type KTJobRun<Payload extends object, Ctx extends object> = (
  payload: Payload[],
  ctx: Ctx,
  publisher: KTPublisher,
  jobParams: KTJobHandlerParams,
) => Promise<void>

export type KTJobHandler<Payload extends object, Ctx extends object> = {
  job: KTJobEvent<Payload>
  run: KTJobRun<Payload, Ctx>
  options?: KTJobHandlerOptions
}

export const KTJobHandler = <Payload extends object, Ctx extends object>(
  params: KTJobHandler<Payload, Ctx & KTLogger>,
): KTJobHandler<Payload, Ctx & KTLogger> => params;
