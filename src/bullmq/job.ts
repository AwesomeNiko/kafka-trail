import type { DefaultJobOptions, JobsOptions, RepeatOptions } from "bullmq";
import { v4 } from "uuid";

import { ktDecode, ktEncode } from "../libs/helpers/default-data-parser.js";
import type { KTCodec } from "../libs/schema/schema-codec.js";

export type KTJobSettings = {
  name: string
  defaultJobOptions?: DefaultJobOptions
}

export type KTJobMeta = Record<string, string>

export type KTJobOptions = JobsOptions & {
  meta?: KTJobMeta
}

export type KTJobData = {
  message: string
  meta: KTJobMeta
}

export type KTJobPayload = {
  jobName: string
  data: KTJobData
  options: JobsOptions
}

export type KTJobEvent<Payload extends object> = {
  (payload: Payload, options?: KTJobOptions): KTJobPayload
  jobSettings: KTJobSettings
  decode: KTCodec<Payload>["decode"]
}

export type KTPayloadFromJob<T> = T extends KTJobEvent<infer P> ? P : never

export type KTJobScheduler = {
  schedulerId: string
  repeat: Omit<RepeatOptions, "key" | "jobId">
  job: KTJobPayload
}

export const CreateKTJob = <Payload extends object>(
  settings: KTJobSettings,
  codec?: KTCodec<Payload>,
): KTJobEvent<Payload> => {
  if (!settings.name || settings.name.includes(":")) {
    throw new Error("BullMQ job name must be non-empty and must not contain ':'");
  }

  const validatePayload = (payload: unknown): payload is Payload => {
    codec?.validate?.(payload);

    return true;
  };

  const job = (payload: Payload, { meta, ...options }: KTJobOptions = {}): KTJobPayload => {
    validatePayload(payload);

    return {
      jobName: settings.name,
      data: {
        message: codec ? codec.encode(payload) : ktEncode(payload),
        meta: { ...meta, traceId: meta?.traceId ?? v4() },
      },
      options: { ...settings.defaultJobOptions, ...options },
    };
  };

  job.jobSettings = settings;

  job.decode = (data: string | Buffer): Payload => {
    const decoded = codec ? codec.decode(data) : ktDecode<Payload>(data);
    validatePayload(decoded);

    return decoded;
  };

  return job;
};
