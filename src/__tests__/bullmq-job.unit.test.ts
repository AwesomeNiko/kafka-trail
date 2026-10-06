import { describe, expect, it } from "@jest/globals";
import { z } from "zod";

import { CreateKTJob } from "../bullmq/job.js";
import { createZodCodec } from "../libs/schema/adapters/zod-adapter.js";
import type { KTCodec } from "../libs/schema/schema-codec.js";
import { KTSchemaValidationError } from "../libs/schema/schema-errors.js";

describe("BullMQ job definitions", () => {
  it("round-trips payloads and combines defaults, job options and metadata", () => {
    const SendEmail = CreateKTJob<{ email: string }>({
      name: "send.email",
      defaultJobOptions: { attempts: 3, backoff: { type: "exponential", delay: 1000 }, removeOnComplete: true },
    });
    const job = SendEmail({ email: "test@example.com" }, { attempts: 5, jobId: "email-1", meta: { traceId: "trace-1", tenantId: "tenant-1" } });

    expect(job.jobName).toBe("send.email");
    expect(SendEmail.decode(job.data.message)).toEqual({ email: "test@example.com" });
    expect(job.options).toEqual({ attempts: 5, backoff: { type: "exponential", delay: 1000 }, removeOnComplete: true, jobId: "email-1" });
    expect(job.data.meta).toEqual({ traceId: "trace-1", tenantId: "tenant-1" });
    expect(SendEmail({ email: "test@example.com" }).data.meta.traceId).toEqual(expect.any(String));
  });

  it("uses the same codecs as Kafka and validates both producer and consumer payloads", () => {
    const Job = CreateKTJob({ name: "validated" }, createZodCodec(z.object({ value: z.number() })));

    expect(Job.decode(Job({ value: 42 }).data.message)).toEqual({ value: 42 });
    expect(() => Job({ value: "invalid" } as unknown as { value: number })).toThrow(KTSchemaValidationError);
    expect(() => Job.decode('{"value":"invalid"}')).toThrow(KTSchemaValidationError);
  });

  it("supports custom encoding without assuming JSON on the consumer", () => {
    const codec: KTCodec<{ value: number }> = {
      encode: payload => String(payload.value),
      decode: data => ({ value: Number(data.toString()) }),
    };
    const Job = CreateKTJob({ name: "custom.codec" }, codec);

    expect(Job({ value: 42 }).data.message).toBe("42");
    expect(Job.decode(Buffer.from("42"))).toEqual({ value: 42 });
  });

  it.each(["", "invalid:name"])("rejects an invalid BullMQ queue name: %s", name => {
    expect(() => CreateKTJob({ name })).toThrow("BullMQ job name");
  });
});
