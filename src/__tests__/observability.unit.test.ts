import { describe, expect, it, jest } from "@jest/globals";

import { createHandlerTraceAttributes, KTObservability } from "../libs/helpers/observability.js";
import { KTTracing } from "../libs/helpers/tracing.js";

import { createMeterMocks, createTracingMocks } from "./mocks/observability.js";

describe("createHandlerTraceAttributes", () => {
  it("should include payload content length even when payload tracing is disabled", () => {
    const batchedValues = [{ text: "hello" }]

    const attributes = createHandlerTraceAttributes({
      topicName: "test.topic",
      partition: 1,
      lastOffset: "42",
      batchedValues,
      payloadContentLength: 128,
      opts: {
        addPayloadToTrace: false,
      },
    })

    expect(attributes["messaging.payload"]).toBeUndefined()
    expect(attributes["messaging.message.body.size"]).toBe(128)
    expect(attributes["messaging.kafka.offset"]).toBe("42")
  })

  it("should reuse the serialized payload when payload tracing is enabled", () => {
    const batchedValues = [{ text: "payload" }, { text: "trace" }]
    const serializedPayload = JSON.stringify(batchedValues)

    const attributes = createHandlerTraceAttributes({
      topicName: "test.topic",
      partition: 2,
      lastOffset: undefined,
      batchedValues,
      payloadContentLength: 256,
      opts: {
        addPayloadToTrace: true,
      },
    })

    expect(attributes["messaging.message.body.size"]).toBe(256)
    expect(attributes["messaging.payload"]).toBe(serializedPayload)
    expect(attributes["messaging.batch.message_count"]).toBe(2)
    expect(attributes["messaging.kafka.offset"]).toBeUndefined()
  })
})

describe("shared message observability", () => {
  it.each(["kafka", "bullmq"] as const)("counts %s publication only after success and by message count", async system => {
    const metrics = createMeterMocks();
    const observability = new KTObservability({ tracing: new KTTracing(), meter: metrics.meter });
    const release = Promise.withResolvers<undefined>();
    const publishing = observability.withSpan({
      system,
      name: "test.message",
      operation: "publish",
      messageCount: 3,
      attributes: { "messaging.message.id": "job-42", "messaging.trace_id": "trace-42" },
    }, () => release.promise);
    expect(metrics.produced).not.toHaveBeenCalled();
    release.resolve(undefined);
    await publishing;

    await observability.withSpan({ system, name: "test.message", operation: "publish", messageCount: 0 }, () => Promise.resolve());
    const error = new Error("publication failed");
    await expect(observability.withSpan({ system, name: "test.message", operation: "publish", messageCount: 5 }, () => Promise.reject(error))).rejects.toBe(error);
    expect(metrics.produced.mock.calls).toEqual([
      [3, { "messaging.system": system, "messaging.destination.name": "test.message" }],
      [0, { "messaging.system": system, "messaging.destination.name": "test.message" }],
    ]);
    expect(metrics.consumed).not.toHaveBeenCalled();
    expect(metrics.add).not.toHaveBeenCalled();
  });

  it("separates Kafka consumption by consumer group and records empty batches", () => {
    const metrics = createMeterMocks();
    const observability = new KTObservability({ tracing: new KTTracing(), meter: metrics.meter });
    observability.recordConsumed({ system: "kafka", name: "events", count: 0, consumerGroup: "group-a" });
    observability.recordConsumed({ system: "kafka", name: "events", count: 2, consumerGroup: "group-a" });
    observability.recordConsumed({ system: "kafka", name: "events", count: 2, consumerGroup: "group-b" });
    observability.recordConsumed({ system: "bullmq", name: "task", count: 1 });

    expect(metrics.consumed.mock.calls).toEqual([
      [0, { "messaging.system": "kafka", "messaging.destination.name": "events", "messaging.consumer.group.name": "group-a" }],
      [2, { "messaging.system": "kafka", "messaging.destination.name": "events", "messaging.consumer.group.name": "group-a" }],
      [2, { "messaging.system": "kafka", "messaging.destination.name": "events", "messaging.consumer.group.name": "group-b" }],
      [1, { "messaging.system": "bullmq", "messaging.destination.name": "task" }],
    ]);
  });

  it.each(["kafka", "bullmq"] as const)("records %s processing duration in seconds without high-cardinality metric attributes", async system => {
    const metrics = createMeterMocks();
    const observability = new KTObservability({ tracing: new KTTracing(), meter: metrics.meter });
    const now = jest.spyOn(performance, "now").mockReturnValueOnce(1000).mockReturnValueOnce(1250);

    try {
      await expect(observability.withSpan({
        system,
        name: "test.message",
        operation: "process",
        attributes: { "messaging.message.id": "message-42", "messaging.trace_id": "trace-42" },
      }, () => Promise.resolve(42))).resolves.toBe(42);

      const attributes = { "messaging.system": system, "messaging.destination.name": "test.message", "messaging.handler.outcome": "completed" };
      expect(metrics.add).toHaveBeenCalledWith(1, attributes);
      expect(metrics.record).toHaveBeenCalledWith(0.25, attributes);
    } finally {
      now.mockRestore();
    }
  });

  it.each([
    { operation: "publish", error: "publication failed" },
    { operation: "process", error: new Error("handler failed") },
  ] as const)("marks failed $operation spans and preserves the original error", async ({ operation, error }) => {
    const tracing = createTracingMocks();
    const metrics = createMeterMocks();
    const observability = new KTObservability({ tracing: new KTTracing({ otel: tracing.otel, addPayloadToTrace: false }), meter: metrics.meter });

    try {
      // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- Application handlers can reject with non-Error values.
      await expect(observability.withSpan({ system: "bullmq", name: "test.message", operation }, () => Promise.reject(error))).rejects.toBe(error);
      const traced = tracing.spans[0];
      expect(traced?.options).toEqual({
        kind: operation === "publish" ? tracing.otel.SpanKind.PRODUCER : tracing.otel.SpanKind.CONSUMER,
        attributes: { "messaging.system": "bullmq", "messaging.destination.name": "test.message", "messaging.operation.name": operation },
      });
      expect(traced?.span.recordException).toHaveBeenCalledWith(error);
      expect(traced?.span.setStatus).toHaveBeenCalledWith({ code: tracing.otel.SpanStatusCode.ERROR, message: String(error) });
      expect(traced?.span.end).toHaveBeenCalledTimes(1);
      expect(metrics.add).toHaveBeenCalledTimes(operation === "process" ? 1 : 0);

      if (operation === "process") {
        expect(metrics.add).toHaveBeenCalledWith(1, { "messaging.system": "bullmq", "messaging.destination.name": "test.message", "messaging.handler.outcome": "failed" });
      }
    } finally {
      tracing.restore();
    }
  });
});
