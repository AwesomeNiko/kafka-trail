import { jest } from "@jest/globals";
import * as otel from "@opentelemetry/api";
import type { Counter, Histogram, Meter, Span, SpanOptions } from "@opentelemetry/api";

export const createMeterMocks = () => {
  const add = jest.fn<Counter["add"]>();
  const record = jest.fn<Histogram["record"]>();
  const produced = jest.fn<Counter["add"]>();
  const consumed = jest.fn<Counter["add"]>();
  const createCounter = jest.fn<Meter["createCounter"]>().mockImplementation(name => ({
    add: name === "messages_produced" ? produced : name === "messages_consumed" ? consumed : add,
  }));
  const createHistogram = jest.fn<Meter["createHistogram"]>().mockReturnValue({ record });
  const meter = { createCounter, createHistogram } as unknown as Meter;

  return { meter, add, record, produced, consumed, createCounter, createHistogram };
};

const createSpanMock = () => ({
  end: jest.fn<Span["end"]>(),
  recordException: jest.fn<Span["recordException"]>(),
  setStatus: jest.fn<Span["setStatus"]>(),
  setAttribute: jest.fn<Span["setAttribute"]>(),
});

export const createTracingMocks = () => {
  const spans: Array<{ name: string, options: SpanOptions | undefined, span: ReturnType<typeof createSpanMock> }> = [];
  const tracer = otel.trace.getTracer("test");
  const startSpan = jest.spyOn(tracer, "startSpan").mockImplementation((name, options) => {
    const span = createSpanMock();
    spans.push({ name, options, span });

    return span as unknown as Span;
  });
  const getTracer = jest.spyOn(otel.trace, "getTracer").mockReturnValue(tracer);

  return {
    otel,
    spans,
    startSpan,
    restore: () => { getTracer.mockRestore(); startSpan.mockRestore(); },
  };
};
