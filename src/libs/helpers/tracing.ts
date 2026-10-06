import type { Span, SpanOptions } from "@opentelemetry/api";

type KTOtelApi = Pick<
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports
  typeof import("@opentelemetry/api"),
  "context" | "trace" | "SpanKind" | "SpanStatusCode"
>;

export type KTTracingSettings = {
  otel?: KTOtelApi
  addPayloadToTrace: boolean
}

export class KTTracing {
  readonly otel: KTOtelApi | undefined;
  readonly addPayloadToTrace: boolean;

  constructor(settings?: KTTracingSettings) {
    this.otel = settings?.otel;
    this.addPayloadToTrace = settings?.addPayloadToTrace ?? false;
  }

  async withSpan<T>(
    name: string,
    options: SpanOptions,
    run: (span?: Span) => Promise<T>,
  ): Promise<T> {
    if (!this.otel) {
      return run();
    }

    const span = this.otel.trace
      .getTracer("kafka-trail", "1.0.0")
      .startSpan(name, options);

    return this.otel.context.with(
      this.otel.trace.setSpan(this.otel.context.active(), span),
      async () => run(span),
    );
  }
}
