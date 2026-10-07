import type { Attributes, Counter, Histogram, Meter, Span } from "@opentelemetry/api";

import type { KTTracing } from "./tracing.js";

type CreateHandlerTraceAttributes = {
  topicName: string,
  partition: number,
  lastOffset: string | undefined,
  batchedValues: object[],
  payloadContentLength: number,
  opts: {
    addPayloadToTrace: boolean,
  },
}

export const createHandlerTraceAttributes = (params: CreateHandlerTraceAttributes ) => {
  const attributes: Record<string, string | number> = {
    'messaging.system': 'kafka',
    'messaging.destination.name': params.topicName,
    'messaging.batch.message_count': params.batchedValues.length,
    'messaging.message.body.size': params.payloadContentLength,
    'messaging.kafka.partition': params.partition,
  }

  if (params.lastOffset) {
    attributes['messaging.kafka.offset']= params.lastOffset
  }

  if (params.opts.addPayloadToTrace) {
    const payload = JSON.stringify(params.batchedValues)
    attributes['messaging.payload'] = payload
  }

  return attributes
}

type KTMessageOperation = {
  system: "kafka" | "bullmq",
  name: string,
  operation: "publish" | "process",
  messageCount?: number,
  attributes?: Attributes,
}

export class KTObservability {
  #tracing: KTTracing;
  #executions: Counter | undefined;
  #duration: Histogram | undefined;
  #produced: Counter | undefined;
  #consumed: Counter | undefined;

  constructor(params: { tracing: KTTracing, meter?: Meter }) {
    this.#tracing = params.tracing;
    this.#executions = params.meter?.createCounter("message_handler_executions", { description: "Number of message processing attempts" });
    this.#duration = params.meter?.createHistogram("message_handler_duration", { description: "Message processing duration", unit: "s" });
    this.#produced = params.meter?.createCounter("messages_produced", { description: "Number of messages successfully published" });
    this.#consumed = params.meter?.createCounter("messages_consumed", { description: "Number of messages whose processing has completed" });
  }

  recordConsumed({ system, name, count, consumerGroup }: {
    system: KTMessageOperation["system"],
    name: string,
    count: number,
    consumerGroup?: string,
  }): void {
    this.#consumed?.add(count, {
      "messaging.system": system,
      "messaging.destination.name": name,
      ...(consumerGroup ? { "messaging.consumer.group.name": consumerGroup } : {}),
    });
  }

  withSpan<T>({ system, name, operation, messageCount = 1, attributes }: KTMessageOperation, run: (span?: Span) => Promise<T>): Promise<T> {
    const otel = this.#tracing.otel;

    return this.#tracing.withSpan(`kafka-trail: ${system} ${operation} ${name}`, {
      ...(otel ? { kind: operation === "publish" ? otel.SpanKind.PRODUCER : otel.SpanKind.CONSUMER } : {}),
      attributes: {
        ...attributes,
        "messaging.system": system,
        "messaging.destination.name": name,
        "messaging.operation.name": operation,
      },
    }, async span => {
      const startedAt = performance.now();
      let outcome = "completed";

      try {
        const result = await run(span);

        if (operation === "publish") {
          this.#produced?.add(messageCount, { "messaging.system": system, "messaging.destination.name": name });
        }

        return result;
      } catch (error) {
        outcome = "failed";
        span?.recordException(error instanceof Error ? error : String(error));

        if (otel) {
          span?.setStatus({ code: otel.SpanStatusCode.ERROR, message: String(error) });
        }

        throw error;
      } finally {
        if (operation === "process") {
          const metricAttributes = {
            "messaging.system": system,
            "messaging.destination.name": name,
            "messaging.handler.outcome": outcome,
          };
          this.#executions?.add(1, metricAttributes);
          this.#duration?.record((performance.now() - startedAt) / 1000, metricAttributes);
          span?.setAttribute("messaging.handler.outcome", outcome);
        }

        span?.end();
      }
    });
  }
}
