import { describe, expect, it } from "@jest/globals";

import { createHandlerTraceAttributes } from "../libs/helpers/observability.js";

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

    expect(attributes["messaging.kafka.payload.content_length"]).toBe(128)
    expect(attributes["messaging.kafka.payload"]).toBeUndefined()
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

    expect(attributes["messaging.kafka.payload.content_length"]).toBe(256)
    expect(attributes["messaging.kafka.payload"]).toBe(serializedPayload)
    expect(attributes["messaging.kafka.offset"]).toBeUndefined()
  })
})
