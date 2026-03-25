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
    'messaging.destination': params.topicName,
    'messaging.kafka.partition': params.partition,
    'messaging.kafka.payload.content_length': params.payloadContentLength,
  }

  if (params.lastOffset) {
    attributes['messaging.kafka.offset']= params.lastOffset
  }

  if (params.opts.addPayloadToTrace) {
    attributes['messaging.kafka.payload']= JSON.stringify(params.batchedValues)
  }

  return attributes
}
