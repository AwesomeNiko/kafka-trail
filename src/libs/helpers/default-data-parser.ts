import type { KTCodec } from "../schema/schema-codec.js";

export type KTTopicPayloadParser<Payload extends object> = KTCodec<Payload>

const ktDecode = <Payload>(data:string | Buffer) => {
  if (Buffer.isBuffer(data)) {
    data = data.toString()
  }

  return JSON.parse(data) as Payload
}

const ktEncode = (data: object) => JSON.stringify(data)

export {
  ktEncode,
  ktDecode,
}
