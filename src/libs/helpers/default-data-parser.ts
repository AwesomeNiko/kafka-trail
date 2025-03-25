export type KTTopicPayloadParser<Payload extends object> = {
  decode: (data: string | Buffer) => Payload
  encode: (data: Payload) => string
}

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