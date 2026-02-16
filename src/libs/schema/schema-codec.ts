export type KTSchemaMeta = {
  provider?: string
  schemaName?: string
  schemaVersion?: string
  schemaId?: string
}

export type KTCodec<Payload extends object> = {
  encode: (data: Payload) => string
  decode: (data: string | Buffer) => Payload
  validate?: (data: unknown) => asserts data is Payload
  schemaMeta?: KTSchemaMeta
}
