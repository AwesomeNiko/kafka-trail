export type KTSchemaValidationSource = 'encode' | 'decode' | 'validate'

export class KTSchemaValidationError extends Error {
  readonly source: KTSchemaValidationSource
  readonly details: unknown

  constructor(params: {
    message: string
    source: KTSchemaValidationSource
    details?: unknown
  }) {
    super(params.message)
    this.name = 'KTSchemaValidationError'
    this.source = params.source
    this.details = params.details
  }
}
