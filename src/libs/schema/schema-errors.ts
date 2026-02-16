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

export class KTSchemaRegistryError extends Error {
  readonly details: unknown

  constructor(params: {
    message: string
    details?: unknown
  }) {
    super(params.message)
    this.name = "KTSchemaRegistryError"
    this.details = params.details
  }
}
