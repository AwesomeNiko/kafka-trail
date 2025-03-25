class KTCustomError extends Error {
  public status?: number = undefined;
  constructor(message: string, statusCode: number) {
    super(message);
    this.name = this.constructor.name;
    this.status = statusCode
  }
}

export { KTCustomError };
