import { KTCustomError } from "./custom-error.js";

export class BullMQProducerNotInitializedError extends KTCustomError {
  constructor() {
    super("BullMQ producer is not initialized", 500);
  }
}

export class NoJobHandlersError extends KTCustomError {
  constructor() {
    super("No local BullMQ job handlers registered", 500);
  }
}
