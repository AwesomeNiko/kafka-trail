import { KTCustomError } from "./custom-error.js";

class ConsumerSubscribeError extends KTCustomError {
  constructor(message = "CONSUMER_SUBSCRIBE_ERROR") {
    super(message, 500);
  }
}

class KTRetryError extends KTCustomError {
  constructor(message = "MAX_RETRIES") {
    super(message, 500);
  }
}

class NoLocalHandlersError extends KTCustomError {
  constructor(message = "Handler") {
    super(`No local handlers registered for ${message}`, 500);
  }
}

class NoHandlersError extends KTCustomError {
  constructor(message = "Handler") {
    super(`No local handlers registered for ${message}`, 500);
  }
}

class UnableDecreasePartitionsError extends KTCustomError {
  constructor(message = "Unable to decrease partitions due to data loss") {
    super(message, 500);
  }
}

class ArgumentIsRequired extends KTCustomError {
  constructor(argName = 'field') {
    super(`${argName} is not provided`, 500);
  }
}

class ProducerNotInitializedError extends KTCustomError {
  constructor(message = "Producer is not initialized") {
    super(message, 500);
  }
}

export {
  ConsumerSubscribeError, KTRetryError, NoLocalHandlersError, NoHandlersError, UnableDecreasePartitionsError, ArgumentIsRequired, ProducerNotInitializedError,
};
