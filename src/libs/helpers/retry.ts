import type pino from "pino";

import { KTRetryError } from "../../custom-errors/kafka-errors.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const retry = async (
  fn: () => Promise<void>,
  logger: pino.Logger,
  { maxRetries = 3, interval = 1000 } = {},
) => {
  let attempt = 1;

  while (attempt <= maxRetries) {
    try {
      await fn();

      return true;
    } catch (e) {
      logger.warn(e, `Error, retrying | ${attempt}`);
      await sleep(interval);
      attempt++;
    }
  }

  throw new KTRetryError();
};

export { retry };