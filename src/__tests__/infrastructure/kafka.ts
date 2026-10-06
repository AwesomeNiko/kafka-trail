export const INTEGRATION_TEST_TIMEOUT_MS = Number(process.env.KAFKA_INT_TEST_TIMEOUT_MS ?? 30_000);

export const getIntTestConfig = () => {
  const brokerUrl = process.env.KT_TEST_KAFKA_BROKER_URL;

  if (!brokerUrl) {
    throw new Error("Redpanda test infrastructure is not initialized");
  }

  return {
    brokerUrl,
    timeoutMs: INTEGRATION_TEST_TIMEOUT_MS,
  };
};
