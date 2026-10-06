export const getRedisTestConfig = () => {
  const host = process.env.KT_TEST_REDIS_HOST;
  const port = process.env.KT_TEST_REDIS_PORT;

  if (!host || !port) {
    throw new Error("Redis test infrastructure is not initialized");
  }

  return { connection: { host, port: Number(port) } };
};
