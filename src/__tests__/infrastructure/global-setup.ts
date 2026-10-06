import { RedisContainer, type StartedRedisContainer } from "@testcontainers/redis";
import { RedpandaContainer, type StartedRedpandaContainer } from "@testcontainers/redpanda";

const testGlobal = globalThis as typeof globalThis & {
  __KT_REDPANDA_CONTAINER__?: StartedRedpandaContainer
  __KT_REDIS_CONTAINER__?: StartedRedisContainer
};

export default async function globalSetup(): Promise<void> {
  const redpanda = await new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.3.12")
    .withStartupTimeout(120_000)
    .start();

  testGlobal.__KT_REDPANDA_CONTAINER__ = redpanda;
  process.env.KT_TEST_KAFKA_BROKER_URL = redpanda.getBootstrapServers();

  try {
    const redis = await new RedisContainer("redis:7.4.2-alpine")
      .withStartupTimeout(120_000)
      .start();

    testGlobal.__KT_REDIS_CONTAINER__ = redis;
    process.env.KT_TEST_REDIS_HOST = redis.getHost();
    process.env.KT_TEST_REDIS_PORT = String(redis.getPort());
  } catch (error) {
    await redpanda.stop();
    throw error;
  }
}
