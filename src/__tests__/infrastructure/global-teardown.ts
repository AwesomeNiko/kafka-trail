import type { StartedRedisContainer } from "@testcontainers/redis";
import type { StartedRedpandaContainer } from "@testcontainers/redpanda";

const testGlobal = globalThis as typeof globalThis & {
  __KT_REDPANDA_CONTAINER__?: StartedRedpandaContainer
  __KT_REDIS_CONTAINER__?: StartedRedisContainer
};

export default async function globalTeardown(): Promise<void> {
  await Promise.all([
    testGlobal.__KT_REDPANDA_CONTAINER__?.stop(),
    testGlobal.__KT_REDIS_CONTAINER__?.stop(),
  ]);
}
