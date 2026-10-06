import type { StartedRedpandaContainer } from "@testcontainers/redpanda";

const testGlobal = globalThis as typeof globalThis & {
  __KT_REDPANDA_CONTAINER__?: StartedRedpandaContainer
};

export default async function globalTeardown(): Promise<void> {
  await testGlobal.__KT_REDPANDA_CONTAINER__?.stop();
}
