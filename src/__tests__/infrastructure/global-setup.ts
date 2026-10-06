import { RedpandaContainer, type StartedRedpandaContainer } from "@testcontainers/redpanda";

const testGlobal = globalThis as typeof globalThis & {
  __KT_REDPANDA_CONTAINER__?: StartedRedpandaContainer
};

export default async function globalSetup(): Promise<void> {
  const redpanda = await new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v23.3.12")
    .withStartupTimeout(120_000)
    .start();

  testGlobal.__KT_REDPANDA_CONTAINER__ = redpanda;
  process.env.KT_TEST_KAFKA_BROKER_URL = redpanda.getBootstrapServers();
}
