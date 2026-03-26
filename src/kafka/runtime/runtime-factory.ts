import type { KafkaBrokerConfig } from "../kafka-broker.js";

import { KTConfluentKafkaClientAdapter } from "./confluent-adapter.js";
import type { KTRuntimeClient } from "./transport-types.js";

type CreateKafkaRuntimeFn = (params: KafkaBrokerConfig) => KTRuntimeClient

let createKafkaRuntimeImpl: CreateKafkaRuntimeFn = (params) => {
  return new KTConfluentKafkaClientAdapter(params)
}

const createKafkaRuntime = (params: KafkaBrokerConfig): KTRuntimeClient => {
  return createKafkaRuntimeImpl(params)
}

const setKafkaRuntimeFactoryForTests = (fn: CreateKafkaRuntimeFn) => {
  createKafkaRuntimeImpl = fn
}

const resetKafkaRuntimeFactoryForTests = () => {
  createKafkaRuntimeImpl = (params) => {
    return new KTConfluentKafkaClientAdapter(params)
  }
}

export {
  createKafkaRuntime,
  resetKafkaRuntimeFactoryForTests,
  setKafkaRuntimeFactoryForTests,
};
