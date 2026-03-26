import type { KafkaBrokerConfig } from "../kafka-broker.js";

import { KTConfluentKafkaJSClientAdapter } from "./confluent-kafkajs-adapter.js";
import { KTKafkaJSClientAdapter } from "./kafkajs-adapter.js";
import type { KTRuntimeClient } from "./transport-types.js";

type CreateKafkaRuntimeFn = (params: KafkaBrokerConfig) => KTRuntimeClient

let createKafkaRuntimeImpl: CreateKafkaRuntimeFn = (params) => {
  if (params.kafkaSettings.runtime === "kafkajs") {
    return new KTKafkaJSClientAdapter(params)
  }

  return new KTConfluentKafkaJSClientAdapter(params)
}

const createKafkaRuntime = (params: KafkaBrokerConfig): KTRuntimeClient => {
  return createKafkaRuntimeImpl(params)
}

const setKafkaRuntimeFactoryForTests = (fn: CreateKafkaRuntimeFn) => {
  createKafkaRuntimeImpl = fn
}

const resetKafkaRuntimeFactoryForTests = () => {
  createKafkaRuntimeImpl = (params) => {
    if (params.kafkaSettings.runtime === "kafkajs") {
      return new KTKafkaJSClientAdapter(params)
    }

    return new KTConfluentKafkaJSClientAdapter(params)
  }
}

export {
  createKafkaRuntime,
  resetKafkaRuntimeFactoryForTests,
  setKafkaRuntimeFactoryForTests,
};
