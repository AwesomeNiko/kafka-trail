import { KTKafkaJSClientAdapter, type KTKafkaRuntimeConfig } from "./kafkajs-adapter.js";
import type { KTRuntimeClient } from "./transport-types.js";

type CreateKafkaRuntimeFn = (params: KTKafkaRuntimeConfig) => KTRuntimeClient

let createKafkaRuntimeImpl: CreateKafkaRuntimeFn = (params) => new KTKafkaJSClientAdapter(params)

const createKafkaRuntime = (params: KTKafkaRuntimeConfig): KTRuntimeClient => {
  return createKafkaRuntimeImpl(params)
}

const setKafkaRuntimeFactoryForTests = (fn: CreateKafkaRuntimeFn) => {
  createKafkaRuntimeImpl = fn
}

const resetKafkaRuntimeFactoryForTests = () => {
  createKafkaRuntimeImpl = (params) => new KTKafkaJSClientAdapter(params)
}

export {
  createKafkaRuntime,
  resetKafkaRuntimeFactoryForTests,
  setKafkaRuntimeFactoryForTests,
};
