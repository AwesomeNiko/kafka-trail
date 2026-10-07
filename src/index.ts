import { ConsumerSubscribeError, KTRetryError, NoHandlersError, UnableDecreasePartitionsError, ArgumentIsRequired, ProducerNotInitializedError, ProducerInitRequiredForDLQError } from "./custom-errors/kafka-errors.js"
import { KTHandler } from "./kafka/consumer-handler.js";
import { CustomPartitioner } from "./kafka/custom-partitioner.js";
import { KTTopicBatch, CreateKTTopicBatch } from "./kafka/topic-batch.js"
import { KTTopic, CreateKTTopic, DLQKTTopic } from "./kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "./libs/branded-types/kafka/index.js";
import { createAjvCodec, createAjvCodecFromSchema } from "./libs/schema/adapters/ajv-adapter.js";
import { createAwsGlueCodec, clearAwsGlueSchemaCache, createAwsGlueSchemaRegistryAdapter, preloadAwsGlueSchemas } from "./libs/schema/adapters/aws-glue-adapter.js";
import { createZodCodec } from "./libs/schema/adapters/zod-adapter.js";
import { KTSchemaRegistryError, KTSchemaValidationError } from "./libs/schema/schema-errors.js";
import { KTMessageQueue } from "./message-queue/index.js";

export type { KTPayloadFromTopic, KTTopicEvent } from "./kafka/topic.js"
export type { KTCodec, KTSchemaMeta } from "./libs/schema/schema-codec.js"
export type {
  AjvValidateLike,
  AjvErrorLike,
  AjvCompilerLike,
  AjvSchemaLike,
} from "./libs/schema/adapters/ajv-adapter.js"
export type {
  AwsGlueSchemaLookup,
  AwsGlueSchemaFetcherResult,
  AwsGlueSchemaFetcherLike,
  AwsGlueSchemaRegistryAdapter,
  AwsGlueCodecCacheOptions,
  AwsGlueClientLike,
  AwsGlueStaticCredentials,
  CreateAwsGlueSchemaRegistryAdapterParams,
  AwsGlueAdapterPreload,
  AwsGlueZodSchemaFactory,
  AwsGlueZodSchemaFactoryParams,
  AwsGlueResolvedSchemaCacheEntry,
  AwsGlueCompiledSchemaCacheEntry,
} from "./libs/schema/adapters/aws-glue-adapter.js"
export type { ZodSchemaLike } from "./libs/schema/adapters/zod-adapter.js"

export {
  KTMessageQueue,
  KTTopic,
  KTTopicBatch,
  DLQKTTopic,
  KTHandler,
  KafkaClientId,
  KafkaMessageKey,
  KafkaTopicName,
  ConsumerSubscribeError,
  KTRetryError,
  NoHandlersError,
  UnableDecreasePartitionsError,
  ArgumentIsRequired,
  ProducerNotInitializedError,
  ProducerInitRequiredForDLQError,
  CustomPartitioner,
  CreateKTTopic,
  CreateKTTopicBatch,
  createAjvCodec,
  createAjvCodecFromSchema,
  createAwsGlueCodec,
  clearAwsGlueSchemaCache,
  createAwsGlueSchemaRegistryAdapter,
  preloadAwsGlueSchemas,
  createZodCodec,
  KTSchemaValidationError,
  KTSchemaRegistryError,
}
export { CreateKTJob } from "./bullmq/job.js"
export type {
  KTJobEvent,
  KTJobSettings,
  KTJobOptions,
  KTJobMeta,
  KTJobData,
  KTJobPayload,
  KTJobScheduler,
  KTPayloadFromJob,
} from "./bullmq/job.js"
export { KTJobHandler } from "./bullmq/consumer-handler.js"
export type {
  KTJobRun,
  KTJobHandlerParams,
  KTJobHandlerOptions,
  KTJobFailureParams,
  KTJobFailureHandler,
} from "./bullmq/consumer-handler.js"
export type {
  KTBullMQProducerConfig,
  KTBullMQConsumerConfig,
  KTBullMQShutdownOptions,
} from "./bullmq/index.js"
export { UnrecoverableError as UnrecoverableJobError } from "bullmq"
export { BullMQProducerNotInitializedError, NoJobHandlersError } from "./custom-errors/bullmq-errors.js"
export type { KTPublisher } from "./message-queue/publisher.js"
export type { KTLogger } from "./libs/helpers/logger.js"
