import { ConsumerSubscribeError, KTRetryError, NoHandlersError, UnableDecreasePartitionsError, ArgumentIsRequired, ProducerNotInitializedError, ProducerInitRequiredForDLQError } from "./custom-errors/kafka-errors.js"
import { KTHandler } from "./kafka/consumer-handler.js";
import { KTCompressionTypes } from "./kafka/kafka-types.js";
import { CreateKTTopicBatch } from "./kafka/topic-batch.js"
import { CreateKTTopic } from "./kafka/topic.js";
import { KafkaClientId, KafkaMessageKey, KafkaTopicName } from "./libs/branded-types/kafka/index.js";
import { createAjvCodec, createAjvCodecFromSchema } from "./libs/schema/adapters/ajv-adapter.js";
import { createAwsGlueCodec, clearAwsGlueSchemaCache, createAwsGlueSchemaRegistryAdapter, preloadAwsGlueSchemas } from "./libs/schema/adapters/aws-glue-adapter.js";
import { createZodCodec } from "./libs/schema/adapters/zod-adapter.js";
import { KTSchemaRegistryError, KTSchemaValidationError } from "./libs/schema/schema-errors.js";
import { KTMessageQueue } from "./message-queue/index.js";

export type { KTPayloadFromTopic, KTTopicEvent } from "./kafka/topic.js"
export type {
  KTCompressionType,
  KTHeaders,
  KTTopicConfig,
  KTPureKafkaConfig,
} from "./kafka/kafka-types.js"
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
  KTCompressionTypes,
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
