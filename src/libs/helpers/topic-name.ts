import { KafkaTopicName } from "../branded-types/kafka/index.js";

export const CreateDlqTopicName = (topicName: KafkaTopicName | string) => KafkaTopicName.fromString(`dlq.${topicName}`)
