import type { Branded } from "../branded/index.js";

// ClientID
export type KafkaClientId = Branded<string, 'kafkaClientId'>
export const KafkaClientId = {
  new() {
    return Date.now().toString() as KafkaClientId
  },
  fromString(clientId: string) {
    if (!clientId) {
      throw new Error("clientId required")
    }
      
    return clientId as KafkaClientId
  },
}

//Topic name
export type KafkaTopicName = Branded<string, 'kafkaTopicName'>;
export const KafkaTopicName = {
  fromString(topicName: string) {
    if (!topicName) {
      throw new Error("topicName required")
    }

    return topicName as KafkaTopicName;
  },
};

export type KafkaMessageKey = Branded<string, 'kafkaMessageKey'> | null
export const KafkaMessageKey = {
  NULL: null,
  fromString(messageKey?: string) {
    return (messageKey ?? null) as KafkaMessageKey
  },
}