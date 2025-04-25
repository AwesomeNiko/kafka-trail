import type {  PartitionerArgs } from "kafkajs";

const roundRobin = () => {
  let msgNumber = 0

  return (params: PartitionerArgs) => {
    if (params.message.key) {
      return roundRobinNumber()(params)
    }

    const specifiedPartitionNumber = msgNumber % params.partitionMetadata.length

    if (msgNumber > 0 && specifiedPartitionNumber === 0) {
      msgNumber = 1
    } else {
      msgNumber +=1
    }

    return specifiedPartitionNumber
  }
}

const roundRobinNumber = () => {
  return (params: PartitionerArgs) => {
    if (Buffer.isBuffer(params.message.key)) {
      params.message.key = params.message.key.toString();
    }

    const key = params.message.key

    if (!key) {
      return 0
    }

    const keyNumber = parseInt(key);

    if (isNaN(keyNumber)) {
      return 0;
    }

    return keyNumber % params.partitionMetadata.length;
  };
}

const CustomPartitioner = {
  roundRobinNumber,
  roundRobin,
}
export { CustomPartitioner };
