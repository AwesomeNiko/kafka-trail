import type { KTCustomPartitionerArgs } from "./kafka-types.js";

const roundRobin = () => {
  let msgNumber = 0
  const byNumber = roundRobinNumber()

  return (params: KTCustomPartitionerArgs) => {
    if (params.message.key) {
      return byNumber(params)
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
  return (params: KTCustomPartitionerArgs) => {
    const key = Buffer.isBuffer(params.message.key)
      ? params.message.key.toString()
      : params.message.key

    if (!key) {
      return 0
    }

    const keyNumber = parseInt(String(key));

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
