const path = require("node:path");

const bindingPath = path.join(__dirname, "kafka_trail_lz4.node");

try {
  module.exports = require(bindingPath);
} catch (error) {
  const message = [
    "Native LZ4 binding is not available.",
    "Build it with `yarn build:native` before using the default LZ4 codec.",
  ].join(" ");

  error.message = `${message} Original error: ${error.message}`;
  throw error;
}
