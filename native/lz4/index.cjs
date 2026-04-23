const fs = require("node:fs");
const path = require("node:path");

const loadErrors = [];

function isMusl() {
  if (process.platform !== "linux") {
    return false;
  }

  try {
    return fs.readFileSync("/usr/bin/ldd", "utf8").includes("musl");
  } catch {}

  if (typeof process.report?.getReport === "function") {
    const report = process.report.getReport();

    if (report?.header?.glibcVersionRuntime) {
      return false;
    }

    if (Array.isArray(report?.sharedObjects)) {
      return report.sharedObjects.some((file) => file.includes("musl"));
    }
  }

  return false;
}

function getBindingCandidates() {
  const name = "kafka_trail_lz4";

  if (process.platform === "darwin") {
    return [`${name}.darwin-${process.arch}.node`, `${name}.node`];
  }

  if (process.platform === "win32") {
    return [`${name}.win32-${process.arch}-msvc.node`, `${name}.node`];
  }

  if (process.platform === "linux") {
    const libc = isMusl() ? "musl" : "gnu";
    return [`${name}.linux-${process.arch}-${libc}.node`, `${name}.node`];
  }

  return [`${name}.node`];
}

for (const fileName of getBindingCandidates()) {
  try {
    module.exports = require(path.join(__dirname, fileName));
    return;
  } catch (error) {
    loadErrors.push(error);
  }
}

const message = [
  "Native LZ4 binding is not available for this platform.",
  `Tried: ${getBindingCandidates().join(", ")}.`,
  "If you are publishing the package, make sure the release workflow attached the prebuilt native binaries.",
].join(" ");

const error = new Error(message);
error.cause = loadErrors.length > 0 ? loadErrors[loadErrors.length - 1] : undefined;

throw error;
