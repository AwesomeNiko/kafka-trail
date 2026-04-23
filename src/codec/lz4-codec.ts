import nativeBinding from "../../native/lz4/index.cjs";

export type lz4Codec = typeof lz4Codec

type NativeLz4Binding = {
  compress(input: Buffer): Buffer;
  decompress(input: Buffer): Buffer;
}

const binding = nativeBinding as NativeLz4Binding;

const lz4Codec = {
  compress(encoder: { buffer: ArrayBufferLike }) {
    return Buffer.from(binding.compress(Buffer.from(encoder.buffer)));
  },

  decompress<T>(buffer: Buffer) {
    return Buffer.from(binding.decompress(buffer)) as T;
  },
};

export { lz4Codec };
