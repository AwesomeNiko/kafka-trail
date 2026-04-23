import nativeBinding from "../../native/lz4/index.cjs";

export type lz4Codec = typeof lz4Codec

type NativeLz4Binding = {
  compress(input: Buffer): Buffer;
  decompress(input: Buffer): Buffer;
}

const binding = nativeBinding as NativeLz4Binding;

const lz4Codec = {
  compress(encoder: Buffer) {
    return binding.compress(encoder);
  },

  decompress<T>(buffer: Buffer) {
    return binding.decompress(buffer) as T;
  },
};

export { lz4Codec };
