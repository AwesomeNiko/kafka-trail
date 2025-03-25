import lz4 from "lz4";

export type lz4Codec = typeof lz4Codec

const lz4Codec = {
  compress(encoder: Buffer) {
    return lz4.encode(encoder);
  },

  decompress<T>(buffer: Buffer) {
    return lz4.decode(buffer) as T;
  },
};

export { lz4Codec };
