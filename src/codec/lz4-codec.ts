import lz4 from "lz4";

export type lz4Codec = typeof lz4Codec

const lz4Codec = {
  compress(encoder: Buffer) {
    // @ts-expect-error encode type required Buffer, but implementation for a some reason required ArrayBufferLike
    return lz4.encode(encoder.buffer);
  },

  decompress<T>(buffer: Buffer) {
    return lz4.decode(buffer) as T;
  },
};

export { lz4Codec };
