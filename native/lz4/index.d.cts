declare const nativeBinding: {
  compress(input: Buffer): Buffer;
  decompress(input: Buffer): Buffer;
};

export = nativeBinding;
