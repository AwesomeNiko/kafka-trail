use std::io::{Cursor, Read, Write};

use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

#[napi]
pub fn compress(input: Buffer) -> napi::Result<Buffer> {
  let mut encoder = FrameEncoder::new(Vec::new());

  encoder
    .write_all(input.as_ref())
    .map_err(|error| napi::Error::from_reason(error.to_string()))?;

  let compressed = encoder
    .finish()
    .map_err(|error| napi::Error::from_reason(error.to_string()))?;

  Ok(compressed.into())
}

#[napi]
pub fn decompress(input: Buffer) -> napi::Result<Buffer> {
  let mut decoder = FrameDecoder::new(Cursor::new(input.as_ref()));
  let mut decompressed = Vec::new();

  decoder
    .read_to_end(&mut decompressed)
    .map_err(|error| napi::Error::from_reason(error.to_string()))?;

  Ok(decompressed.into())
}
