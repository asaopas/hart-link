//! Canonical HART frame representation and streaming decoder.

mod address;
mod decoder;
mod frame;

pub use address::{Address, Master};
pub use decoder::{
    ChecksumPolicy, DecodeEvent, DecodeLimits, DecodeLimitsError, DecodeStatistics, FrameDecoder,
    MAXIMUM_DECODE_BUFFER_CAPACITY, MAXIMUM_DECODE_EVENTS_PER_PUSH,
};
pub use frame::{Frame, FrameKind, FrameRepair, MAX_ENCODED_FRAME_SIZE, PhysicalLayer, WireError};

pub(crate) fn xor_checksum(bytes: &[u8]) -> u8 {
    bytes.iter().fold(0, |sum, byte| sum ^ byte)
}
