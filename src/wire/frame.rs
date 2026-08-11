use alloc::vec::Vec;

use thiserror::Error;

use crate::wire::{Address, xor_checksum};

/// Largest canonical frame: 255 preambles, long address, three expansion bytes, and 255 payload bytes.
pub const MAX_ENCODED_FRAME_SIZE: usize = 522;

/// Physical layer mode encoded in the frame delimiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhysicalLayer {
    /// 1200 bit/s frequency-shift keying.
    Fsk,
    /// Phase-shift keying used by the high-speed physical layer.
    Psk,
}

impl PhysicalLayer {
    pub(crate) const fn delimiter_bit(self) -> u8 {
        match self {
            Self::Fsk => 0,
            Self::Psk => 0x08,
        }
    }

    pub(crate) const fn from_delimiter(delimiter: u8) -> Self {
        if delimiter & 0x08 == 0 {
            Self::Fsk
        } else {
            Self::Psk
        }
    }
}

/// Frame purpose encoded in the delimiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// Request from a master to a device.
    Request,
    /// Device response to a request.
    Response,
    /// Unsolicited device message.
    Burst,
}

/// Explicit repair applied for a known gateway behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameRepair {
    /// The reserved `0x10` delimiter bit was altered.
    DelimiterBit,
    /// The gateway rewrote the short address without updating the checksum.
    ShortAddressRewrite {
        /// Address used to calculate the checksum.
        checksum_source_node: u8,
    },
}

impl FrameKind {
    pub(crate) const fn delimiter_base(self) -> u8 {
        match self {
            Self::Request => 0x02,
            Self::Response => 0x06,
            Self::Burst => 0x01,
        }
    }

    pub(crate) const fn from_delimiter(delimiter: u8) -> Option<Self> {
        match delimiter & 0x1f {
            0x02 | 0x0a => Some(Self::Request),
            0x06 | 0x0e => Some(Self::Response),
            0x01 | 0x09 => Some(Self::Burst),
            _ => None,
        }
    }
}

/// Error while constructing or decoding a data-link frame.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WireError {
    /// The short address is outside the valid range.
    #[error("short address {0} is outside the 0..=63 range")]
    PollingAddress(u8),
    /// The expanded device type does not fit the address field.
    #[error("expanded device type {0} does not fit in 14 bits")]
    ExpandedDeviceType(u16),
    /// The device identifier does not fit in 24 bits.
    #[error("device identifier {0} does not fit in 24 bits")]
    DeviceIdentifier(u32),
    /// The frame contains more than three expansion bytes.
    #[error("frame contains an invalid expansion length: {0}")]
    ExpansionLength(usize),
    /// The payload does not fit the one-byte length field.
    #[error("payload is too large: {0} bytes")]
    PayloadLength(usize),
    /// The stream ended before the frame was complete.
    #[error("truncated frame")]
    Truncated,
    /// The delimiter does not identify a supported HART frame.
    #[error("unknown delimiter 0x{0:02x}")]
    Delimiter(u8),
    /// The frame checksum does not match.
    #[error("checksum mismatch: expected 0x{expected:02x}, received 0x{actual:02x}")]
    Checksum {
        /// Calculated value.
        expected: u8,
        /// Value received from the line.
        actual: u8,
    },
}

/// Complete data-link frame without unrelated line bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Number of preambles before the delimiter.
    pub preambles: u8,
    /// Frame direction and purpose.
    pub kind: FrameKind,
    /// Frame physical layer.
    pub physical_layer: PhysicalLayer,
    /// Sender or recipient address.
    pub address: Address,
    /// Optional data-link header expansion bytes.
    pub expansion: Vec<u8>,
    /// One-byte command number carried on the wire.
    pub wire_command: u8,
    /// Payload. Responses include the response code and device status.
    pub payload: Vec<u8>,
    /// Known gateway repair applied while decoding, if any.
    pub repair: Option<FrameRepair>,
}

impl Frame {
    /// Encodes the canonical byte sequence including the checksum.
    pub fn encode(&self) -> Result<Vec<u8>, WireError> {
        if self.expansion.len() > 3 {
            return Err(WireError::ExpansionLength(self.expansion.len()));
        }
        let count = u8::try_from(self.payload.len())
            .map_err(|_| WireError::PayloadLength(self.payload.len()))?;
        let long_bit = if self.address.encoded_len() == 5 {
            0x80
        } else {
            0
        };
        let delimiter = self.kind.delimiter_base()
            | self.physical_layer.delimiter_bit()
            | long_bit
            | (u8::try_from(self.expansion.len()).unwrap_or(0) << 5);
        let mut output = Vec::with_capacity(
            usize::from(self.preambles)
                + 1
                + self.address.encoded_len()
                + self.expansion.len()
                + 3
                + self.payload.len(),
        );
        output.resize(usize::from(self.preambles), 0xff);
        let checksum_start = output.len();
        output.push(delimiter);
        self.address
            .write_to(self.kind == FrameKind::Burst, &mut output);
        output.extend_from_slice(&self.expansion);
        output.push(self.wire_command);
        output.push(count);
        output.extend_from_slice(&self.payload);
        output.push(xor_checksum(&output[checksum_start..]));
        Ok(output)
    }

    /// Returns the delimiter corresponding to the frame contents.
    pub fn delimiter(&self) -> Result<u8, WireError> {
        if self.expansion.len() > 3 {
            return Err(WireError::ExpansionLength(self.expansion.len()));
        }
        Ok(self.kind.delimiter_base()
            | self.physical_layer.delimiter_bit()
            | if self.address.encoded_len() == 5 {
                0x80
            } else {
                0
            }
            | (u8::try_from(self.expansion.len()).unwrap_or(0) << 5))
    }
}
