use alloc::{collections::VecDeque, vec::Vec};

use crate::wire::{Address, Frame, FrameKind, FrameRepair, PhysicalLayer, WireError, xor_checksum};

/// Hard upper bound for an incomplete streaming-decoder tail.
pub const MAXIMUM_DECODE_BUFFER_CAPACITY: usize = 16 * 1024 * 1024;
/// Largest number of frame or rejection events produced by one input fragment.
pub const MAXIMUM_DECODE_EVENTS_PER_PUSH: usize = 4096;

/// Strictly bounded compatibility policy for a confirmed gateway behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ChecksumPolicy {
    /// Reject every checksum mismatch.
    #[default]
    Strict,
    /// Allow only explicitly listed hardware distortions.
    KnownGateway {
        /// Allow alteration of the reserved `0x10` delimiter bit.
        delimiter_bit: bool,
        /// Allow a checksum calculated before the specified short address was rewritten.
        checksum_source_node: Option<u8>,
    },
}

/// Streaming decoder limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum number of buffered bytes between calls.
    pub buffer_capacity: usize,
    /// Minimum number of preambles required to recognize a frame.
    pub minimum_preambles: u8,
    /// Checksum and known-distortion policy.
    pub checksum_policy: ChecksumPolicy,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            buffer_capacity: 4096,
            minimum_preambles: 2,
            checksum_policy: ChecksumPolicy::Strict,
        }
    }
}

impl DecodeLimits {
    /// Sets the maximum retained incomplete stream tail.
    pub const fn with_buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity = buffer_capacity;
        self
    }

    /// Sets the minimum preamble run required before a delimiter.
    pub const fn with_minimum_preambles(mut self, minimum_preambles: u8) -> Self {
        self.minimum_preambles = minimum_preambles;
        self
    }

    /// Sets the checksum acceptance policy.
    pub const fn with_checksum_policy(mut self, checksum_policy: ChecksumPolicy) -> Self {
        self.checksum_policy = checksum_policy;
        self
    }

    /// Validates direct decoder resource and compatibility settings.
    pub const fn validate(self) -> Result<(), DecodeLimitsError> {
        if self.buffer_capacity == 0 {
            return Err(DecodeLimitsError::EmptyBuffer);
        }
        if self.buffer_capacity > MAXIMUM_DECODE_BUFFER_CAPACITY {
            return Err(DecodeLimitsError::BufferLimit(self.buffer_capacity));
        }
        if self.minimum_preambles == 0 {
            return Err(DecodeLimitsError::NoPreambles);
        }
        if let ChecksumPolicy::KnownGateway {
            checksum_source_node: Some(node),
            ..
        } = self.checksum_policy
            && node > 63
        {
            return Err(DecodeLimitsError::ChecksumSourceNode(node));
        }
        Ok(())
    }
}

/// Invalid direct streaming-decoder configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeLimitsError {
    /// No bytes could be retained between fragments.
    #[error("decoder buffer_capacity must be greater than zero")]
    EmptyBuffer,
    /// The requested retained tail is implausibly large.
    #[error("decoder buffer_capacity {0} exceeds the supported limit")]
    BufferLimit(usize),
    /// A preamble-free candidate policy is not accepted by validated construction.
    #[error("decoder minimum_preambles must be greater than zero")]
    NoPreambles,
    /// A known gateway source node must fit a short HART address.
    #[error("checksum source node {0} is outside 0..=63")]
    ChecksumSourceNode(u8),
}

/// Streaming decoder counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodeStatistics {
    /// Successfully decoded frames.
    pub frames: u64,
    /// Discarded unrelated bytes.
    pub discarded_bytes: u64,
    /// Frames with an invalid checksum.
    pub checksum_failures: u64,
    /// Bytes removed because of the memory limit.
    pub overflow_bytes: u64,
    /// Frames accepted only through the explicit compatibility policy.
    pub repaired_frames: u64,
    /// Input batches truncated after producing too many individual events.
    pub event_limit_hits: u64,
}

/// Result produced while advancing the streaming decoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeEvent {
    /// A complete validated frame was received.
    Frame(Frame),
    /// A damaged frame was found; the decoder continued searching.
    Rejected(WireError),
}

/// Decoder that accepts arbitrary transport-stream fragments.
#[derive(Debug, Clone)]
pub struct FrameDecoder {
    bytes: VecDeque<u8>,
    limits: DecodeLimits,
    statistics: DecodeStatistics,
}

enum DecoderStep {
    NoCandidate,
    NeedMore,
    Discard(usize),
    Rejected {
        error: WireError,
        consumed: usize,
        checksum_failure: bool,
    },
    Frame {
        frame: Frame,
        consumed: usize,
        repaired: bool,
    },
}

impl FrameDecoder {
    /// Creates a decoder with the specified memory limit.
    pub fn new(limits: DecodeLimits) -> Self {
        let limits = DecodeLimits {
            buffer_capacity: limits
                .buffer_capacity
                .clamp(1, MAXIMUM_DECODE_BUFFER_CAPACITY),
            minimum_preambles: limits.minimum_preambles.max(1),
            checksum_policy: limits.checksum_policy,
        };
        Self {
            bytes: VecDeque::new(),
            limits,
            statistics: DecodeStatistics::default(),
        }
    }

    /// Creates a decoder after rejecting invalid limits instead of clamping them.
    pub fn try_new(limits: DecodeLimits) -> Result<Self, DecodeLimitsError> {
        limits.validate()?;
        Ok(Self::new(limits))
    }

    /// Returns the effective decoder limits.
    pub const fn limits(&self) -> DecodeLimits {
        self.limits
    }

    /// Adds a fragment and returns all completed events.
    pub fn push(&mut self, fragment: &[u8]) -> Vec<DecodeEvent> {
        self.append_bounded(fragment);
        let mut events = Vec::new();
        while events.len() < MAXIMUM_DECODE_EVENTS_PER_PUSH
            && let Some(event) = self.next_event()
        {
            events.push(event);
        }
        if events.len() == MAXIMUM_DECODE_EVENTS_PER_PUSH && !self.bytes.is_empty() {
            self.statistics.event_limit_hits = self.statistics.event_limit_hits.saturating_add(1);
            self.retain_possible_preambles();
        }
        events
    }

    /// Returns accumulated counters.
    pub const fn statistics(&self) -> DecodeStatistics {
        self.statistics
    }

    /// Returns the number of incomplete buffered bytes.
    pub fn buffered_len(&self) -> usize {
        self.bytes.len()
    }

    /// Removes the incomplete stream tail.
    pub fn clear(&mut self) {
        self.bytes.clear();
    }

    fn append_bounded(&mut self, fragment: &[u8]) {
        let overflow = self
            .bytes
            .len()
            .saturating_add(fragment.len())
            .saturating_sub(self.limits.buffer_capacity);
        if overflow == 0 {
            self.bytes.extend(fragment.iter().copied());
            return;
        }

        let from_buffer = overflow.min(self.bytes.len());
        self.bytes.drain(..from_buffer);
        let from_fragment = overflow.saturating_sub(from_buffer).min(fragment.len());
        self.bytes.extend(fragment[from_fragment..].iter().copied());
        self.statistics.overflow_bytes = self
            .statistics
            .overflow_bytes
            .saturating_add(u64::try_from(overflow).unwrap_or(u64::MAX));
    }

    fn next_event(&mut self) -> Option<DecodeEvent> {
        loop {
            let limits = self.limits;
            let step = Self::analyze(self.bytes.make_contiguous(), limits);
            match step {
                DecoderStep::NoCandidate => {
                    self.retain_possible_preambles();
                    return None;
                }
                DecoderStep::NeedMore => return None,
                DecoderStep::Discard(count) => self.discard(count),
                DecoderStep::Rejected {
                    error,
                    consumed,
                    checksum_failure,
                } => {
                    if checksum_failure {
                        self.statistics.checksum_failures =
                            self.statistics.checksum_failures.saturating_add(1);
                    }
                    self.discard(consumed);
                    return Some(DecodeEvent::Rejected(error));
                }
                DecoderStep::Frame {
                    frame,
                    consumed,
                    repaired,
                } => {
                    self.bytes.drain(..consumed);
                    self.statistics.frames = self.statistics.frames.saturating_add(1);
                    if repaired {
                        self.statistics.repaired_frames =
                            self.statistics.repaired_frames.saturating_add(1);
                    }
                    return Some(DecodeEvent::Frame(frame));
                }
            }
        }
    }

    fn analyze(bytes: &[u8], limits: DecodeLimits) -> DecoderStep {
        let Some((delimiter_index, preamble_start, preamble_count)) =
            Self::find_candidate(bytes, limits)
        else {
            return DecoderStep::NoCandidate;
        };
        if preamble_start > 0 {
            return DecoderStep::Discard(preamble_start);
        }

        let mut delimiter = bytes[delimiter_index];
        let mut repair = None;
        if FrameKind::from_delimiter(delimiter).is_none()
            && Self::allow_delimiter_repair(limits.checksum_policy)
            && FrameKind::from_delimiter(delimiter ^ 0x10).is_some()
        {
            delimiter ^= 0x10;
            repair = Some(FrameRepair::DelimiterBit);
        }
        let long = delimiter & 0x80 != 0;
        let address_len = if long { 5 } else { 1 };
        let expansion_len = usize::from((delimiter >> 5) & 0x03);
        let command_index = delimiter_index + 1 + address_len + expansion_len;
        let count_index = command_index + 1;
        if bytes.len() <= count_index {
            return DecoderStep::NeedMore;
        }
        let payload_len = usize::from(bytes[count_index]);
        let checksum_index = count_index + 1 + payload_len;
        if bytes.len() <= checksum_index {
            return DecoderStep::NeedMore;
        }

        let expected = xor_checksum(&bytes[delimiter_index..checksum_index]);
        let actual = bytes[checksum_index];
        let delimiter_checksum_matches =
            Self::allow_delimiter_repair(limits.checksum_policy) && expected ^ 0x10 == actual;
        let address_repair = if expected != actual && !delimiter_checksum_matches && !long {
            Self::short_address_repair(
                limits.checksum_policy,
                bytes[delimiter_index + 1],
                expected,
                actual,
            )
        } else {
            None
        };
        if expected != actual && !delimiter_checksum_matches && address_repair.is_none() {
            return DecoderStep::Rejected {
                error: WireError::Checksum { expected, actual },
                consumed: delimiter_index + 1,
                checksum_failure: true,
            };
        }
        if delimiter_checksum_matches {
            repair = Some(FrameRepair::DelimiterBit);
        }
        if let Some(address_repair) = address_repair {
            repair = Some(address_repair);
        }

        let address_start = delimiter_index + 1;
        let address_end = address_start + address_len;
        let address = match Address::read_from(&bytes[address_start..address_end], long) {
            Ok(address) => address,
            Err(error) => {
                return DecoderStep::Rejected {
                    error,
                    consumed: checksum_index + 1,
                    checksum_failure: false,
                };
            }
        };
        let Some(kind) = FrameKind::from_delimiter(delimiter) else {
            return DecoderStep::Rejected {
                error: WireError::Delimiter(delimiter),
                consumed: delimiter_index + 1,
                checksum_failure: false,
            };
        };
        DecoderStep::Frame {
            frame: Frame {
                preambles: u8::try_from(preamble_count).unwrap_or(u8::MAX),
                kind,
                physical_layer: PhysicalLayer::from_delimiter(delimiter),
                address,
                expansion: bytes[address_end..command_index].to_vec(),
                wire_command: bytes[command_index],
                payload: bytes[count_index + 1..checksum_index].to_vec(),
                repair,
            },
            consumed: checksum_index + 1,
            repaired: repair.is_some(),
        }
    }

    fn find_candidate(bytes: &[u8], limits: DecodeLimits) -> Option<(usize, usize, usize)> {
        let minimum_preambles = usize::from(limits.minimum_preambles);
        bytes.iter().enumerate().find_map(|(index, byte)| {
            if !Self::is_possible_delimiter(*byte, limits.checksum_policy) {
                return None;
            }
            let preamble_start = bytes[..index]
                .iter()
                .rposition(|value| *value != 0xff)
                .map_or(0, |position| position + 1);
            let preamble_count = index - preamble_start;
            let retained_count = preamble_count.min(usize::from(u8::MAX));
            let retained_start = index - retained_count;
            (preamble_count >= minimum_preambles).then_some((index, retained_start, retained_count))
        })
    }

    fn retain_possible_preambles(&mut self) {
        let keep = self
            .bytes
            .iter()
            .rev()
            .take_while(|byte| **byte == 0xff)
            .count()
            .min(usize::from(u8::MAX));
        let discarded = self.bytes.len().saturating_sub(keep);
        self.discard(discarded);
    }

    fn discard(&mut self, count: usize) {
        self.bytes.drain(..count.min(self.bytes.len()));
        self.statistics.discarded_bytes = self
            .statistics
            .discarded_bytes
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
    }

    fn is_possible_delimiter(byte: u8, policy: ChecksumPolicy) -> bool {
        FrameKind::from_delimiter(byte).is_some()
            || (Self::allow_delimiter_repair(policy)
                && FrameKind::from_delimiter(byte ^ 0x10).is_some())
    }

    const fn allow_delimiter_repair(policy: ChecksumPolicy) -> bool {
        matches!(
            policy,
            ChecksumPolicy::KnownGateway {
                delimiter_bit: true,
                ..
            }
        )
    }

    fn short_address_repair(
        policy: ChecksumPolicy,
        received_address: u8,
        expected: u8,
        actual: u8,
    ) -> Option<FrameRepair> {
        let ChecksumPolicy::KnownGateway {
            checksum_source_node: Some(source),
            ..
        } = policy
        else {
            return None;
        };
        if source > 63 {
            return None;
        }
        let reconstructed = received_address ^ expected ^ actual;
        (reconstructed & 0xc0 == received_address & 0xc0 && reconstructed & 0x3f == source)
            .then_some(FrameRepair::ShortAddressRewrite {
                checksum_source_node: source,
            })
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(DecodeLimits::default())
    }
}
