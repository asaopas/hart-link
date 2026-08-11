use thiserror::Error;

/// Largest transfer representable by 65,536 maximum-size blocks.
pub const MAXIMUM_TRANSFER_BYTES: usize = 65_536 * 251;

/// One sequential application-transfer block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferBlock {
    /// Zero-based number.
    pub sequence: u16,
    /// Last-block marker.
    pub last: bool,
    /// Data without the service header.
    pub data: Vec<u8>,
}

/// Block-transfer progress state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferProgress {
    /// Next expected number.
    pub next_sequence: u16,
    /// Number of acknowledged bytes.
    pub transferred_bytes: usize,
    /// Whether the transfer is complete.
    pub complete: bool,
}

/// Splits source data into bounded sequential blocks.
#[derive(Debug, Clone)]
pub struct BlockSender<'a> {
    source: &'a [u8],
    block_size: usize,
    offset: usize,
    sequence: u16,
    complete: bool,
}

impl<'a> BlockSender<'a> {
    /// Creates a sender with a block size in `1..=251`.
    pub fn new(source: &'a [u8], block_size: usize) -> Result<Self, TransferError> {
        if !(1..=251).contains(&block_size) {
            return Err(TransferError::BlockSize(block_size));
        }
        if source.len() > MAXIMUM_TRANSFER_BYTES {
            return Err(TransferError::Limit);
        }
        let required_blocks = source.len().max(1).div_ceil(block_size);
        if required_blocks > usize::from(u16::MAX) + 1 {
            return Err(TransferError::SequenceSpace);
        }
        Ok(Self {
            source,
            block_size,
            offset: 0,
            sequence: 0,
            complete: false,
        })
    }

    /// Returns the next block. An empty source produces one final empty block.
    pub fn next_block(&mut self) -> Result<Option<TransferBlock>, TransferError> {
        if self.complete {
            return Ok(None);
        }
        let end = self
            .offset
            .saturating_add(self.block_size)
            .min(self.source.len());
        let last = end == self.source.len();
        let block = TransferBlock {
            sequence: self.sequence,
            last,
            data: self.source[self.offset..end].to_vec(),
        };
        self.offset = end;
        self.complete = last;
        if !last {
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or(TransferError::SequenceSpace)?;
        }
        Ok(Some(block))
    }

    /// Returns the sender state.
    pub fn progress(&self) -> TransferProgress {
        TransferProgress {
            next_sequence: self.sequence,
            transferred_bytes: self.offset.min(self.source.len()),
            complete: self.complete,
        }
    }
}

/// Receiver that enforces ordering and a total memory limit.
#[derive(Debug, Clone)]
pub struct BlockReceiver {
    bytes: Vec<u8>,
    maximum_bytes: usize,
    next_sequence: u16,
    complete: bool,
}

impl BlockReceiver {
    /// Creates an empty receiver.
    pub fn new(maximum_bytes: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum_bytes: maximum_bytes.min(MAXIMUM_TRANSFER_BYTES),
            next_sequence: 0,
            complete: false,
        }
    }

    /// Creates a receiver after rejecting an implausible aggregate byte limit.
    pub fn try_new(maximum_bytes: usize) -> Result<Self, TransferError> {
        if maximum_bytes > MAXIMUM_TRANSFER_BYTES {
            return Err(TransferError::Limit);
        }
        Ok(Self::new(maximum_bytes))
    }

    /// Returns the effective aggregate byte limit.
    pub const fn maximum_bytes(&self) -> usize {
        self.maximum_bytes
    }

    /// Accepts the next block or returns the exact rejection reason.
    pub fn accept(&mut self, block: &TransferBlock) -> Result<TransferProgress, TransferError> {
        if self.complete {
            return Err(TransferError::AlreadyComplete);
        }
        if block.data.len() > 251 {
            return Err(TransferError::BlockDataSize(block.data.len()));
        }
        if block.data.is_empty() && !block.last {
            return Err(TransferError::EmptyIntermediateBlock);
        }
        if block.sequence != self.next_sequence {
            return Err(TransferError::OutOfOrder {
                expected: self.next_sequence,
                actual: block.sequence,
            });
        }
        let new_len = self
            .bytes
            .len()
            .checked_add(block.data.len())
            .ok_or(TransferError::Limit)?;
        if new_len > self.maximum_bytes {
            return Err(TransferError::Limit);
        }
        self.bytes.extend_from_slice(&block.data);
        self.complete = block.last;
        if !block.last {
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or(TransferError::SequenceSpace)?;
        }
        Ok(self.progress())
    }

    /// Returns the receiver state.
    pub fn progress(&self) -> TransferProgress {
        TransferProgress {
            next_sequence: self.next_sequence,
            transferred_bytes: self.bytes.len(),
            complete: self.complete,
        }
    }

    /// Extracts data only after the final block has arrived.
    pub fn finish(self) -> Result<Vec<u8>, TransferError> {
        if self.complete {
            Ok(self.bytes)
        } else {
            Err(TransferError::Incomplete)
        }
    }
}

/// Block-transfer state error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum TransferError {
    /// Invalid block size.
    #[error("block size {0} is outside 1..=251")]
    BlockSize(usize),
    /// A received block exceeds the application payload space.
    #[error("block contains {0} data bytes and exceeds the 251-byte limit")]
    BlockDataSize(usize),
    /// A non-final block must advance the retained payload.
    #[error("a non-final transfer block cannot be empty")]
    EmptyIntermediateBlock,
    /// A block number does not match the expected number.
    #[error("expected block {expected}, received {actual}")]
    OutOfOrder {
        /// Expected number.
        expected: u16,
        /// Received number.
        actual: u16,
    },
    /// The total memory limit was exceeded.
    #[error("block-transfer data limit exceeded")]
    Limit,
    /// The transfer is already complete.
    #[error("transfer is already complete")]
    AlreadyComplete,
    /// The final block has not arrived yet.
    #[error("transfer is not complete")]
    Incomplete,
    /// The 16-bit block-number space was exhausted.
    #[error("block-number space exhausted")]
    SequenceSpace,
}
