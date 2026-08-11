use thiserror::Error;
use zeroize::Zeroize;

/// Secret 128-bit key whose memory is cleared on drop.
pub struct KeyMaterial {
    bytes: [u8; 16],
}

impl KeyMaterial {
    /// Creates a key from an array whose ownership is transferred to the value.
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self { bytes }
    }

    /// Copies a key into protected storage and immediately clears the caller's mutable buffer.
    pub fn new_and_zeroize(bytes: &mut [u8; 16]) -> Self {
        let key = Self { bytes: *bytes };
        bytes.zeroize();
        key
    }

    /// Provides temporary byte access to a cryptographic provider.
    pub const fn expose(&self) -> &[u8; 16] {
        &self.bytes
    }
}

impl core::fmt::Debug for KeyMaterial {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("KeyMaterial([redacted])")
    }
}

impl Drop for KeyMaterial {
    fn drop(&mut self) {
        self.bytes.zeroize();
    }
}

/// Sliding counter window for replay protection.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayWindow {
    highest: u32,
    seen: u64,
    initialized: bool,
}

impl ReplayWindow {
    /// Validates a counter and records it when first observed.
    pub fn accept(&mut self, counter: u32) -> Result<(), SecurityError> {
        if !self.initialized {
            self.highest = counter;
            self.seen = 1;
            self.initialized = true;
            return Ok(());
        }
        if counter > self.highest {
            let shift = counter - self.highest;
            self.seen = if shift >= 64 {
                1
            } else {
                (self.seen << shift) | 1
            };
            self.highest = counter;
            return Ok(());
        }
        let age = self.highest - counter;
        if age >= 64 {
            return Err(SecurityError::TooOld(counter));
        }
        let mask = 1u64 << age;
        if self.seen & mask != 0 {
            return Err(SecurityError::Replay(counter));
        }
        self.seen |= mask;
        Ok(())
    }

    /// Returns the greatest accepted counter.
    pub fn highest(&self) -> Option<u32> {
        self.initialized.then_some(self.highest)
    }
}

/// Network-message security validation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SecurityError {
    /// The counter was already observed.
    #[error("counter {0} was already accepted")]
    Replay(u32),
    /// The counter is outside the replay window.
    #[error("counter {0} is too old")]
    TooOld(u32),
}
