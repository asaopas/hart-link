use alloc::vec::Vec;

use crate::operation::OperationError;

/// Sequential writer for values in network byte order.
#[derive(Debug, Clone, Default)]
pub struct PayloadWriter {
    bytes: Vec<u8>,
}

impl PayloadWriter {
    /// Creates an empty writer.
    pub const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    /// Appends one byte.
    pub fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    /// Appends a 16-bit integer in network byte order.
    pub fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Appends a 24-bit integer in network byte order.
    pub fn u24(&mut self, value: u32) -> Result<(), OperationError> {
        if value > 0x00ff_ffff {
            return Err(OperationError::InvalidValue("24-bit integer"));
        }
        self.bytes.extend_from_slice(&value.to_be_bytes()[1..]);
        Ok(())
    }

    /// Appends a 32-bit integer in network byte order.
    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Appends a floating-point value in network byte order.
    pub fn f32(&mut self, value: f32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    /// Appends an unchanged byte sequence.
    pub fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    /// Finishes writing and returns the data.
    pub fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

/// Sequential reader for values in network byte order.
#[derive(Debug, Clone, Copy)]
pub struct PayloadReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    /// Creates a cursor at the beginning of the data.
    pub const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Returns the current position.
    pub const fn position(&self) -> usize {
        self.offset
    }

    /// Returns the number of unread bytes.
    pub const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    /// Reads one byte.
    pub fn u8(&mut self, field: &'static str) -> Result<u8, OperationError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or(OperationError::Truncated {
                field,
                offset: self.offset,
            })?;
        self.offset += 1;
        Ok(value)
    }

    /// Reads a 16-bit integer.
    pub fn u16(&mut self, field: &'static str) -> Result<u16, OperationError> {
        let bytes = self.take_array::<2>(field)?;
        Ok(u16::from_be_bytes(bytes))
    }

    /// Reads a 24-bit integer.
    pub fn u24(&mut self, field: &'static str) -> Result<u32, OperationError> {
        let bytes = self.take_array::<3>(field)?;
        Ok((u32::from(bytes[0]) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]))
    }

    /// Reads a 32-bit integer.
    pub fn u32(&mut self, field: &'static str) -> Result<u32, OperationError> {
        Ok(u32::from_be_bytes(self.take_array::<4>(field)?))
    }

    /// Reads a floating-point value.
    pub fn f32(&mut self, field: &'static str) -> Result<f32, OperationError> {
        Ok(f32::from_be_bytes(self.take_array::<4>(field)?))
    }

    /// Reads the specified number of bytes.
    pub fn bytes(
        &mut self,
        length: usize,
        field: &'static str,
    ) -> Result<&'a [u8], OperationError> {
        let end = self.offset.saturating_add(length);
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(OperationError::Truncated {
                field,
                offset: self.offset,
            })?;
        self.offset = end;
        Ok(value)
    }

    /// Returns the entire unread tail.
    pub fn rest(&mut self) -> &'a [u8] {
        let rest = &self.bytes[self.offset..];
        self.offset = self.bytes.len();
        rest
    }

    /// Verifies that all data was consumed.
    pub fn finish(self) -> Result<(), OperationError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(OperationError::TrailingBytes(self.remaining()))
        }
    }

    fn take_array<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], OperationError> {
        let value = self.bytes(N, field)?;
        let mut output = [0; N];
        output.copy_from_slice(value);
        Ok(output)
    }
}
