use alloc::vec::Vec;

use crate::operation::{
    CommandCode, DeviceReply, Operation, OperationError, PayloadReader, PayloadWriter,
};

/// Reads primary transducer information with Command 14.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadTransducerInformation;

/// Reads primary output information with Command 15.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadOutputInformation;

macro_rules! raw_information_operation {
    ($type:ty, $command:literal) => {
        impl Operation for $type {
            type Output = Vec<u8>;

            fn command(&self) -> CommandCode {
                CommandCode::new($command)
            }

            fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
                Ok(())
            }

            fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
                reply.require_success(self.command())?;
                if reply.data.is_empty() {
                    return Err(OperationError::InvalidValue("empty device information"));
                }
                Ok(reply.data.clone())
            }
        }
    };
}

raw_information_operation!(ReadTransducerInformation, 14);
raw_information_operation!(ReadOutputInformation, 15);

/// Reads the 24-bit final assembly number with Command 16.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadFinalAssemblyNumber;

impl Operation for ReadFinalAssemblyNumber {
    type Output = u32;

    fn command(&self) -> CommandCode {
        CommandCode::new(16)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let number = reader.u24("final assembly number")?;
        reader.finish()?;
        Ok(number)
    }
}

/// Writes the 24-bit final assembly number with Command 19.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteFinalAssemblyNumber(pub u32);

impl WriteFinalAssemblyNumber {
    /// Creates a value that fits the 24-bit Command 19 field.
    pub const fn new(value: u32) -> Result<Self, OperationError> {
        if value > 0x00ff_ffff {
            return Err(OperationError::InvalidValue("24-bit integer"));
        }
        Ok(Self(value))
    }
}

impl Operation for WriteFinalAssemblyNumber {
    type Output = u32;

    fn command(&self) -> CommandCode {
        CommandCode::new(19)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.u24(self.0)
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if reply.data.is_empty() {
            return Ok(self.0);
        }
        let mut reader = PayloadReader::new(&reply.data);
        let number = reader.u24("final assembly number")?;
        reader.finish()?;
        Ok(number)
    }
}
