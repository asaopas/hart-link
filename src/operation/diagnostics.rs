use alloc::vec::Vec;

use crate::operation::{CommandCode, DeviceReply, Operation, OperationError, PayloadWriter};

/// Requests additional device status.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadAdditionalStatus;

/// Additional status preserving every unknown bit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdditionalStatus {
    /// Status bytes in their original order.
    pub bytes: Vec<u8>,
}

impl Operation for ReadAdditionalStatus {
    type Output = AdditionalStatus;

    fn command(&self) -> CommandCode {
        CommandCode::new(48)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        Ok(AdditionalStatus {
            bytes: reply.data.clone(),
        })
    }
}
