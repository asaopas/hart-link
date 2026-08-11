use crate::operation::{
    CommandCode, DeviceReply, Operation, OperationError, PayloadReader, PayloadWriter,
};

/// Short address and loop-current mode configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollingAddress {
    /// Short address in the `0..=63` range.
    pub node: u8,
    /// Whether loop current is enabled independently of the address.
    pub loop_current_enabled: bool,
}

impl PollingAddress {
    /// Creates a validated HART 6/7 polling-address configuration.
    pub const fn new(node: u8, loop_current_enabled: bool) -> Result<Self, OperationError> {
        if node > 63 {
            return Err(OperationError::InvalidValue("polling address"));
        }
        Ok(Self {
            node,
            loop_current_enabled,
        })
    }
}

/// Changes the short polling address using the two-byte HART 6/7 Command 6 request.
#[derive(Debug, Clone, Copy)]
pub struct WritePollingAddress {
    /// New configuration.
    pub value: PollingAddress,
}

impl WritePollingAddress {
    /// Creates a validated HART 6/7 Command 6 operation.
    pub fn new(node: u8, loop_current_enabled: bool) -> Result<Self, OperationError> {
        Ok(Self {
            value: PollingAddress::new(node, loop_current_enabled)?,
        })
    }
}

/// Changes the short polling address using the one-byte HART 5 Command 6 request.
#[derive(Debug, Clone, Copy)]
pub struct WriteLegacyPollingAddress {
    /// New address in `0..=15` for a conforming HART 5 multidrop loop.
    pub node: u8,
}

impl WriteLegacyPollingAddress {
    /// Creates a validated one-byte HART 5 Command 6 operation.
    pub const fn new(node: u8) -> Result<Self, OperationError> {
        if node > 15 {
            return Err(OperationError::InvalidValue("HART 5 polling address"));
        }
        Ok(Self { node })
    }
}

/// Reads the complete address and loop-current configuration with Command 7.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadLoopConfiguration;

/// Command 7 response preserving an unknown mode code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoopConfiguration {
    /// Short address.
    pub node: u8,
    /// Loop-current mode code, when supplied by the device.
    pub loop_current_mode: Option<u8>,
}

impl Operation for ReadLoopConfiguration {
    type Output = LoopConfiguration;

    fn command(&self) -> CommandCode {
        CommandCode::new(7)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let node = reader.u8("polling address")? & 0x3f;
        let loop_current_mode = (reader.remaining() > 0)
            .then(|| reader.u8("loop-current mode"))
            .transpose()?;
        reader.finish()?;
        Ok(LoopConfiguration {
            node,
            loop_current_mode,
        })
    }
}

impl Operation for WritePollingAddress {
    type Output = PollingAddress;

    fn command(&self) -> CommandCode {
        CommandCode::new(6)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        if self.value.node > 63 {
            return Err(OperationError::InvalidValue("polling address"));
        }
        writer.u8(self.value.node);
        writer.u8(u8::from(self.value.loop_current_enabled));
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_polling_address(&reply.data)
    }
}

impl Operation for WriteLegacyPollingAddress {
    type Output = PollingAddress;

    fn command(&self) -> CommandCode {
        CommandCode::new(6)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        if self.node > 15 {
            return Err(OperationError::InvalidValue("HART 5 polling address"));
        }
        writer.u8(self.node);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_polling_address(&reply.data)
    }
}

fn decode_polling_address(data: &[u8]) -> Result<PollingAddress, OperationError> {
    let mut reader = PayloadReader::new(data);
    let node = reader.u8("polling address")? & 0x3f;
    let loop_current_enabled = if reader.remaining() == 0 {
        node == 0
    } else {
        reader.u8("loop-current mode")? != 0
    };
    reader.finish()?;
    Ok(PollingAddress {
        node,
        loop_current_enabled,
    })
}
