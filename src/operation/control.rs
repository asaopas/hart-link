use alloc::vec::Vec;

use crate::operation::{
    CommandCode, DeviceReply, Operation, OperationError, PayloadReader, PayloadWriter,
};

/// Sets the damping value with Command 34.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SetDamping {
    /// Value in seconds.
    pub seconds: f32,
}

impl SetDamping {
    /// Creates a validated damping update.
    pub fn new(seconds: f32) -> Result<Self, OperationError> {
        if !seconds.is_finite() || seconds < 0.0 {
            return Err(OperationError::InvalidValue("damping value"));
        }
        Ok(Self { seconds })
    }
}

impl Operation for SetDamping {
    type Output = f32;

    fn command(&self) -> CommandCode {
        CommandCode::new(34)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        if !self.seconds.is_finite() || self.seconds < 0.0 {
            return Err(OperationError::InvalidValue("damping value"));
        }
        writer.f32(self.seconds);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let value = reader.f32("actual damping value")?;
        reader.finish()?;
        Ok(value)
    }
}

/// Unit and range limits used by Command 35.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RangeValues {
    /// Unit code.
    pub unit: u8,
    /// Upper limit.
    pub upper: f32,
    /// Lower limit.
    pub lower: f32,
}

impl RangeValues {
    /// Creates finite lower and upper range values without assuming their ordering.
    pub fn new(unit: u8, lower: f32, upper: f32) -> Result<Self, OperationError> {
        if !upper.is_finite() || !lower.is_finite() {
            return Err(OperationError::InvalidValue("range limits"));
        }
        Ok(Self { unit, upper, lower })
    }
}

/// Writes range limits with Command 35.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SetRange(pub RangeValues);

impl SetRange {
    /// Creates a validated Command 35 operation.
    pub fn new(unit: u8, lower: f32, upper: f32) -> Result<Self, OperationError> {
        Ok(Self(RangeValues::new(unit, lower, upper)?))
    }
}

impl Operation for SetRange {
    type Output = RangeValues;

    fn command(&self) -> CommandCode {
        CommandCode::new(35)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        if !self.0.upper.is_finite() || !self.0.lower.is_finite() {
            return Err(OperationError::InvalidValue("range limits"));
        }
        writer.u8(self.0.unit);
        writer.f32(self.0.upper);
        writer.f32(self.0.lower);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let value = RangeValues {
            unit: reader.u8("range unit")?,
            upper: reader.f32("upper range limit")?,
            lower: reader.f32("lower range limit")?,
        };
        reader.finish()?;
        Ok(value)
    }
}

/// Action with no required response data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCommand {
    command: CommandCode,
    data: Vec<u8>,
}

impl ActionCommand {
    /// Creates one of Commands 36, 37, 38, 41, 42, or 43.
    pub fn new(command: u8, data: impl Into<Vec<u8>>) -> Result<Self, OperationError> {
        if !matches!(command, 36..=38 | 41..=43) {
            return Err(OperationError::InvalidValue("action command number"));
        }
        let data = data.into();
        let command = CommandCode::from(command);
        if !crate::request_constraint(command).allows(data.len()) {
            return Err(OperationError::RequestLength {
                command: command.get(),
                actual: data.len(),
            });
        }
        Ok(Self { command, data })
    }
}

impl Operation for ActionCommand {
    type Output = DeviceReply;

    fn command(&self) -> CommandCode {
        self.command
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.data);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        Ok(reply.clone())
    }
}

/// Controls nonvolatile memory with Command 39.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EepromControl(pub u8);

impl Operation for EepromControl {
    type Output = u8;

    fn command(&self) -> CommandCode {
        CommandCode::new(39)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.u8(self.0);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let value = reader.u8("memory control code")?;
        reader.finish()?;
        Ok(value)
    }
}

/// Enters or exits fixed-current mode with Command 40.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FixedCurrent(pub f32);

impl FixedCurrent {
    /// Creates a finite fixed-current request.
    pub fn new(milliamps: f32) -> Result<Self, OperationError> {
        if !milliamps.is_finite() {
            return Err(OperationError::InvalidValue("fixed current"));
        }
        Ok(Self(milliamps))
    }
}

impl Operation for FixedCurrent {
    type Output = f32;

    fn command(&self) -> CommandCode {
        CommandCode::new(40)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        if !self.0.is_finite() {
            return Err(OperationError::InvalidValue("fixed current"));
        }
        writer.f32(self.0);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let value = reader.f32("actual current")?;
        reader.finish()?;
        Ok(value)
    }
}

/// Writes the primary-variable unit with Command 44.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WritePrimaryUnit(pub u8);

impl Operation for WritePrimaryUnit {
    type Output = u8;

    fn command(&self) -> CommandCode {
        CommandCode::new(44)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.u8(self.0);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let value = reader.u8("primary-variable unit")?;
        reader.finish()?;
        Ok(value)
    }
}

/// Trims the current output with Command 45 or 46.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnalogTrim {
    command: CommandCode,
    /// Reference current in milliamperes.
    pub reference_current: f32,
}

impl AnalogTrim {
    /// Creates a zero (`45`) or gain (`46`) trim operation.
    pub fn new(command: u8, reference_current: f32) -> Result<Self, OperationError> {
        if !matches!(command, 45 | 46) || !reference_current.is_finite() {
            return Err(OperationError::InvalidValue("current-output trim"));
        }
        Ok(Self {
            command: CommandCode::from(command),
            reference_current,
        })
    }

    /// Creates a Command 45 zero-trim operation.
    pub fn zero(reference_current: f32) -> Result<Self, OperationError> {
        Self::new(45, reference_current)
    }

    /// Creates a Command 46 gain-trim operation.
    pub fn gain(reference_current: f32) -> Result<Self, OperationError> {
        Self::new(46, reference_current)
    }
}

impl Operation for AnalogTrim {
    type Output = f32;

    fn command(&self) -> CommandCode {
        self.command
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.f32(self.reference_current);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let current = reader.f32("actual trim current")?;
        reader.finish()?;
        Ok(current)
    }
}

/// Writes the transfer function with Command 47.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteTransferFunction(pub u8);

impl Operation for WriteTransferFunction {
    type Output = u8;

    fn command(&self) -> CommandCode {
        CommandCode::new(47)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.u8(self.0);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let function = reader.u8("transfer function")?;
        reader.finish()?;
        Ok(function)
    }
}
