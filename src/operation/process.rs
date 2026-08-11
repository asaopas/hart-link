use alloc::vec::Vec;

use crate::operation::{
    CommandCode, DeviceReply, Operation, OperationError, PayloadReader, PayloadWriter,
};

/// Requests the primary process variable.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadPrimaryValue;

/// Primary process variable together with its unit code.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PrimaryValue {
    /// Unit code from the HART common table.
    pub unit: u8,
    /// Value in the specified unit.
    pub value: f32,
}

/// A value together with its unit code.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MeasuredValue {
    /// Unit code.
    pub unit: u8,
    /// Numeric value.
    pub value: f32,
}

/// Requests loop current and available dynamic variables with Command 3.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadDynamicValues;

/// Command 3 response containing a variable number of PV/SV/TV/QV values.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicValues {
    /// Loop current in milliamperes.
    pub loop_current: f32,
    /// One to four dynamic variables.
    pub values: Vec<MeasuredValue>,
}

impl Operation for ReadDynamicValues {
    type Output = DynamicValues;

    fn command(&self) -> CommandCode {
        CommandCode::new(3)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if reply.data.len() < 9 || !(reply.data.len() - 4).is_multiple_of(5) {
            return Err(OperationError::InvalidValue(
                "dynamic-variable payload length",
            ));
        }
        let mut reader = PayloadReader::new(&reply.data);
        let loop_current = reader.f32("loop current")?;
        let mut values = Vec::new();
        while reader.remaining() >= 5 && values.len() < 4 {
            values.push(MeasuredValue {
                unit: reader.u8("dynamic-variable unit")?,
                value: reader.f32("dynamic variable")?,
            });
        }
        reader.finish()?;
        Ok(DynamicValues {
            loop_current,
            values,
        })
    }
}

/// Reads PV/SV/TV/QV classifications with Command 8.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadVariableClassifications;

/// One to four dynamic-variable classifications.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableClassifications {
    /// Codes in PV, SV, TV, QV order.
    pub classes: Vec<u8>,
}

impl Operation for ReadVariableClassifications {
    type Output = VariableClassifications;

    fn command(&self) -> CommandCode {
        CommandCode::new(8)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if !(1..=4).contains(&reply.data.len()) {
            return Err(OperationError::InvalidValue("classification count"));
        }
        Ok(VariableClassifications {
            classes: reply.data.clone(),
        })
    }
}

/// Requests up to eight device variables with Command 9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDeviceVariables {
    /// Requested slot codes.
    pub slots: Vec<u8>,
}

/// One variable returned by Command 9.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DeviceVariable {
    /// Variable code.
    pub code: u8,
    /// Classification code.
    pub classification: u8,
    /// Unit code.
    pub unit: u8,
    /// Value.
    pub value: f32,
    /// Variable status bits.
    pub status: u8,
}

/// Command 9 response.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceVariables {
    /// Extended field-device status.
    pub extended_status: u8,
    /// Decoded variables.
    pub variables: Vec<DeviceVariable>,
}

impl ReadDeviceVariables {
    /// Creates a request for one to eight slots.
    pub fn new(slots: impl Into<Vec<u8>>) -> Result<Self, OperationError> {
        let slots = slots.into();
        if !(1..=8).contains(&slots.len()) {
            return Err(OperationError::RequestLength {
                command: 9,
                actual: slots.len(),
            });
        }
        Ok(Self { slots })
    }
}

impl Operation for ReadDeviceVariables {
    type Output = DeviceVariables;

    fn command(&self) -> CommandCode {
        CommandCode::new(9)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.slots);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if reply.data.is_empty() || !(reply.data.len() - 1).is_multiple_of(8) {
            return Err(OperationError::InvalidValue(
                "device-variable payload length",
            ));
        }
        let mut reader = PayloadReader::new(&reply.data);
        let extended_status = reader.u8("extended status")?;
        let mut variables = Vec::new();
        while reader.remaining() >= 8 && variables.len() < 8 {
            variables.push(DeviceVariable {
                code: reader.u8("variable code")?,
                classification: reader.u8("variable classification")?,
                unit: reader.u8("variable unit")?,
                value: reader.f32("variable value")?,
                status: reader.u8("variable status")?,
            });
        }
        reader.finish()?;
        Ok(DeviceVariables {
            extended_status,
            variables,
        })
    }
}

/// Reads one to four selected variables with Command 33.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSelectedVariables {
    /// Variable codes in the requested order.
    pub variables: Vec<u8>,
}

/// One value returned by Command 33.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SelectedVariable {
    /// Variable code.
    pub code: u8,
    /// Unit code.
    pub unit: u8,
    /// Value.
    pub value: f32,
}

impl ReadSelectedVariables {
    /// Validates the number of selected variables.
    pub fn new(variables: impl Into<Vec<u8>>) -> Result<Self, OperationError> {
        let variables = variables.into();
        if !(1..=4).contains(&variables.len()) {
            return Err(OperationError::RequestLength {
                command: 33,
                actual: variables.len(),
            });
        }
        Ok(Self { variables })
    }
}

impl Operation for ReadSelectedVariables {
    type Output = Vec<SelectedVariable>;

    fn command(&self) -> CommandCode {
        CommandCode::new(33)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.variables);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if reply.data.is_empty() || !reply.data.len().is_multiple_of(6) || reply.data.len() > 24 {
            return Err(OperationError::InvalidValue(
                "selected-variable payload length",
            ));
        }
        let mut reader = PayloadReader::new(&reply.data);
        let mut output = Vec::new();
        while reader.remaining() >= 6 {
            output.push(SelectedVariable {
                code: reader.u8("selected-variable code")?,
                unit: reader.u8("selected-variable unit")?,
                value: reader.f32("selected-variable value")?,
            });
        }
        reader.finish()?;
        Ok(output)
    }
}

impl Operation for ReadPrimaryValue {
    type Output = PrimaryValue;

    fn command(&self) -> CommandCode {
        CommandCode::new(1)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let value = PrimaryValue {
            unit: reader.u8("primary-variable unit")?,
            value: reader.f32("primary variable")?,
        };
        reader.finish()?;
        Ok(value)
    }
}

/// Requests loop current and percent of range.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadLoopSignal;

/// Primary-output electrical signal.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoopSignal {
    /// Loop current in milliamperes.
    pub milliamps: f32,
    /// Percent of the configured range.
    pub percent: f32,
}

impl Operation for ReadLoopSignal {
    type Output = LoopSignal;

    fn command(&self) -> CommandCode {
        CommandCode::new(2)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        let mut reader = PayloadReader::new(&reply.data);
        let signal = LoopSignal {
            milliamps: reader.f32("loop current")?,
            percent: reader.f32("percent of range")?,
        };
        reader.finish()?;
        Ok(signal)
    }
}
