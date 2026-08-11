//! Application-level requests and responses.

mod codec;
mod configuration;
mod control;
mod diagnostics;
mod identity;
mod information;
mod process;
mod text;

use alloc::vec::Vec;

use thiserror::Error;

use crate::{
    catalog::{OperationSafety, command_descriptor},
    wire::{Address, Frame, FrameKind, PhysicalLayer, WireError},
};

pub use codec::{PayloadReader, PayloadWriter};
pub use configuration::{
    LoopConfiguration, PollingAddress, ReadLoopConfiguration, WriteLegacyPollingAddress,
    WritePollingAddress,
};
pub use control::{
    ActionCommand, AnalogTrim, EepromControl, FixedCurrent, RangeValues, SetDamping, SetRange,
    WritePrimaryUnit, WriteTransferFunction,
};
pub use diagnostics::{AdditionalStatus, ReadAdditionalStatus};
pub use identity::ReadIdentity;
pub use identity::{DeviceIdentity, ReadDeviceIdentity};
pub use information::{
    ReadFinalAssemblyNumber, ReadOutputInformation, ReadTransducerInformation,
    WriteFinalAssemblyNumber,
};
pub use process::{
    DeviceVariable, DeviceVariables, DynamicValues, LoopSignal, MeasuredValue, PrimaryValue,
    ReadDeviceVariables, ReadDynamicValues, ReadLoopSignal, ReadPrimaryValue,
    ReadSelectedVariables, ReadVariableClassifications, SelectedVariable, VariableClassifications,
};
pub use text::{
    DeviceDate, ReadLongTag, ReadMessage, ReadTagDescriptorDate, TagDescriptorDate, WriteLongTag,
    WriteMessage, WriteTagDescriptorDate, pack_text, unpack_text,
};

/// Logical HART operation number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandCode(u16);

impl CommandCode {
    /// Creates a number in the full 16-bit command space.
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the numeric value.
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Reports whether the number must be carried through Command 31.
    pub const fn is_expanded(self) -> bool {
        self.0 > u8::MAX as u16
    }
}

impl From<u8> for CommandCode {
    fn from(value: u8) -> Self {
        Self(u16::from(value))
    }
}

impl From<u16> for CommandCode {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

/// Application codec error.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum OperationError {
    /// The response names a different command.
    #[error("expected command {expected}, received command {actual}")]
    CommandMismatch {
        /// Expected logical number.
        expected: u16,
        /// Actual logical number.
        actual: u16,
    },
    /// The device returned an error or warning that prevents payload decoding.
    #[error("device returned response code {code} and status 0x{status:02x}")]
    DeviceStatus {
        /// HART response code.
        code: u8,
        /// Device status bits.
        status: u8,
    },
    /// The first response byte is a communication-error summary, not an application code.
    #[error("device reported communication status 0x{summary:02x} and status 0x{status:02x}")]
    CommunicationStatus {
        /// Communication-error summary with its most significant bit set.
        summary: u8,
        /// Second status byte retained for diagnostics.
        status: u8,
    },
    /// The payload is too short for the declared format.
    #[error("field {field} is truncated at offset {offset}")]
    Truncated {
        /// Application field name.
        field: &'static str,
        /// Read offset.
        offset: usize,
    },
    /// Unexpected data remains after decoding.
    #[error("{0} unexpected bytes remain after decoding")]
    TrailingBytes(usize),
    /// A value cannot be represented by the protocol type.
    #[error("field {0} is outside the allowed range")]
    InvalidValue(&'static str),
    /// The request length does not match the explicit command contract.
    #[error("command {command} does not accept a {actual}-byte request")]
    RequestLength {
        /// Logical command number.
        command: u16,
        /// Actual length.
        actual: usize,
    },
    /// Link-layer error.
    #[error(transparent)]
    Wire(#[from] WireError),
}

/// Request before conversion into a link-layer frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Device address.
    pub address: Address,
    /// Logical command number.
    pub command: CommandCode,
    /// Request data without the expanded-command service number.
    pub data: Vec<u8>,
    /// Preamble count used for the initial transmission.
    pub preambles: u8,
    /// Link-layer header extension.
    pub frame_expansion: Vec<u8>,
    /// Physical mode of the generated frame.
    pub physical_layer: PhysicalLayer,
    /// Explicit automatic-retry safety, or `None` for registry lookup and conservative fallback.
    pub retry_safety: Option<OperationSafety>,
}

impl Request {
    /// Creates a request with five preambles.
    pub fn new(address: Address, command: impl Into<CommandCode>, data: Vec<u8>) -> Self {
        Self {
            address,
            command: command.into(),
            data,
            preambles: 5,
            frame_expansion: Vec::new(),
            physical_layer: PhysicalLayer::Fsk,
            retry_safety: None,
        }
    }

    /// Creates a request and immediately checks its wire-size constraints.
    pub fn try_new(
        address: Address,
        command: impl Into<CommandCode>,
        data: impl Into<Vec<u8>>,
    ) -> Result<Self, OperationError> {
        let request = Self::new(address, command, data.into());
        request.validate()?;
        Ok(request)
    }

    /// Sets the preamble count.
    pub const fn with_preambles(mut self, preambles: u8) -> Self {
        self.preambles = preambles;
        self
    }

    /// Sets the physical mode of the frame.
    pub const fn with_physical_layer(mut self, physical_layer: PhysicalLayer) -> Self {
        self.physical_layer = physical_layer;
        self
    }

    /// Sets up to three link-layer header-extension bytes.
    pub fn with_frame_expansion(
        mut self,
        expansion: impl Into<Vec<u8>>,
    ) -> Result<Self, OperationError> {
        let expansion = expansion.into();
        if expansion.len() > 3 {
            return Err(OperationError::InvalidValue("frame expansion length"));
        }
        self.frame_expansion = expansion;
        Ok(self)
    }

    /// Declares whether repeating this exact request can safely occur after an uncertain result.
    pub const fn with_retry_safety(mut self, safety: OperationSafety) -> Self {
        self.retry_safety = Some(safety);
        self
    }

    /// Validates frame-size fields without cloning or encoding request data.
    pub fn validate(&self) -> Result<(), OperationError> {
        let service_bytes = usize::from(self.command.is_expanded()) * 2;
        let payload_len = self.data.len().saturating_add(service_bytes);
        if payload_len > usize::from(u8::MAX) {
            return Err(OperationError::Wire(WireError::PayloadLength(payload_len)));
        }
        if self.frame_expansion.len() > 3 {
            return Err(OperationError::Wire(WireError::ExpansionLength(
                self.frame_expansion.len(),
            )));
        }
        Ok(())
    }

    /// Converts the request into a link-layer frame.
    pub fn to_frame(&self) -> Result<Frame, OperationError> {
        self.validate()?;
        let (wire_command, payload) = if self.command.is_expanded() {
            let mut payload = Vec::with_capacity(self.data.len() + 2);
            payload.extend_from_slice(&self.command.get().to_be_bytes());
            payload.extend_from_slice(&self.data);
            (31, payload)
        } else {
            (
                u8::try_from(self.command.get())
                    .map_err(|_| OperationError::InvalidValue("standard command number"))?,
                self.data.clone(),
            )
        };
        let frame = Frame {
            preambles: self.preambles,
            kind: FrameKind::Request,
            physical_layer: self.physical_layer,
            address: self.address,
            expansion: self.frame_expansion.clone(),
            wire_command,
            payload,
            repair: None,
        };
        frame.delimiter()?;
        Ok(frame)
    }
}

/// Validated device response with a normalized command number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceReply {
    /// Address carried by the response.
    pub address: Address,
    /// Logical command number.
    pub command: CommandCode,
    /// Operation result code.
    pub response_code: u8,
    /// Device status bits.
    pub device_status: u8,
    /// Application payload without service bytes.
    pub data: Vec<u8>,
    /// Whether the response was an unsolicited message.
    pub burst: bool,
    /// Physical mode of the received frame.
    pub physical_layer: PhysicalLayer,
    /// Preamble count of the received frame.
    pub response_preambles: u8,
    /// Link-layer header extension bytes.
    pub frame_expansion: Vec<u8>,
}

impl DeviceReply {
    /// Decodes a response or unsolicited frame.
    pub fn from_frame(frame: Frame) -> Result<Self, OperationError> {
        let Frame {
            preambles,
            kind,
            physical_layer,
            address,
            expansion,
            wire_command,
            payload,
            ..
        } = frame;
        if !matches!(kind, FrameKind::Response | FrameKind::Burst) {
            return Err(OperationError::InvalidValue("response frame type"));
        }
        if payload.len() < 2 {
            return Err(OperationError::Truncated {
                field: "response code and status",
                offset: payload.len(),
            });
        }
        let response_code = payload[0];
        let device_status = payload[1];
        let (command, data_start) = if wire_command == 31 {
            if payload.len() < 4 {
                return Err(OperationError::Truncated {
                    field: "expanded command number",
                    offset: payload.len(),
                });
            }
            (
                CommandCode::new(u16::from_be_bytes([payload[2], payload[3]])),
                4,
            )
        } else {
            (CommandCode::from(wire_command), 2)
        };
        Ok(Self {
            address,
            command,
            response_code,
            device_status,
            data: payload[data_start..].to_vec(),
            burst: kind == FrameKind::Burst,
            physical_layer,
            response_preambles: preambles,
            frame_expansion: expansion,
        })
    }

    /// Validates the command and ensures that the device reported no error.
    pub fn require_success(&self, expected: CommandCode) -> Result<(), OperationError> {
        self.require_command(expected)?;
        if self.response_code & 0x80 != 0 {
            return Err(OperationError::CommunicationStatus {
                summary: self.response_code,
                status: self.device_status,
            });
        }
        if self.response_code != 0 {
            return Err(OperationError::DeviceStatus {
                code: self.response_code,
                status: self.device_status,
            });
        }
        Ok(())
    }

    /// Validates only the logical command, preserving every response status for the caller.
    pub fn require_command(&self, expected: CommandCode) -> Result<(), OperationError> {
        if self.command != expected {
            return Err(OperationError::CommandMismatch {
                expected: expected.get(),
                actual: self.command.get(),
            });
        }
        Ok(())
    }

    /// Reports whether the first status byte is a link communication-error summary.
    pub const fn has_communication_error(&self) -> bool {
        self.response_code & 0x80 != 0
    }

    /// Reports an exact successful application response.
    pub const fn is_success(&self) -> bool {
        self.response_code == 0
    }
}

/// Typed value accompanied by the exact device result bytes that allowed its decoding.
#[derive(Debug, Clone, PartialEq)]
pub struct CommandOutcome<T> {
    /// Decoded command-specific value.
    pub value: T,
    /// Original seven-bit application response code.
    pub response_code: u8,
    /// Original device-status byte.
    pub device_status: u8,
}

impl<T> CommandOutcome<T> {
    /// Reports whether the accepted result was accompanied by a warning.
    pub const fn is_warning(&self) -> bool {
        self.response_code != 0
    }
}

/// Typed operation performed on a device.
pub trait Operation {
    /// Result of successful response decoding.
    type Output;

    /// Returns the logical command number.
    fn command(&self) -> CommandCode;

    /// Returns the retry semantics used when building a request.
    ///
    /// Unknown commands default to [`OperationSafety::Action`]. Vendor codecs should override
    /// this method only when their exact operation semantics are known.
    fn retry_safety(&self) -> OperationSafety {
        command_descriptor(self.command()).map_or(OperationSafety::Action, |entry| entry.safety)
    }

    /// Encodes request data.
    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError>;

    /// Decodes application response data.
    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError>;

    /// Decodes a success or one of the explicitly accepted command-specific warning codes.
    ///
    /// Warning meaning is not inferred from a broad numeric range. Callers should obtain the
    /// accepted list from the applicable command specification or exact-revision DeviceInfo.
    fn decode_reply_accepting(
        &self,
        reply: &DeviceReply,
        accepted_warnings: &[u8],
    ) -> Result<CommandOutcome<Self::Output>, OperationError> {
        reply.require_command(self.command())?;
        if reply.has_communication_error() {
            return Err(OperationError::CommunicationStatus {
                summary: reply.response_code,
                status: reply.device_status,
            });
        }
        if reply.response_code != 0 && !accepted_warnings.contains(&reply.response_code) {
            return Err(OperationError::DeviceStatus {
                code: reply.response_code,
                status: reply.device_status,
            });
        }
        let mut accepted = reply.clone();
        accepted.response_code = 0;
        Ok(CommandOutcome {
            value: self.decode_reply(&accepted)?,
            response_code: reply.response_code,
            device_status: reply.device_status,
        })
    }

    /// Creates a complete request for the supplied address.
    fn request(&self, address: Address) -> Result<Request, OperationError> {
        let mut writer = PayloadWriter::new();
        self.encode_request(&mut writer)?;
        Ok(Request::new(address, self.command(), writer.finish())
            .with_retry_safety(self.retry_safety()))
    }
}

/// Raw operation with no assumptions about the vendor payload format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawOperation {
    command: CommandCode,
    data: Vec<u8>,
    retry_safety: OperationSafety,
}

impl RawOperation {
    /// Creates an arbitrary standard or expanded command.
    pub fn new(command: impl Into<CommandCode>, data: impl Into<Vec<u8>>) -> Self {
        Self {
            command: command.into(),
            data: data.into(),
            retry_safety: OperationSafety::Action,
        }
    }

    /// Creates a read-only vendor command that may be retried and coalesced safely.
    pub fn read(command: impl Into<CommandCode>, data: impl Into<Vec<u8>>) -> Self {
        Self::new(command, data).with_retry_safety(OperationSafety::ReadOnly)
    }

    /// Creates an idempotent write that may be repeated after an uncertain result.
    pub fn idempotent_write(command: impl Into<CommandCode>, data: impl Into<Vec<u8>>) -> Self {
        Self::new(command, data).with_retry_safety(OperationSafety::IdempotentWrite)
    }

    /// Creates a one-shot action that is never retried after a completed transmission.
    pub fn action(command: impl Into<CommandCode>, data: impl Into<Vec<u8>>) -> Self {
        Self::new(command, data)
    }

    /// Declares retry semantics for a vendor operation whose exact behavior is known.
    pub const fn with_retry_safety(mut self, safety: OperationSafety) -> Self {
        self.retry_safety = safety;
        self
    }
}

impl Operation for RawOperation {
    type Output = DeviceReply;

    fn command(&self) -> CommandCode {
        self.command
    }

    fn retry_safety(&self) -> OperationSafety {
        self.retry_safety
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.data);
        Ok(())
    }

    fn request(&self, address: Address) -> Result<Request, OperationError> {
        let maximum = if self.command.is_expanded() { 253 } else { 255 };
        if self.data.len() > maximum {
            return Err(OperationError::RequestLength {
                command: self.command.get(),
                actual: self.data.len(),
            });
        }
        Ok(Request::new(address, self.command, self.data.clone())
            .with_retry_safety(self.retry_safety))
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        if reply.command != self.command {
            return Err(OperationError::CommandMismatch {
                expected: self.command.get(),
                actual: reply.command.get(),
            });
        }
        Ok(reply.clone())
    }
}

/// Raw command validated against a known request length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedOperation {
    command: CommandCode,
    data: Vec<u8>,
    retry_safety: OperationSafety,
}

impl CheckedOperation {
    /// Validates the length against the explicit registry and preserves the original data.
    pub fn new(
        command: impl Into<CommandCode>,
        data: impl Into<Vec<u8>>,
    ) -> Result<Self, OperationError> {
        let command = command.into();
        let data = data.into();
        let fits_frame = if command.is_expanded() {
            data.len() <= usize::from(u8::MAX) - 2
        } else {
            u8::try_from(data.len()).is_ok()
        };
        if !fits_frame || !crate::catalog::request_constraint(command).allows(data.len()) {
            return Err(OperationError::RequestLength {
                command: command.get(),
                actual: data.len(),
            });
        }
        let retry_safety = command_descriptor(command)
            .map_or(OperationSafety::Action, |descriptor| descriptor.safety);
        Ok(Self {
            command,
            data,
            retry_safety,
        })
    }

    /// Overrides retry semantics after validating them against the applicable specification.
    pub const fn with_retry_safety(mut self, safety: OperationSafety) -> Self {
        self.retry_safety = safety;
        self
    }
}

impl Operation for CheckedOperation {
    type Output = DeviceReply;

    fn command(&self) -> CommandCode {
        self.command
    }

    fn retry_safety(&self) -> OperationSafety {
        self.retry_safety
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.data);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        if reply.command != self.command {
            return Err(OperationError::CommandMismatch {
                expected: self.command.get(),
                actual: reply.command.get(),
            });
        }
        Ok(reply.clone())
    }
}
