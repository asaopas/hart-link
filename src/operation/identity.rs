use alloc::vec::Vec;

use crate::operation::{
    CommandCode, DeviceReply, Operation, OperationError, PayloadReader, PayloadWriter,
};
use crate::wire::{Address, Master, WireError};

/// Requests device identification.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadDeviceIdentity;

/// Identification operation for Commands 0, 11, 21, 73, and 75.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIdentity {
    command: CommandCode,
    request_data: Vec<u8>,
}

impl ReadIdentity {
    /// Creates one of the identification commands.
    pub fn new(command: u8, request_data: impl Into<Vec<u8>>) -> Result<Self, OperationError> {
        if !matches!(command, 0 | 11 | 21 | 73 | 75) {
            return Err(OperationError::InvalidValue(
                "identification command number",
            ));
        }
        let request_data = request_data.into();
        if !crate::request_constraint(CommandCode::from(command)).allows(request_data.len()) {
            return Err(OperationError::RequestLength {
                command: u16::from(command),
                actual: request_data.len(),
            });
        }
        Ok(Self {
            command: CommandCode::from(command),
            request_data,
        })
    }

    /// Creates Command 11 from an eight-character short tag.
    pub fn by_tag(tag: &str) -> Result<Self, OperationError> {
        Self::new(11, super::text::pack_text(tag, 6)?)
    }

    /// Creates Command 21 from a long Latin-1 tag.
    pub fn by_long_tag(tag: &str) -> Result<Self, OperationError> {
        Self::new(21, super::text::encode_latin1(tag, 32)?)
    }
}

/// Identification data that preserves an unknown response tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceIdentity {
    /// Response format identifier, normally `0xFE`.
    pub response_identifier: u8,
    /// Manufacturer identifier.
    pub manufacturer_id: u16,
    /// Expanded device type.
    pub device_type: u16,
    /// Number of request preambles required by the device.
    pub request_preambles: u8,
    /// Universal-command revision.
    pub universal_revision: u8,
    /// Device-specific revision.
    pub device_revision: u8,
    /// Software revision.
    pub software_revision: u8,
    /// Hardware revision without physical-signal bits.
    pub hardware_revision: u8,
    /// Supported physical-signal code.
    pub physical_signaling: u8,
    /// Device capability flags.
    pub flags: u8,
    /// Unique 24-bit part of the address.
    pub device_id: u32,
    /// Required response preamble count in newer revisions.
    pub response_preambles: Option<u8>,
    /// Maximum number of device variables.
    pub maximum_device_variables: Option<u8>,
    /// Configuration-change counter.
    pub configuration_change_counter: Option<u16>,
    /// Extended device status.
    pub extended_status: Option<u8>,
    /// Private-label distributor identifier.
    pub private_label_distributor: Option<u16>,
    /// Device profile code.
    pub device_profile: Option<u8>,
    /// Additional data introduced by newer revisions.
    pub extension: Vec<u8>,
}

impl DeviceIdentity {
    /// Returns the 14-bit expanded device type used in a long HART address.
    ///
    /// Command 0 encodes this value differently before HART 7: the high
    /// address byte comes from the legacy manufacturer field and the low byte
    /// comes from the legacy device type. HART 7 and newer return the expanded
    /// type directly in those two positions.
    pub const fn address_device_type(&self) -> u16 {
        if self.universal_revision >= 7 {
            self.device_type & 0x3fff
        } else {
            ((self.manufacturer_id & 0x3f) << 8) | (self.device_type & 0x00ff)
        }
    }

    /// Builds the unique address that must be used after discovery.
    pub fn unique_address(&self, master: Master) -> Result<Address, WireError> {
        Address::unique(self.address_device_type(), self.device_id, master)
    }
}

impl Operation for ReadDeviceIdentity {
    type Output = DeviceIdentity;

    fn command(&self) -> CommandCode {
        CommandCode::new(0)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_identity_data(&reply.data)
    }
}

fn decode_identity_data(data: &[u8]) -> Result<DeviceIdentity, OperationError> {
    if data.len() < 12 {
        return Err(OperationError::Truncated {
            field: "mandatory identification",
            offset: data.len(),
        });
    }
    let mut reader = PayloadReader::new(data);
    let response_identifier = reader.u8("response identifier")?;
    if response_identifier != 0xfe {
        return Err(OperationError::InvalidValue(
            "Command 0 response identifier",
        ));
    }
    let legacy_manufacturer = reader.u8("manufacturer or high part of device type")?;
    let legacy_device_type = reader.u8("device type")?;
    let request_preambles = reader.u8("request preamble count")?;
    if request_preambles == 0 {
        return Err(OperationError::InvalidValue("request preamble count"));
    }
    let universal_revision = reader.u8("universal-command revision")?;
    let required_length = match universal_revision {
        0..=5 => 12,
        6 => 17,
        _ => 22,
    };
    if data.len() < required_length {
        return Err(OperationError::Truncated {
            field: "identification data for the selected HART revision",
            offset: data.len(),
        });
    }
    let device_revision = reader.u8("device revision")?;
    let software_revision = reader.u8("software revision")?;
    let hardware_and_signal = reader.u8("hardware revision and physical signal")?;
    let flags = reader.u8("capability flags")?;
    let device_id = reader.u24("device identifier")?;
    let response_preambles = (universal_revision >= 6)
        .then(|| reader.u8("response preamble count"))
        .transpose()?;
    let maximum_device_variables = (universal_revision >= 6)
        .then(|| reader.u8("maximum device-variable count"))
        .transpose()?;
    let configuration_change_counter = (universal_revision >= 6)
        .then(|| reader.u16("configuration-change counter"))
        .transpose()?;
    let extended_status = (universal_revision >= 6)
        .then(|| reader.u8("extended status"))
        .transpose()?;
    let manufacturer_id = if universal_revision >= 7 {
        reader.u16("manufacturer identifier")?
    } else {
        u16::from(legacy_manufacturer)
    };
    let private_label_distributor = (universal_revision >= 7)
        .then(|| reader.u16("private-label distributor"))
        .transpose()?;
    let device_profile = (universal_revision >= 7)
        .then(|| reader.u8("device profile"))
        .transpose()?;
    Ok(DeviceIdentity {
        response_identifier,
        manufacturer_id,
        device_type: if universal_revision >= 7 {
            u16::from_be_bytes([legacy_manufacturer, legacy_device_type])
        } else {
            u16::from(legacy_device_type)
        },
        request_preambles,
        universal_revision,
        device_revision,
        software_revision,
        hardware_revision: hardware_and_signal >> 3,
        physical_signaling: hardware_and_signal & 0x07,
        flags,
        device_id,
        response_preambles,
        maximum_device_variables,
        configuration_change_counter,
        extended_status,
        private_label_distributor,
        device_profile,
        extension: reader.rest().to_vec(),
    })
}

impl Operation for ReadIdentity {
    type Output = DeviceIdentity;

    fn command(&self) -> CommandCode {
        self.command
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.request_data);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_identity_data(&reply.data)
    }
}
