use crate::wire::WireError;

/// Master that originates a frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Master {
    /// Primary line master.
    Primary,
    /// Secondary line master.
    Secondary,
}

impl Master {
    pub(crate) const fn address_bit(self) -> u8 {
        match self {
            Self::Primary => 0x80,
            Self::Secondary => 0,
        }
    }

    pub(crate) const fn from_address_byte(byte: u8) -> Self {
        if byte & 0x80 == 0 {
            Self::Secondary
        } else {
            Self::Primary
        }
    }
}

/// Transport-independent node address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Address {
    /// Short polling address in the `0..=63` range.
    Polling {
        /// Node number on the line.
        node: u8,
        /// Master performing the exchange.
        master: Master,
    },
    /// Long address composed of the expanded device type and 24-bit identifier.
    Unique {
        /// Expanded device type; its lower 14 bits are encoded in the address.
        expanded_type: u16,
        /// Unique 24-bit part of the address.
        device_id: u32,
        /// Master performing the exchange.
        master: Master,
    },
}

impl Address {
    /// Creates a validated short address.
    pub fn polling(node: u8, master: Master) -> Result<Self, WireError> {
        if node > 63 {
            return Err(WireError::PollingAddress(node));
        }
        Ok(Self::Polling { node, master })
    }

    /// Creates a validated long address.
    pub fn unique(expanded_type: u16, device_id: u32, master: Master) -> Result<Self, WireError> {
        if expanded_type > 0x3fff {
            return Err(WireError::ExpandedDeviceType(expanded_type));
        }
        if device_id > 0x00ff_ffff {
            return Err(WireError::DeviceIdentifier(device_id));
        }
        Ok(Self::Unique {
            expanded_type,
            device_id,
            master,
        })
    }

    /// Returns the number of address bytes in the frame.
    pub const fn encoded_len(self) -> usize {
        match self {
            Self::Polling { .. } => 1,
            Self::Unique { .. } => 5,
        }
    }

    /// Returns the master encoded in the address.
    pub const fn master(self) -> Master {
        match self {
            Self::Polling { master, .. } | Self::Unique { master, .. } => master,
        }
    }

    /// Returns the short polling node, if this is a short address.
    pub const fn polling_node(self) -> Option<u8> {
        match self {
            Self::Polling { node, .. } => Some(node),
            Self::Unique { .. } => None,
        }
    }

    /// Returns the expanded device type and 24-bit identifier of a long address.
    pub const fn unique_parts(self) -> Option<(u16, u32)> {
        match self {
            Self::Unique {
                expanded_type,
                device_id,
                ..
            } => Some((expanded_type, device_id)),
            Self::Polling { .. } => None,
        }
    }

    pub(crate) fn write_to(self, burst: bool, output: &mut alloc::vec::Vec<u8>) {
        let burst_bit = u8::from(burst) << 6;
        match self {
            Self::Polling { node, master } => {
                output.push(master.address_bit() | burst_bit | node);
            }
            Self::Unique {
                expanded_type,
                device_id,
                master,
            } => {
                let type_bytes = expanded_type.to_be_bytes();
                output.push(master.address_bit() | burst_bit | (type_bytes[0] & 0x3f));
                output.push(type_bytes[1]);
                output.extend_from_slice(&device_id.to_be_bytes()[1..]);
            }
        }
    }

    pub(crate) fn read_from(bytes: &[u8], long: bool) -> Result<Self, WireError> {
        let first = *bytes.first().ok_or(WireError::Truncated)?;
        let master = Master::from_address_byte(first);
        if !long {
            return Self::polling(first & 0x3f, master);
        }
        if bytes.len() < 5 {
            return Err(WireError::Truncated);
        }
        let expanded_type = (u16::from(first & 0x3f) << 8) | u16::from(bytes[1]);
        let device_id =
            (u32::from(bytes[2]) << 16) | (u32::from(bytes[3]) << 8) | u32::from(bytes[4]);
        Self::unique(expanded_type, device_id, master)
    }
}
