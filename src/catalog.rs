//! Explicit registry of known commands without broad range classification.

use crate::operation::CommandCode;

/// Declared command class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandClass {
    /// Universal command.
    Universal,
    /// Common-practice command.
    CommonPractice,
}

/// Safety of automatic retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationSafety {
    /// The operation only reads state.
    ReadOnly,
    /// A retry establishes the same state.
    IdempotentWrite,
    /// A retry may trigger the action again.
    Action,
}

/// Request-payload length constraint for a known command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadConstraint {
    /// No payload.
    Empty,
    /// Exact length required.
    Exact(u8),
    /// A continuous length range is accepted.
    Range {
        /// Minimum byte count.
        minimum: u8,
        /// Maximum byte count.
        maximum: u8,
    },
    /// Two distinct lengths are accepted.
    Either(u8, u8),
    /// The format is defined by DeviceInfo or vendor information.
    Unspecified,
}

impl PayloadConstraint {
    /// Validates the actual length.
    pub const fn allows(self, length: usize) -> bool {
        match self {
            Self::Empty => length == 0,
            Self::Exact(expected) => length == expected as usize,
            Self::Range { minimum, maximum } => {
                length >= minimum as usize && length <= maximum as usize
            }
            Self::Either(left, right) => length == left as usize || length == right as usize,
            Self::Unspecified => length <= u8::MAX as usize,
        }
    }
}

/// Concise declared description of a known operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandDescriptor {
    /// Logical number.
    pub code: CommandCode,
    /// Declared class.
    pub class: CommandClass,
    /// Safe-retry rule.
    pub safety: OperationSafety,
    /// Short operation purpose.
    pub purpose: &'static str,
}

const fn descriptor(
    code: u16,
    class: CommandClass,
    safety: OperationSafety,
    purpose: &'static str,
) -> CommandDescriptor {
    CommandDescriptor {
        code: CommandCode::new(code),
        class,
        safety,
        purpose,
    }
}

const COMMANDS: &[CommandDescriptor] = &[
    descriptor(
        0,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "device identification",
    ),
    descriptor(
        1,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "primary variable",
    ),
    descriptor(
        2,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "current and percent of range",
    ),
    descriptor(
        3,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "dynamic variables and current",
    ),
    descriptor(
        6,
        CommandClass::Universal,
        OperationSafety::IdempotentWrite,
        "polling address",
    ),
    descriptor(
        7,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "loop-current mode",
    ),
    descriptor(
        8,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "variable classification",
    ),
    descriptor(
        9,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "variables and status",
    ),
    descriptor(
        11,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "identification by tag",
    ),
    descriptor(
        12,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "device message",
    ),
    descriptor(
        13,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "tag, descriptor, and date",
    ),
    descriptor(
        14,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "sensor information",
    ),
    descriptor(
        15,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "primary-output information",
    ),
    descriptor(
        16,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "assembly number",
    ),
    descriptor(
        17,
        CommandClass::Universal,
        OperationSafety::IdempotentWrite,
        "write message",
    ),
    descriptor(
        18,
        CommandClass::Universal,
        OperationSafety::IdempotentWrite,
        "write tag and descriptor",
    ),
    descriptor(
        19,
        CommandClass::Universal,
        OperationSafety::IdempotentWrite,
        "write assembly number",
    ),
    descriptor(
        20,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "long tag",
    ),
    descriptor(
        21,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "identification by long tag",
    ),
    descriptor(
        22,
        CommandClass::Universal,
        OperationSafety::IdempotentWrite,
        "write long tag",
    ),
    descriptor(
        33,
        CommandClass::CommonPractice,
        OperationSafety::ReadOnly,
        "device variables",
    ),
    descriptor(
        34,
        CommandClass::CommonPractice,
        OperationSafety::IdempotentWrite,
        "damping",
    ),
    descriptor(
        35,
        CommandClass::CommonPractice,
        OperationSafety::IdempotentWrite,
        "range limits",
    ),
    descriptor(
        36,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "set upper range value from the applied input",
    ),
    descriptor(
        37,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "set lower range value from the applied input",
    ),
    descriptor(
        38,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "reset configuration-changed flag",
    ),
    descriptor(
        39,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "nonvolatile-memory control",
    ),
    descriptor(
        40,
        CommandClass::CommonPractice,
        OperationSafety::IdempotentWrite,
        "fixed current",
    ),
    descriptor(
        41,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "self-test",
    ),
    descriptor(
        42,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "device reset",
    ),
    descriptor(
        43,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "zero primary variable",
    ),
    descriptor(
        44,
        CommandClass::CommonPractice,
        OperationSafety::IdempotentWrite,
        "primary-variable unit",
    ),
    descriptor(
        45,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "current-output zero trim",
    ),
    descriptor(
        46,
        CommandClass::CommonPractice,
        OperationSafety::Action,
        "current-output gain trim",
    ),
    descriptor(
        47,
        CommandClass::CommonPractice,
        OperationSafety::IdempotentWrite,
        "transfer function",
    ),
    descriptor(
        48,
        CommandClass::Universal,
        OperationSafety::ReadOnly,
        "additional status",
    ),
    descriptor(
        73,
        CommandClass::CommonPractice,
        OperationSafety::ReadOnly,
        "connected-device discovery",
    ),
    descriptor(
        75,
        CommandClass::CommonPractice,
        OperationSafety::ReadOnly,
        "subdevice information",
    ),
];

/// Returns a description only for a command present in the explicit registry.
pub fn command_descriptor(code: CommandCode) -> Option<&'static CommandDescriptor> {
    COMMANDS.iter().find(|entry| entry.code == code)
}

/// Returns the complete built-in registry.
pub const fn known_commands() -> &'static [CommandDescriptor] {
    COMMANDS
}

/// Returns the enforceable request-payload constraint.
pub const fn request_constraint(code: CommandCode) -> PayloadConstraint {
    match code.get() {
        0..=3 | 7 | 8 | 12..=16 | 20 | 36 | 37 | 41..=43 | 48 => PayloadConstraint::Empty,
        6 => PayloadConstraint::Either(1, 2),
        9 => PayloadConstraint::Range {
            minimum: 1,
            maximum: 8,
        },
        11 => PayloadConstraint::Exact(6),
        17 => PayloadConstraint::Exact(24),
        18 => PayloadConstraint::Exact(21),
        19 => PayloadConstraint::Exact(3),
        21 | 22 => PayloadConstraint::Exact(32),
        33 => PayloadConstraint::Range {
            minimum: 1,
            maximum: 4,
        },
        34 | 40 | 45 | 46 => PayloadConstraint::Exact(4),
        35 => PayloadConstraint::Exact(9),
        38 => PayloadConstraint::Either(0, 2),
        39 | 44 | 47 => PayloadConstraint::Exact(1),
        _ => PayloadConstraint::Unspecified,
    }
}
