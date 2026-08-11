use std::{string::String, time::SystemTime};

use crate::{
    CommandCode,
    operation::{
        AdditionalStatus, DynamicValues, LoopSignal, PrimaryValue, ReadAdditionalStatus,
        ReadDynamicValues, ReadLongTag, ReadLoopSignal, ReadPrimaryValue, ReadTagDescriptorDate,
        TagDescriptorDate,
    },
    service::{DeviceSession, SessionError, SessionProfile},
};

/// Selects optional groups included in a device snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotOptions {
    /// Reads Commands 1, 2, and 3.
    pub process_values: bool,
    /// Reads Commands 13 and, when supported by the HART revision, 20.
    pub tags: bool,
    /// Reads Command 48.
    pub diagnostics: bool,
}

impl SnapshotOptions {
    /// Returns only identity already held by the device session.
    pub const IDENTITY_ONLY: Self = Self {
        process_values: false,
        tags: false,
        diagnostics: false,
    };

    /// Requests every snapshot group supported by the library.
    pub const FULL: Self = Self {
        process_values: true,
        tags: true,
        diagnostics: true,
    };

    /// Enables or disables process-value reads.
    pub const fn with_process_values(mut self, enabled: bool) -> Self {
        self.process_values = enabled;
        self
    }

    /// Enables or disables short and long tag reads.
    pub const fn with_tags(mut self, enabled: bool) -> Self {
        self.tags = enabled;
        self
    }

    /// Enables or disables additional-status reads.
    pub const fn with_diagnostics(mut self, enabled: bool) -> Self {
        self.diagnostics = enabled;
        self
    }
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self::FULL
    }
}

/// One command failure retained without discarding other snapshot values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotError {
    /// Logical command that failed.
    pub command: CommandCode,
    /// Stable human-readable error captured at collection time.
    pub message: String,
}

/// State of one optional snapshot field.
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotField<T> {
    /// The caller did not request this group.
    NotRequested,
    /// The identified HART revision does not support the command.
    Unsupported {
        /// Static reason for skipping the command.
        reason: &'static str,
    },
    /// The command completed and decoded successfully.
    Value(T),
    /// The command failed while other fields remained available.
    Error(SnapshotError),
}

impl<T> SnapshotField<T> {
    /// Returns a shared successful value.
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::NotRequested | Self::Unsupported { .. } | Self::Error(_) => None,
        }
    }

    /// Returns the retained command failure.
    pub const fn error(&self) -> Option<&SnapshotError> {
        match self {
            Self::Error(error) => Some(error),
            Self::NotRequested | Self::Unsupported { .. } | Self::Value(_) => None,
        }
    }

    /// Reports whether the command failed.
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error(_))
    }
}

/// Partial, timestamped view of one identified device.
#[derive(Debug, Clone, PartialEq)]
pub struct DeviceSnapshot {
    /// Host time immediately before optional commands were collected.
    pub captured_at: SystemTime,
    /// Identity, unique address, and learned preamble count from the session.
    pub profile: SessionProfile,
    /// Command 1 result.
    pub primary_value: SnapshotField<PrimaryValue>,
    /// Command 2 result.
    pub loop_signal: SnapshotField<LoopSignal>,
    /// Command 3 result.
    pub dynamic_values: SnapshotField<DynamicValues>,
    /// Command 13 result.
    pub tag: SnapshotField<TagDescriptorDate>,
    /// Command 20 result.
    pub long_tag: SnapshotField<String>,
    /// Command 48 result.
    pub additional_status: SnapshotField<AdditionalStatus>,
}

impl DeviceSnapshot {
    /// Reports whether at least one requested command failed.
    pub const fn has_errors(&self) -> bool {
        self.primary_value.is_error()
            || self.loop_signal.is_error()
            || self.dynamic_values.is_error()
            || self.tag.is_error()
            || self.long_tag.is_error()
            || self.additional_status.is_error()
    }
}

pub(crate) async fn capture_snapshot(
    session: &DeviceSession,
    options: SnapshotOptions,
) -> DeviceSnapshot {
    let captured_at = SystemTime::now();
    let primary_value = if options.process_values {
        capture(
            CommandCode::new(1),
            session.execute_default(&ReadPrimaryValue).await,
        )
    } else {
        SnapshotField::NotRequested
    };
    let loop_signal = if options.process_values {
        capture(
            CommandCode::new(2),
            session.execute_default(&ReadLoopSignal).await,
        )
    } else {
        SnapshotField::NotRequested
    };
    let dynamic_values = if options.process_values {
        capture(
            CommandCode::new(3),
            session.execute_default(&ReadDynamicValues).await,
        )
    } else {
        SnapshotField::NotRequested
    };
    let tag = if options.tags {
        capture(
            CommandCode::new(13),
            session.execute_default(&ReadTagDescriptorDate).await,
        )
    } else {
        SnapshotField::NotRequested
    };
    let long_tag = if !options.tags {
        SnapshotField::NotRequested
    } else if session.profile().identity.universal_revision < 6 {
        SnapshotField::Unsupported {
            reason: "long tag requires HART 6 or newer",
        }
    } else {
        capture(
            CommandCode::new(20),
            session.execute_default(&ReadLongTag).await,
        )
    };
    let additional_status = if options.diagnostics {
        capture(
            CommandCode::new(48),
            session.execute_default(&ReadAdditionalStatus).await,
        )
    } else {
        SnapshotField::NotRequested
    };
    DeviceSnapshot {
        captured_at,
        profile: session.profile().clone(),
        primary_value,
        loop_signal,
        dynamic_values,
        tag,
        long_tag,
        additional_status,
    }
}

fn capture<T>(command: CommandCode, result: Result<T, SessionError>) -> SnapshotField<T> {
    match result {
        Ok(value) => SnapshotField::Value(value),
        Err(error) => SnapshotField::Error(SnapshotError {
            command,
            message: error.to_string(),
        }),
    }
}
