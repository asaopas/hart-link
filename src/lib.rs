//! Tools for communicating with HART devices.
//!
//! The architecture separates wire-byte representation, operation semantics,
//! and exchange execution. Core types and the streaming decoder are available
//! without the standard library. The asynchronous queue and network channels
//! are enabled by the `runtime` feature.
//!
//! # Minimal wire example
//!
//! ```
//! use hart_link::{Address, Master, Request, inspect_bytes};
//!
//! let address = Address::polling(0, Master::Primary)?;
//! let bytes = Request::new(address, 0u8, vec![]).to_frame()?.encode()?;
//! let report = inspect_bytes(&bytes).map_err(|issue| issue.message)?;
//! assert_eq!(report.command.get(), 0);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_code)]
#![warn(missing_docs, rustdoc::broken_intra_doc_links)]

extern crate alloc;

/// Registry of known operations and their declared properties.
pub mod catalog;
/// Validation and explanation of complete frames.
pub mod inspect;
/// Typed operations grouped by purpose.
pub mod operation;
/// Versioned common tables that preserve unknown codes.
pub mod tables;
/// Link-layer byte representation and streaming decoding.
pub mod wire;

/// I/O channels for physical and network links.
#[cfg(feature = "runtime")]
pub mod channel;
/// Dynamic descriptions of devices and application fields.
#[cfg(feature = "device-info")]
pub mod device;
/// Test and link-emulation facilities.
#[cfg(feature = "emulator")]
pub mod emulator;
/// HART-IP packet transport.
#[cfg(feature = "hart-ip")]
pub mod ip;
/// Host-side WirelessHART mechanisms.
#[cfg(feature = "wireless-hart")]
pub mod mesh;
/// Bounded DeviceInfo extraction from an FDI package.
#[cfg(feature = "fdi-package")]
pub mod package;
/// Physical-link, gateway, and device-discovery profiles.
#[cfg(feature = "runtime")]
pub mod profile;
/// Exchange queue, runner, and device session.
#[cfg(feature = "runtime")]
pub mod service;
/// Metric snapshots independent of a specific observability system.
#[cfg(feature = "runtime")]
pub mod telemetry;
/// Recording and replay of byte-exact exchanges.
#[cfg(feature = "runtime")]
pub mod trace;
/// Local functional campaigns against an emulator or hardware.
#[cfg(feature = "runtime")]
pub mod verification;

pub use catalog::{
    CommandClass, CommandDescriptor, OperationSafety, PayloadConstraint, command_descriptor,
    request_constraint,
};
pub use inspect::{
    ExchangeReport, FrameReport, InspectionIssue, MAXIMUM_INSPECTION_TEXT_BYTES, inspect_base64,
    inspect_bytes, inspect_exchange, inspect_hex,
};
pub use operation::{
    CheckedOperation, CommandCode, CommandOutcome, DeviceReply, Operation, OperationError,
    RawOperation, Request,
};
pub use wire::{
    Address, ChecksumPolicy, DecodeEvent, DecodeLimits, DecodeLimitsError, Frame, FrameDecoder,
    FrameKind, FrameRepair, MAX_ENCODED_FRAME_SIZE, MAXIMUM_DECODE_BUFFER_CAPACITY,
    MAXIMUM_DECODE_EVENTS_PER_PUSH, Master, PhysicalLayer, WireError,
};

#[cfg(feature = "runtime")]
pub use service::{
    AdaptiveTiming, CommandContext, CommandPolicy, CommandRouting, DEFAULT_LATE_RESPONSE_GUARD,
    DeviceHealthConfigError, DeviceHealthOptions, DeviceHealthSnapshot, DeviceSnapshot,
    ExchangeError, LinkBuildError, LinkBuilder, LinkClient, LinkConfig, LinkConfigError, LinkEvent,
    LinkRunner, LinkSnapshot, MAXIMUM_LATE_RESPONSE_GUARD, MAXIMUM_LINK_DECODER_BUFFER,
    MAXIMUM_LINK_EVENT_CAPACITY, MAXIMUM_LINK_QUEUE_CAPACITY, MAXIMUM_LINK_READ_BUFFER,
    MAXIMUM_RETRY_DURATION, MAXIMUM_TRANSMIT_PREFIX, ManagedDeviceSession, ManagedSessionError,
    PendingReply, Priority, PriorityClient, QueueMode, QueueScheduling, QueueSchedulingError,
    RetryCause, RetryPolicy, RetryPolicyError, SnapshotError, SnapshotField, SnapshotOptions,
    StartError, create_link, try_create_link,
};
