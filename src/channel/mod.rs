//! Asynchronous byte channels with no HART-specific logic.

use core::{future::Future, pin::Pin};

use thiserror::Error;

#[cfg(feature = "serial")]
mod serial;
#[cfg(feature = "tcp")]
mod tcp;

#[cfg(feature = "serial")]
pub use serial::{SerialChannel, SerialOptions};
#[cfg(feature = "tcp")]
pub use tcp::{
    DEFAULT_CONNECT_TIMEOUT, MAXIMUM_TCP_CONFIGURATION_DURATION, TcpChannel, TcpOptions,
};

/// Type-erased future returned by a channel operation.
pub type ChannelFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, ChannelError>> + Send + 'a>>;

/// Transport-channel error.
#[derive(Debug, Error)]
pub enum ChannelError {
    /// The operating system returned an I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The serial-port driver rejected the operation.
    #[cfg(feature = "serial")]
    #[error("serial-port error: {0}")]
    Serial(#[from] tokio_serial::Error),
    /// The connection closed before the exchange completed.
    #[error("channel closed by the remote peer")]
    Closed,
    /// A transport operation exceeded its explicit deadline.
    #[error("channel {0} timeout expired")]
    Timeout(&'static str),
    /// The channel rejected an invalid configuration.
    #[error("invalid channel configuration: {0}")]
    Configuration(&'static str),
    /// A transport-specific protocol rejected a packet or session transition.
    #[error("transport protocol error: {0}")]
    Protocol(String),
}

/// Sequential byte stream with a single-owner guarantee.
///
/// `receive` must be cancellation-safe: dropping its future before completion must not consume
/// bytes or corrupt framing. The runner may cancel an idle read when queued work becomes ready.
/// A cancelled or timed-out `send` is treated as indeterminate and the runner permanently stops
/// using that channel, so custom implementations do not need to recover a partial write.
pub trait ByteChannel: Send {
    /// Sends the complete chunk or returns an error.
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()>;

    /// Receives the next nonempty chunk into the supplied buffer.
    fn receive<'a>(&'a mut self, buffer: &'a mut [u8]) -> ChannelFuture<'a, usize>;

    /// Flushes transport buffers when supported.
    fn flush(&mut self) -> ChannelFuture<'_, ()>;
}
