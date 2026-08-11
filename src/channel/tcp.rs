use core::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
    time::timeout,
};

use crate::channel::{ByteChannel, ChannelError, ChannelFuture};

/// Default upper bound for establishing a transparent TCP connection.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Hard upper bound for connect and keepalive configuration durations.
pub const MAXIMUM_TCP_CONFIGURATION_DURATION: Duration = Duration::from_hours(24);

/// Settings for a transparent TCP-to-serial gateway.
#[derive(Debug, Clone, Copy)]
pub struct TcpOptions {
    /// Allows small packets without Nagle delay.
    pub no_delay: bool,
    /// TCP keepalive interval when supported by the system.
    pub keepalive: Option<Duration>,
}

impl Default for TcpOptions {
    fn default() -> Self {
        Self {
            no_delay: true,
            keepalive: Some(Duration::from_secs(30)),
        }
    }
}

impl TcpOptions {
    /// Enables or disables the Nagle algorithm.
    pub const fn with_no_delay(mut self, no_delay: bool) -> Self {
        self.no_delay = no_delay;
        self
    }

    /// Sets the TCP keepalive interval, or disables keepalive with `None`.
    pub const fn with_keepalive(mut self, keepalive: Option<Duration>) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Rejects keepalive settings that are zero or implausibly large.
    pub fn validate(self) -> Result<(), ChannelError> {
        if let Some(keepalive) = self.keepalive {
            if keepalive.is_zero() {
                return Err(ChannelError::Configuration(
                    "TCP keepalive interval must be greater than zero",
                ));
            }
            if keepalive > MAXIMUM_TCP_CONFIGURATION_DURATION {
                return Err(ChannelError::Configuration(
                    "TCP keepalive interval exceeds the supported limit",
                ));
            }
        }
        Ok(())
    }
}

/// Transparent byte channel over TCP.
#[derive(Debug)]
pub struct TcpChannel {
    stream: TcpStream,
}

impl TcpChannel {
    /// Opens a connection to the serial gateway.
    pub async fn connect(
        address: impl ToSocketAddrs,
        options: TcpOptions,
    ) -> Result<Self, ChannelError> {
        Self::connect_with_timeout(address, options, DEFAULT_CONNECT_TIMEOUT).await
    }

    /// Opens a connection with an explicit end-to-end connect timeout.
    pub async fn connect_with_timeout(
        address: impl ToSocketAddrs,
        options: TcpOptions,
        connect_timeout: Duration,
    ) -> Result<Self, ChannelError> {
        options.validate()?;
        if connect_timeout.is_zero() {
            return Err(ChannelError::Configuration(
                "TCP connect timeout must be greater than zero",
            ));
        }
        if connect_timeout > MAXIMUM_TCP_CONFIGURATION_DURATION {
            return Err(ChannelError::Configuration(
                "TCP connect timeout exceeds the supported limit",
            ));
        }
        let stream = timeout(connect_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| ChannelError::Timeout("connect"))??;
        stream.set_nodelay(options.no_delay)?;
        let socket = SockRef::from(&stream);
        if let Some(interval) = options.keepalive {
            socket.set_keepalive(true)?;
            socket.set_tcp_keepalive(
                &TcpKeepalive::new()
                    .with_time(interval)
                    .with_interval(interval),
            )?;
        } else {
            socket.set_keepalive(false)?;
        }
        Ok(Self { stream })
    }

    /// Wraps an already connected stream.
    pub const fn from_stream(stream: TcpStream) -> Self {
        Self { stream }
    }

    /// Returns a reference to the underlying stream.
    pub const fn stream(&self) -> &TcpStream {
        &self.stream
    }
}

impl ByteChannel for TcpChannel {
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            self.stream.write_all(bytes).await?;
            self.stream.flush().await?;
            Ok(())
        })
    }

    fn receive<'a>(&'a mut self, buffer: &'a mut [u8]) -> ChannelFuture<'a, usize> {
        Box::pin(async move {
            let read = self.stream.read(buffer).await?;
            if read == 0 {
                return Err(ChannelError::Closed);
            }
            Ok(read)
        })
    }

    fn flush(&mut self) -> ChannelFuture<'_, ()> {
        Box::pin(async move {
            self.stream.flush().await?;
            Ok(())
        })
    }
}
