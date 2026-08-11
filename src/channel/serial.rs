use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_serial::{DataBits, FlowControl, Parity, SerialPortBuilderExt, SerialStream, StopBits};

use crate::channel::{ByteChannel, ChannelError, ChannelFuture};

/// HART serial-modem settings.
#[derive(Debug, Clone)]
pub struct SerialOptions {
    /// Serial-port path.
    pub path: PathBuf,
    /// Link speed.
    pub baud_rate: u32,
}

impl SerialOptions {
    /// Creates settings using the standard HART FSK speed.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            baud_rate: 1200,
        }
    }

    /// Overrides the serial rate for a confirmed modem or gateway mode.
    pub const fn with_baud_rate(mut self, baud_rate: u32) -> Self {
        self.baud_rate = baud_rate;
        self
    }
}

/// Serial byte channel.
#[derive(Debug)]
pub struct SerialChannel {
    stream: SerialStream,
}

impl SerialChannel {
    /// Opens and configures the port for 8O1 without flow control.
    pub fn open(options: &SerialOptions) -> Result<Self, ChannelError> {
        let stream = tokio_serial::new(path_string(&options.path)?, options.baud_rate)
            .data_bits(DataBits::Eight)
            .parity(Parity::Odd)
            .stop_bits(StopBits::One)
            .flow_control(FlowControl::None)
            .open_native_async()?;
        Ok(Self { stream })
    }
}

fn path_string(path: &Path) -> Result<&str, ChannelError> {
    path.to_str().ok_or(ChannelError::Configuration(
        "serial-port path is not valid UTF-8",
    ))
}

impl ByteChannel for SerialChannel {
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
