//! HART-IP packet transport and managed session.

use std::collections::VecDeque;

use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
    time::timeout,
};

use crate::channel::{ByteChannel, ChannelError, ChannelFuture};

/// Standard HART-IP server port.
pub const DEFAULT_PORT: u16 = 5094;

/// HART-IP protocol version used by the public Version 1 packet format.
pub const PROTOCOL_VERSION_1: u8 = 1;
/// Largest body representable by the 16-bit HART-IP packet length.
pub const MAXIMUM_IP_BODY_BYTES: usize = 65_535 - IpPacket::HEADER_SIZE;
/// Hard upper bound for retained Publish packet metadata.
pub const MAXIMUM_IP_PUBLISHED_PACKETS: usize = 4096;
/// Hard upper bound for stale non-Publish packets skipped by one exchange.
pub const MAXIMUM_IP_UNMATCHED_PACKETS: usize = 4096;
/// Hard upper bound for direct HART-IP I/O and exchange deadlines.
pub const MAXIMUM_IP_TIMEOUT: core::time::Duration = core::time::Duration::from_hours(24);
/// Default aggregate Publish payload retention limit.
pub const DEFAULT_MAXIMUM_IP_PUBLISHED_BYTES: usize = 1024 * 1024;
/// Hard upper bound for retained Publish payload bytes.
pub const MAXIMUM_IP_PUBLISHED_BYTES: usize = 64 * 1024 * 1024;

/// HART-IP message type carried in the low nibble of header byte 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Client request.
    Request = 0,
    /// Server response.
    Response = 1,
    /// Published device message.
    Publish = 2,
    /// Negative acknowledgement.
    Nak = 15,
}

impl TryFrom<u8> for MessageType {
    type Error = IpError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Request),
            1 => Ok(Self::Response),
            2 => Ok(Self::Publish),
            15 => Ok(Self::Nak),
            _ => Err(IpError::MessageType(value)),
        }
    }
}

/// Standard HART-IP Version 1 message identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageId {
    /// Opens a logical client session.
    SessionInitiate = 0,
    /// Closes a logical client session.
    SessionClose = 1,
    /// Prevents inactivity expiration.
    KeepAlive = 2,
    /// Carries a traditional HART token-passing PDU.
    TokenPassingPdu = 3,
}

impl From<MessageId> for u8 {
    fn from(value: MessageId) -> Self {
        value as Self
    }
}

/// One validated HART-IP packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpPacket {
    /// Packet-protocol version.
    pub version: u8,
    /// Message purpose.
    pub message_type: MessageType,
    /// Application-message identifier.
    pub message_id: u8,
    /// Packet-processing status.
    pub status: u8,
    /// Session sequence number.
    pub sequence: u16,
    /// Packet payload.
    pub body: Vec<u8>,
}

impl IpPacket {
    /// Fixed-header length.
    pub const HEADER_SIZE: usize = 8;

    /// Encodes the header and body in network byte order.
    pub fn encode(&self) -> Result<Vec<u8>, IpError> {
        let total = Self::HEADER_SIZE
            .checked_add(self.body.len())
            .ok_or(IpError::Length(self.body.len()))?;
        let total = u16::try_from(total).map_err(|_| IpError::Length(self.body.len()))?;
        let mut bytes = Vec::with_capacity(usize::from(total));
        bytes.extend_from_slice(&[
            self.version,
            self.message_type as u8,
            self.message_id,
            self.status,
        ]);
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&total.to_be_bytes());
        bytes.extend_from_slice(&self.body);
        Ok(bytes)
    }

    /// Decodes exactly one complete packet.
    pub fn decode(bytes: &[u8], maximum_body: usize) -> Result<Self, IpError> {
        if bytes.len() < Self::HEADER_SIZE {
            return Err(IpError::Truncated);
        }
        let total = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
        if total != bytes.len() || total < Self::HEADER_SIZE {
            return Err(IpError::PacketLength {
                declared: total,
                actual: bytes.len(),
            });
        }
        let body_len = total - Self::HEADER_SIZE;
        if body_len > maximum_body {
            return Err(IpError::BodyLimit(body_len));
        }
        if bytes[1] & 0xf0 != 0 {
            return Err(IpError::ReservedMessageBits(bytes[1] & 0xf0));
        }
        Ok(Self {
            version: bytes[0],
            message_type: MessageType::try_from(bytes[1] & 0x0f)?,
            message_id: bytes[2],
            status: bytes[3],
            sequence: u16::from_be_bytes([bytes[4], bytes[5]]),
            body: bytes[Self::HEADER_SIZE..].to_vec(),
        })
    }
}

/// Packet-session error.
#[derive(Debug, Error)]
pub enum IpError {
    /// Socket error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// A packet is truncated.
    #[error("HART-IP packet is truncated")]
    Truncated,
    /// Unknown message purpose.
    #[error("unknown HART-IP message type {0}")]
    MessageType(u8),
    /// Reserved high message-type bits are nonzero.
    #[error("reserved HART-IP message-type bits are nonzero: 0x{0:02x}")]
    ReservedMessageBits(u8),
    /// The body does not fit in the length field.
    #[error("body length {0} does not fit in a packet")]
    Length(usize),
    /// The declared length does not match the actual length.
    #[error("packet declared length {declared}, but {actual} bytes were received")]
    PacketLength {
        /// Header value.
        declared: usize,
        /// Actual length.
        actual: usize,
    },
    /// The body exceeds the local limit.
    #[error("{0}-byte HART-IP body exceeds the configured limit")]
    BodyLimit(usize),
    /// A response does not match the session request.
    #[error("too many HART-IP responses did not match the expected sequence and message ID")]
    Sequence,
    /// A peer packet uses a different packet-protocol version.
    #[error("expected HART-IP protocol version {expected}, received {actual}")]
    ProtocolVersion {
        /// Version emitted by this session.
        expected: u8,
        /// Version received from the peer.
        actual: u8,
    },
    /// The server rejected the request.
    #[error("HART-IP server returned status {0}")]
    RemoteStatus(u8),
    /// A response uses an unexpected message type.
    #[error("HART-IP server returned unexpected message type {0:?}")]
    UnexpectedMessageType(MessageType),
    /// A logical session is already open.
    #[error("HART-IP session is already open")]
    AlreadyOpen,
    /// A message requires an open logical session.
    #[error("HART-IP session is not open")]
    NotOpen,
    /// The inactivity timeout does not fit the Version 1 session request.
    #[error("HART-IP inactivity timeout does not fit in milliseconds")]
    InactivityTimeout,
    /// A configured I/O or exchange deadline expired.
    #[error("HART-IP {0} timeout expired")]
    Timeout(&'static str),
    /// A timeout must allow the session to make progress.
    #[error("HART-IP timeout must be greater than zero")]
    ZeroTimeout,
    /// A configured deadline is implausibly large.
    #[error("HART-IP {0} timeout exceeds the supported limit")]
    TimeoutLimit(&'static str),
    /// A previous cancelled or failed I/O operation left stream framing uncertain.
    #[error("HART-IP stream must be reconnected after an indeterminate I/O result")]
    StreamUnusable,
}

/// Parameters of a Version 1 Session-Initiate request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionOptions {
    /// Server-side inactivity close time.
    pub inactivity_close: core::time::Duration,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            inactivity_close: core::time::Duration::from_mins(1),
        }
    }
}

/// Deadlines enforced even when [`IpSession`] is used without the queue runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpTimeouts {
    /// Maximum duration of one complete socket read or write.
    pub io: core::time::Duration,
    /// Maximum duration of one request-response exchange, including Publish packets.
    pub exchange: core::time::Duration,
}

impl Default for IpTimeouts {
    fn default() -> Self {
        Self {
            io: core::time::Duration::from_secs(10),
            exchange: core::time::Duration::from_secs(30),
        }
    }
}

impl IpTimeouts {
    /// Rejects deadlines that cannot make progress or are implausibly large.
    pub fn validate(self) -> Result<(), IpError> {
        if self.io.is_zero() || self.exchange.is_zero() {
            return Err(IpError::ZeroTimeout);
        }
        if self.io > MAXIMUM_IP_TIMEOUT {
            return Err(IpError::TimeoutLimit("I/O"));
        }
        if self.exchange > MAXIMUM_IP_TIMEOUT {
            return Err(IpError::TimeoutLimit("exchange"));
        }
        Ok(())
    }
}

/// Bounded HART-IP session counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IpSessionSnapshot {
    /// Publish packets currently waiting for the application.
    pub published_queued: usize,
    /// Combined body bytes retained in the Publish queue.
    pub published_bytes: usize,
    /// Publish packets discarded because their queue was disabled or full.
    pub published_dropped: u64,
    /// Stale responses skipped while finding the matching sequence.
    pub unmatched_skipped: u64,
}

/// Packet session over an arbitrary asynchronous stream.
#[derive(Debug)]
pub struct IpSession<S> {
    stream: S,
    next_sequence: u16,
    maximum_body: usize,
    protocol_version: u8,
    open: bool,
    published: VecDeque<IpPacket>,
    published_bytes: usize,
    maximum_published: usize,
    maximum_published_bytes: usize,
    maximum_unmatched: usize,
    timeouts: IpTimeouts,
    published_dropped: u64,
    unmatched_skipped: u64,
    usable: bool,
}

impl IpSession<TcpStream> {
    /// Connects to a TCP server.
    pub async fn connect(address: impl ToSocketAddrs) -> Result<Self, IpError> {
        let stream = timeout(IpTimeouts::default().io, TcpStream::connect(address))
            .await
            .map_err(|_| IpError::Timeout("connect"))??;
        stream.set_nodelay(true)?;
        Ok(Self::new(stream))
    }
}

impl<S> IpSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Creates a session over an existing secured or plain stream.
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            next_sequence: 1,
            maximum_body: 8192,
            protocol_version: PROTOCOL_VERSION_1,
            open: false,
            published: VecDeque::new(),
            published_bytes: 0,
            maximum_published: 128,
            maximum_published_bytes: DEFAULT_MAXIMUM_IP_PUBLISHED_BYTES,
            maximum_unmatched: 16,
            timeouts: IpTimeouts {
                io: core::time::Duration::from_secs(10),
                exchange: core::time::Duration::from_secs(30),
            },
            published_dropped: 0,
            unmatched_skipped: 0,
            usable: true,
        }
    }

    /// Sets the incoming-packet body limit.
    pub const fn with_maximum_body(mut self, maximum_body: usize) -> Self {
        self.maximum_body = if maximum_body > MAXIMUM_IP_BODY_BYTES {
            MAXIMUM_IP_BODY_BYTES
        } else {
            maximum_body
        };
        self
    }

    /// Sets the maximum number of Publish packets retained while awaiting a response.
    pub fn with_maximum_published(mut self, maximum_published: usize) -> Self {
        self.maximum_published = maximum_published.min(MAXIMUM_IP_PUBLISHED_PACKETS);
        self.enforce_published_limits();
        self
    }

    /// Sets the aggregate Publish body-byte limit; zero disables retention.
    pub fn with_maximum_published_bytes(mut self, maximum_published_bytes: usize) -> Self {
        self.maximum_published_bytes = maximum_published_bytes.min(MAXIMUM_IP_PUBLISHED_BYTES);
        self.enforce_published_limits();
        self
    }

    /// Sets how many stale non-Publish packets may be skipped during one exchange.
    pub const fn with_maximum_unmatched(mut self, maximum_unmatched: usize) -> Self {
        self.maximum_unmatched = if maximum_unmatched > MAXIMUM_IP_UNMATCHED_PACKETS {
            MAXIMUM_IP_UNMATCHED_PACKETS
        } else {
            maximum_unmatched
        };
        self
    }

    /// Replaces socket and whole-exchange deadlines.
    pub fn with_timeouts(mut self, timeouts: IpTimeouts) -> Result<Self, IpError> {
        timeouts.validate()?;
        self.timeouts = timeouts;
        Ok(self)
    }

    /// Returns bounded queue and stale-packet counters.
    pub fn snapshot(&self) -> IpSessionSnapshot {
        IpSessionSnapshot {
            published_queued: self.published.len(),
            published_bytes: self.published_bytes,
            published_dropped: self.published_dropped,
            unmatched_skipped: self.unmatched_skipped,
        }
    }

    /// Returns the effective incoming body limit.
    pub const fn maximum_body(&self) -> usize {
        self.maximum_body
    }

    /// Returns the effective retained Publish packet-count limit.
    pub const fn maximum_published(&self) -> usize {
        self.maximum_published
    }

    /// Returns the effective retained Publish body-byte limit.
    pub const fn maximum_published_bytes(&self) -> usize {
        self.maximum_published_bytes
    }

    /// Returns the effective stale-response skip limit.
    pub const fn maximum_unmatched(&self) -> usize {
        self.maximum_unmatched
    }

    /// Reports whether Session Initiate completed successfully.
    pub const fn is_open(&self) -> bool {
        self.open
    }

    /// Reports whether framed I/O can continue without reconnecting.
    pub const fn is_usable(&self) -> bool {
        self.usable
    }

    /// Returns the protocol version emitted by this client.
    pub const fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Sends a packet and returns the sequence number used.
    pub async fn send(
        &mut self,
        message_type: MessageType,
        message_id: u8,
        body: &[u8],
    ) -> Result<u16, IpError> {
        if !self.usable {
            return Err(IpError::StreamUnusable);
        }
        if body.len() > MAXIMUM_IP_BODY_BYTES {
            return Err(IpError::Length(body.len()));
        }
        let sequence = self.next_sequence;
        let packet = IpPacket {
            version: self.protocol_version,
            message_type,
            message_id,
            status: 0,
            sequence,
            body: body.to_vec(),
        };
        let encoded = packet.encode()?;
        let result = timeout(self.timeouts.io, async {
            self.stream.write_all(&encoded).await?;
            self.stream.flush().await
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.usable = false;
                return Err(IpError::Io(error));
            }
            Err(_) => {
                self.usable = false;
                return Err(IpError::Timeout("write"));
            }
        }
        self.next_sequence = self.next_sequence.wrapping_add(1);
        Ok(sequence)
    }

    /// Receives one complete packet while enforcing the memory limit.
    pub async fn receive(&mut self) -> Result<IpPacket, IpError> {
        if !self.usable {
            return Err(IpError::StreamUnusable);
        }
        match timeout(self.timeouts.io, self.receive_unbounded()).await {
            Ok(Err(IpError::Io(error))) => {
                self.usable = false;
                Err(IpError::Io(error))
            }
            Ok(Err(error @ IpError::PacketLength { .. })) => {
                self.usable = false;
                Err(error)
            }
            Ok(result) => result,
            Err(_) => {
                self.usable = false;
                Err(IpError::Timeout("read"))
            }
        }
    }

    async fn receive_unbounded(&mut self) -> Result<IpPacket, IpError> {
        let mut header = [0; IpPacket::HEADER_SIZE];
        self.stream.read_exact(&mut header).await?;
        let total = usize::from(u16::from_be_bytes([header[6], header[7]]));
        if total < IpPacket::HEADER_SIZE {
            return Err(IpError::PacketLength {
                declared: total,
                actual: IpPacket::HEADER_SIZE,
            });
        }
        let body_len = total - IpPacket::HEADER_SIZE;
        if body_len > self.maximum_body {
            discard_exact(&mut self.stream, body_len).await?;
            return Err(IpError::BodyLimit(body_len));
        }
        let mut bytes = header.to_vec();
        bytes.resize(total, 0);
        self.stream
            .read_exact(&mut bytes[IpPacket::HEADER_SIZE..])
            .await?;
        IpPacket::decode(&bytes, self.maximum_body)
    }

    async fn exchange_unchecked(
        &mut self,
        message_id: u8,
        body: &[u8],
    ) -> Result<IpPacket, IpError> {
        let result = timeout(
            self.timeouts.exchange,
            self.exchange_unchecked_without_total_timeout(message_id, body),
        )
        .await;
        if let Ok(value) = result {
            value
        } else {
            self.usable = false;
            Err(IpError::Timeout("exchange"))
        }
    }

    async fn exchange_unchecked_without_total_timeout(
        &mut self,
        message_id: u8,
        body: &[u8],
    ) -> Result<IpPacket, IpError> {
        let sequence = self.send(MessageType::Request, message_id, body).await?;
        let mut unmatched = 0usize;
        loop {
            let packet = self.receive().await?;
            if packet.version != self.protocol_version {
                return Err(IpError::ProtocolVersion {
                    expected: self.protocol_version,
                    actual: packet.version,
                });
            }
            if packet.message_type == MessageType::Publish {
                self.retain_published(packet);
                continue;
            }
            if packet.sequence != sequence || packet.message_id != message_id {
                unmatched = unmatched.saturating_add(1);
                self.unmatched_skipped = self.unmatched_skipped.saturating_add(1);
                if unmatched > self.maximum_unmatched {
                    return Err(IpError::Sequence);
                }
                continue;
            }
            if packet.message_type == MessageType::Nak {
                return Err(IpError::RemoteStatus(packet.status));
            }
            if packet.message_type != MessageType::Response {
                return Err(IpError::UnexpectedMessageType(packet.message_type));
            }
            if packet.status != 0 {
                return Err(IpError::RemoteStatus(packet.status));
            }
            return Ok(packet);
        }
    }

    /// Opens a Version 1 logical session.
    pub async fn open(&mut self, options: SessionOptions) -> Result<IpPacket, IpError> {
        if self.open {
            return Err(IpError::AlreadyOpen);
        }
        let milliseconds = u32::try_from(options.inactivity_close.as_millis())
            .map_err(|_| IpError::InactivityTimeout)?;
        let mut body = Vec::with_capacity(5);
        body.push(1);
        body.extend_from_slice(&milliseconds.to_be_bytes());
        let response = self
            .exchange_unchecked(MessageId::SessionInitiate.into(), &body)
            .await?;
        self.open = true;
        Ok(response)
    }

    /// Sends a request within an open logical session.
    pub async fn request(&mut self, message_id: u8, body: &[u8]) -> Result<IpPacket, IpError> {
        if !self.open {
            return Err(IpError::NotOpen);
        }
        self.exchange_unchecked(message_id, body).await
    }

    /// Sends an empty keepalive request.
    pub async fn keep_alive(&mut self) -> Result<IpPacket, IpError> {
        self.request(MessageId::KeepAlive.into(), &[]).await
    }

    /// Exchanges a traditional HART token-passing PDU.
    pub async fn token_passing(&mut self, body: &[u8]) -> Result<IpPacket, IpError> {
        self.request(MessageId::TokenPassingPdu.into(), body).await
    }

    /// Closes the logical session after a matching response.
    pub async fn close(&mut self) -> Result<IpPacket, IpError> {
        if !self.open {
            return Err(IpError::NotOpen);
        }
        let response = self
            .exchange_unchecked(MessageId::SessionClose.into(), &[])
            .await?;
        self.open = false;
        Ok(response)
    }

    /// Removes the oldest Publish packet retained during request processing.
    pub fn take_published(&mut self) -> Option<IpPacket> {
        let packet = self.published.pop_front()?;
        self.published_bytes = self.published_bytes.saturating_sub(packet.body.len());
        Some(packet)
    }

    fn retain_published(&mut self, packet: IpPacket) {
        let bytes = packet.body.len();
        if self.maximum_published == 0
            || self.maximum_published_bytes == 0
            || bytes > self.maximum_published_bytes
        {
            self.published_dropped = self.published_dropped.saturating_add(1);
            return;
        }
        while self.published.len() >= self.maximum_published
            || self.published_bytes.saturating_add(bytes) > self.maximum_published_bytes
        {
            let Some(evicted) = self.published.pop_front() else {
                break;
            };
            self.published_bytes = self.published_bytes.saturating_sub(evicted.body.len());
            self.published_dropped = self.published_dropped.saturating_add(1);
        }
        self.published_bytes = self.published_bytes.saturating_add(bytes);
        self.published.push_back(packet);
    }

    fn enforce_published_limits(&mut self) {
        while self.published.len() > self.maximum_published
            || self.published_bytes > self.maximum_published_bytes
        {
            let Some(evicted) = self.published.pop_front() else {
                self.published_bytes = 0;
                break;
            };
            self.published_bytes = self.published_bytes.saturating_sub(evicted.body.len());
            self.published_dropped = self.published_dropped.saturating_add(1);
        }
    }
}

async fn discard_exact(
    stream: &mut (impl AsyncRead + Unpin),
    mut remaining: usize,
) -> Result<(), std::io::Error> {
    let mut scratch = [0u8; 1024];
    while remaining > 0 {
        let chunk = remaining.min(scratch.len());
        stream.read_exact(&mut scratch[..chunk]).await?;
        remaining -= chunk;
    }
    Ok(())
}

/// Adapts direct-packet bodies to the shared byte-link runner.
#[derive(Debug)]
pub struct IpPacketChannel<S> {
    session: IpSession<S>,
    message_id: u8,
    pending: VecDeque<u8>,
}

impl<S> IpPacketChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    /// Creates an adapter for one application-message identifier.
    pub const fn new(session: IpSession<S>, message_id: u8) -> Self {
        Self {
            session,
            message_id,
            pending: VecDeque::new(),
        }
    }

    /// Returns a reference to the underlying packet session.
    pub const fn session(&self) -> &IpSession<S> {
        &self.session
    }

    /// Recovers the underlying packet session when no byte exchange is active.
    pub fn into_session(self) -> Result<IpSession<S>, ChannelError> {
        if self.pending.is_empty() {
            Ok(self.session)
        } else {
            Err(ChannelError::Configuration(
                "HART-IP response bytes must be drained before recovering the session",
            ))
        }
    }
}

impl<S> ByteChannel for IpPacketChannel<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            if !self.pending.is_empty() {
                return Err(ChannelError::Configuration(
                    "previous HART-IP response bytes were not drained",
                ));
            }
            let packet = self
                .session
                .request(self.message_id, bytes)
                .await
                .map_err(ip_channel_error)?;
            while let Some(published) = self.session.take_published() {
                self.pending.extend(published.body);
            }
            self.pending.extend(packet.body);
            Ok(())
        })
    }

    fn receive<'a>(&'a mut self, buffer: &'a mut [u8]) -> ChannelFuture<'a, usize> {
        Box::pin(async move {
            if buffer.is_empty() {
                return Err(ChannelError::Configuration(
                    "HART-IP receive buffer cannot be empty",
                ));
            }
            if self.pending.is_empty() {
                return Err(ChannelError::Closed);
            }
            let count = buffer.len().min(self.pending.len());
            for destination in &mut buffer[..count] {
                *destination = self.pending.pop_front().unwrap_or_default();
            }
            Ok(count)
        })
    }

    fn flush(&mut self) -> ChannelFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn ip_channel_error(error: IpError) -> ChannelError {
    match error {
        IpError::Io(error) => ChannelError::Io(error),
        _ => ChannelError::Protocol(error.to_string()),
    }
}
