//! Byte-exact channel recording and deterministic scenario replay.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::channel::{ByteChannel, ChannelError, ChannelFuture};

/// Default upper bound for retained capture payload bytes.
pub const DEFAULT_MAXIMUM_TRACE_BYTES: usize = 16 * 1024 * 1024;
/// Hard upper bound for retained trace payload bytes.
pub const MAXIMUM_TRACE_BYTES: usize = 256 * 1024 * 1024;
/// Hard upper bound for retained trace chunk metadata.
pub const MAXIMUM_TRACE_RECORDS: usize = 1_048_576;
/// Default upper bound for steps retained by one replay channel.
pub const DEFAULT_MAXIMUM_REPLAY_STEPS: usize = 65_536;
/// Default upper bound for payload bytes retained by one replay channel.
pub const DEFAULT_MAXIMUM_REPLAY_BYTES: usize = 16 * 1024 * 1024;
/// Hard upper bound for replay step metadata.
pub const MAXIMUM_REPLAY_STEPS: usize = 1_048_576;
/// Hard upper bound for replay payload bytes.
pub const MAXIMUM_REPLAY_BYTES: usize = 256 * 1024 * 1024;

/// Independent limits for a byte-exact capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceLimits {
    /// Maximum number of retained chunks.
    pub maximum_records: usize,
    /// Maximum combined payload size of retained chunks.
    pub maximum_bytes: usize,
}

impl Default for TraceLimits {
    fn default() -> Self {
        Self {
            maximum_records: 4096,
            maximum_bytes: DEFAULT_MAXIMUM_TRACE_BYTES,
        }
    }
}

impl TraceLimits {
    /// Sets the retained chunk-count limit.
    pub const fn with_maximum_records(mut self, maximum_records: usize) -> Self {
        self.maximum_records = maximum_records;
        self
    }

    /// Sets the aggregate retained payload-byte limit.
    pub const fn with_maximum_bytes(mut self, maximum_bytes: usize) -> Self {
        self.maximum_bytes = maximum_bytes;
        self
    }

    /// Validates strict recorder resource limits.
    pub const fn validate(self) -> Result<(), TraceLimitsError> {
        if self.maximum_records == 0 || self.maximum_bytes == 0 {
            return Err(TraceLimitsError::ZeroLimit);
        }
        if self.maximum_records > MAXIMUM_TRACE_RECORDS {
            return Err(TraceLimitsError::RecordLimit(self.maximum_records));
        }
        if self.maximum_bytes > MAXIMUM_TRACE_BYTES {
            return Err(TraceLimitsError::ByteLimit(self.maximum_bytes));
        }
        Ok(())
    }
}

/// Invalid strict trace recorder limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TraceLimitsError {
    /// A recorder limit would prevent all progress.
    #[error("trace limits must be greater than zero")]
    ZeroLimit,
    /// The requested record count is implausibly large.
    #[error("trace record limit {0} exceeds the supported limit")]
    RecordLimit(usize),
    /// The requested retained payload size is implausibly large.
    #[error("trace byte limit {0} exceeds the supported limit")]
    ByteLimit(usize),
}

/// Direction of a recorded chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceDirection {
    /// From the host application to the link.
    Outbound,
    /// From the link to the host application.
    Inbound,
}

/// One chunk with its host-system timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceRecord {
    /// Direction.
    pub direction: TraceDirection,
    /// Capture time.
    pub timestamp: SystemTime,
    /// Unmodified bytes.
    pub bytes: Vec<u8>,
}

/// Bounded exchange recorder.
#[derive(Debug, Clone)]
pub struct Trace {
    records: VecDeque<TraceRecord>,
    limits: TraceLimits,
    retained_bytes: usize,
    dropped_records: u64,
    dropped_bytes: u64,
}

impl Trace {
    /// Creates a recorder that evicts the oldest records when full.
    pub fn new(maximum_records: usize) -> Self {
        Self::with_limits(TraceLimits {
            maximum_records,
            ..TraceLimits::default()
        })
    }

    /// Creates a recorder with independent chunk-count and payload-byte limits.
    pub fn with_limits(limits: TraceLimits) -> Self {
        Self {
            records: VecDeque::new(),
            limits: TraceLimits {
                maximum_records: limits.maximum_records.clamp(1, MAXIMUM_TRACE_RECORDS),
                maximum_bytes: limits.maximum_bytes.clamp(1, MAXIMUM_TRACE_BYTES),
            },
            retained_bytes: 0,
            dropped_records: 0,
            dropped_bytes: 0,
        }
    }

    /// Creates a recorder after rejecting invalid limits instead of clamping them.
    pub fn try_with_limits(limits: TraceLimits) -> Result<Self, TraceLimitsError> {
        limits.validate()?;
        Ok(Self::with_limits(limits))
    }

    /// Adds a record.
    pub fn push(&mut self, record: TraceRecord) {
        let record_bytes = record.bytes.len();
        if record_bytes > self.limits.maximum_bytes {
            self.note_dropped(record_bytes);
            return;
        }
        while self.records.len() >= self.limits.maximum_records
            || self.retained_bytes.saturating_add(record_bytes) > self.limits.maximum_bytes
        {
            let Some(evicted) = self.records.pop_front() else {
                break;
            };
            self.retained_bytes = self.retained_bytes.saturating_sub(evicted.bytes.len());
            self.note_dropped(evicted.bytes.len());
        }
        self.retained_bytes = self.retained_bytes.saturating_add(record_bytes);
        self.records.push_back(record);
    }

    /// Returns records in chronological order.
    pub fn records(&self) -> impl ExactSizeIterator<Item = &TraceRecord> {
        self.records.iter()
    }

    /// Returns the number of evicted old records.
    pub const fn dropped(&self) -> u64 {
        self.dropped_records
    }

    /// Returns the number of payload bytes currently retained.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Returns the combined payload size of evicted or rejected records.
    pub const fn dropped_bytes(&self) -> u64 {
        self.dropped_bytes
    }

    /// Returns the effective capture limits.
    pub const fn limits(&self) -> TraceLimits {
        self.limits
    }

    /// Produces PCAP-NG with the USER0 link type.
    pub fn to_pcapng(&self) -> Vec<u8> {
        let mut output = Vec::new();
        write_section_header(&mut output);
        write_interface(&mut output);
        for record in &self.records {
            write_packet(&mut output, record);
        }
        output
    }

    fn note_dropped(&mut self, bytes: usize) {
        self.dropped_records = self.dropped_records.saturating_add(1);
        self.dropped_bytes = self
            .dropped_bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
    }
}

/// Channel that records incoming and outgoing chunks.
#[derive(Debug)]
pub struct RecordingChannel<C> {
    inner: C,
    trace: Arc<Mutex<Trace>>,
}

impl<C> RecordingChannel<C> {
    /// Wraps a channel and returns the shared recorder.
    pub fn new(inner: C, maximum_records: usize) -> (Self, Arc<Mutex<Trace>>) {
        Self::with_limits(
            inner,
            TraceLimits {
                maximum_records,
                ..TraceLimits::default()
            },
        )
    }

    /// Wraps a channel with independent capture record and byte limits.
    pub fn with_limits(inner: C, limits: TraceLimits) -> (Self, Arc<Mutex<Trace>>) {
        let trace = Arc::new(Mutex::new(Trace::with_limits(limits)));
        (
            Self {
                inner,
                trace: trace.clone(),
            },
            trace,
        )
    }

    /// Wraps a channel after rejecting invalid recorder limits.
    pub fn try_with_limits(
        inner: C,
        limits: TraceLimits,
    ) -> Result<(Self, Arc<Mutex<Trace>>), TraceLimitsError> {
        limits.validate()?;
        Ok(Self::with_limits(inner, limits))
    }

    fn record(&self, direction: TraceDirection, bytes: &[u8]) {
        if let Ok(mut trace) = self.trace.lock() {
            trace.push(TraceRecord {
                direction,
                timestamp: SystemTime::now(),
                bytes: bytes.to_vec(),
            });
        }
    }
}

impl<C: ByteChannel> ByteChannel for RecordingChannel<C> {
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            self.inner.send(bytes).await?;
            self.record(TraceDirection::Outbound, bytes);
            Ok(())
        })
    }

    fn receive<'a>(&'a mut self, buffer: &'a mut [u8]) -> ChannelFuture<'a, usize> {
        Box::pin(async move {
            let count = self.inner.receive(buffer).await?;
            if count > buffer.len() {
                return Err(ChannelError::Configuration(
                    "recorded channel returned more bytes than the receive buffer can hold",
                ));
            }
            self.record(TraceDirection::Inbound, &buffer[..count]);
            Ok(count)
        })
    }

    fn flush(&mut self) -> ChannelFuture<'_, ()> {
        self.inner.flush()
    }
}

/// One step in a replayable scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayStep {
    /// Require an exact outgoing sequence.
    Expect(Vec<u8>),
    /// Return an incoming chunk.
    Provide(Vec<u8>),
    /// Return a transport error.
    Fail,
}

impl ReplayStep {
    const fn payload_bytes(&self) -> usize {
        match self {
            Self::Expect(bytes) | Self::Provide(bytes) => bytes.len(),
            Self::Fail => 0,
        }
    }
}

/// Independent construction limits for a deterministic replay scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayLimits {
    /// Maximum number of retained scenario steps.
    pub maximum_steps: usize,
    /// Maximum combined payload size of all steps.
    pub maximum_bytes: usize,
}

impl Default for ReplayLimits {
    fn default() -> Self {
        Self {
            maximum_steps: DEFAULT_MAXIMUM_REPLAY_STEPS,
            maximum_bytes: DEFAULT_MAXIMUM_REPLAY_BYTES,
        }
    }
}

impl ReplayLimits {
    /// Sets the scenario step-count limit.
    pub const fn with_maximum_steps(mut self, maximum_steps: usize) -> Self {
        self.maximum_steps = maximum_steps;
        self
    }

    /// Sets the aggregate scenario payload-byte limit.
    pub const fn with_maximum_bytes(mut self, maximum_bytes: usize) -> Self {
        self.maximum_bytes = maximum_bytes;
        self
    }

    /// Validates replay construction limits.
    pub const fn validate(self) -> Result<(), ReplayBuildError> {
        if self.maximum_steps == 0 || self.maximum_bytes == 0 {
            return Err(ReplayBuildError::ZeroLimit);
        }
        if self.maximum_steps > MAXIMUM_REPLAY_STEPS {
            return Err(ReplayBuildError::StepLimit(self.maximum_steps));
        }
        if self.maximum_bytes > MAXIMUM_REPLAY_BYTES {
            return Err(ReplayBuildError::ByteLimit(self.maximum_bytes));
        }
        Ok(())
    }
}

/// Invalid replay limits or a scenario that exceeds them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReplayBuildError {
    /// A construction limit would prevent useful scenarios.
    #[error("replay limits must be greater than zero")]
    ZeroLimit,
    /// The configured step count exceeds the hard safety bound.
    #[error("replay step limit {0} exceeds the supported limit")]
    StepLimit(usize),
    /// The configured payload budget exceeds the hard safety bound.
    #[error("replay byte limit {0} exceeds the supported limit")]
    ByteLimit(usize),
    /// The supplied scenario contains too many steps.
    #[error("replay scenario exceeds its {0}-step limit")]
    ScenarioSteps(usize),
    /// The supplied scenario retains too many payload bytes.
    #[error("replay scenario exceeds its {0}-byte limit")]
    ScenarioBytes(usize),
}

/// Channel that executes a predefined scenario.
#[derive(Debug)]
pub struct ReplayChannel {
    steps: VecDeque<ReplayStep>,
    pending: VecDeque<u8>,
}

impl ReplayChannel {
    /// Creates a replay channel with bounded default limits.
    pub fn new(steps: impl IntoIterator<Item = ReplayStep>) -> Result<Self, ReplayBuildError> {
        Self::with_limits(steps, ReplayLimits::default())
    }

    /// Creates a replay channel after validating the complete scenario.
    ///
    /// Iteration stops as soon as either limit is exceeded, so even an
    /// accidentally infinite iterator cannot make construction hang forever.
    pub fn with_limits(
        steps: impl IntoIterator<Item = ReplayStep>,
        limits: ReplayLimits,
    ) -> Result<Self, ReplayBuildError> {
        limits.validate()?;
        let mut retained_bytes = 0usize;
        let mut bounded = VecDeque::new();
        for step in steps {
            if bounded.len() >= limits.maximum_steps {
                return Err(ReplayBuildError::ScenarioSteps(limits.maximum_steps));
            }
            retained_bytes = retained_bytes
                .checked_add(step.payload_bytes())
                .ok_or(ReplayBuildError::ScenarioBytes(limits.maximum_bytes))?;
            if retained_bytes > limits.maximum_bytes {
                return Err(ReplayBuildError::ScenarioBytes(limits.maximum_bytes));
            }
            bounded.push_back(step);
        }
        Ok(Self {
            steps: bounded,
            pending: VecDeque::new(),
        })
    }

    /// Reports whether the complete scenario was consumed.
    pub fn is_complete(&self) -> bool {
        self.steps.is_empty() && self.pending.is_empty()
    }
}

impl ByteChannel for ReplayChannel {
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            match self.steps.pop_front() {
                Some(ReplayStep::Expect(expected)) if expected == bytes => Ok(()),
                Some(ReplayStep::Expect(_)) => Err(ChannelError::Configuration(
                    "outgoing bytes do not match the scenario",
                )),
                Some(_) => Err(ChannelError::Configuration(
                    "scenario expected a read, not a write",
                )),
                None => Err(ChannelError::Closed),
            }
        })
    }

    fn receive<'a>(&'a mut self, buffer: &'a mut [u8]) -> ChannelFuture<'a, usize> {
        Box::pin(async move {
            if buffer.is_empty() {
                return Err(ChannelError::Configuration(
                    "replay receive buffer cannot be empty",
                ));
            }
            if self.pending.is_empty() {
                match self.steps.pop_front() {
                    Some(ReplayStep::Provide(bytes)) => self.pending.extend(bytes),
                    Some(ReplayStep::Fail) => {
                        return Err(ChannelError::Configuration("scheduled read failure"));
                    }
                    Some(ReplayStep::Expect(_)) => {
                        return Err(ChannelError::Configuration(
                            "scenario expected a write, not a read",
                        ));
                    }
                    None => return Err(ChannelError::Closed),
                }
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

fn write_section_header(output: &mut Vec<u8>) {
    push_u32(output, 0x0a0d_0d0a);
    push_u32(output, 28);
    push_u32(output, 0x1a2b_3c4d);
    output.extend_from_slice(&1u16.to_le_bytes());
    output.extend_from_slice(&0u16.to_le_bytes());
    output.extend_from_slice(&u64::MAX.to_le_bytes());
    push_u32(output, 28);
}

fn write_interface(output: &mut Vec<u8>) {
    push_u32(output, 1);
    push_u32(output, 20);
    output.extend_from_slice(&147u16.to_le_bytes());
    output.extend_from_slice(&0u16.to_le_bytes());
    push_u32(output, 65_535);
    push_u32(output, 20);
}

fn write_packet(output: &mut Vec<u8>, record: &TraceRecord) {
    let mut packet = Vec::with_capacity(record.bytes.len() + 1);
    packet.push(match record.direction {
        TraceDirection::Outbound => 0,
        TraceDirection::Inbound => 1,
    });
    packet.extend_from_slice(&record.bytes);
    let padded = (packet.len() + 3) & !3;
    let total = 32 + padded;
    let timestamp = u64::try_from(
        record
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_micros(),
    )
    .unwrap_or(u64::MAX);
    let timestamp_bytes = timestamp.to_be_bytes();
    push_u32(output, 6);
    push_u32(output, u32::try_from(total).unwrap_or(u32::MAX));
    push_u32(output, 0);
    push_u32(
        output,
        u32::from_be_bytes(timestamp_bytes[..4].try_into().unwrap_or_default()),
    );
    push_u32(
        output,
        u32::from_be_bytes(timestamp_bytes[4..].try_into().unwrap_or_default()),
    );
    push_u32(output, u32::try_from(packet.len()).unwrap_or(u32::MAX));
    push_u32(output, u32::try_from(packet.len()).unwrap_or(u32::MAX));
    output.extend_from_slice(&packet);
    output.resize(output.len() + padded - packet.len(), 0);
    push_u32(output, u32::try_from(total).unwrap_or(u32::MAX));
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}
