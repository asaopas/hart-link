//! Deterministic in-memory link and a simple field device.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use tokio::sync::{Mutex, mpsc};

use crate::{
    Address,
    channel::{ByteChannel, ChannelError, ChannelFuture},
    wire::{DecodeEvent, DecodeLimits, Frame, FrameDecoder, FrameKind},
};

/// Hard upper bound for each in-memory channel queue.
pub const MAXIMUM_EMULATOR_CHANNEL_CAPACITY: usize = 65_536;
/// Hard upper bound for noise prepended to one emulated transmission.
pub const MAXIMUM_EMULATOR_NOISE_PREFIX: usize = 1024 * 1024;
/// Hard upper bound for a direct transmission through the emulator.
pub const MAXIMUM_EMULATOR_TRANSMISSION_BYTES: usize = 1024 * 1024;
/// Hard upper bound for an artificial per-fragment delay.
pub const MAXIMUM_EMULATOR_LATENCY: Duration = Duration::from_hours(24);

/// Controlled faults applied to the next transmission.
#[derive(Debug, Clone, Default)]
pub struct FaultPlan {
    /// Drop the next transmission completely.
    pub drop_next: bool,
    /// Echo the transmission back to its sender.
    pub echo: bool,
    /// Corrupt the checksum of the next transmission.
    pub corrupt_next: bool,
    /// Insert unrelated bytes before the next transmission.
    pub noise_prefix: Vec<u8>,
    /// Split the transmission into chunks no larger than the specified size.
    pub fragment_size: Option<usize>,
    /// Artificial delay applied to each chunk.
    pub latency: Duration,
}

impl FaultPlan {
    /// Adds byte-exact noise before the next transmission.
    pub fn with_noise_prefix(mut self, noise: impl Into<Vec<u8>>) -> Self {
        self.noise_prefix = noise.into();
        self
    }

    /// Splits transmissions into chunks of at most this size.
    pub const fn with_fragment_size(mut self, fragment_size: usize) -> Self {
        self.fragment_size = Some(fragment_size);
        self
    }

    /// Adds an artificial delay before every fragment.
    pub const fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    /// Rejects fault settings that could amplify resources or make no progress.
    pub fn validate(&self) -> Result<(), ChannelError> {
        if self.noise_prefix.len() > MAXIMUM_EMULATOR_NOISE_PREFIX {
            return Err(ChannelError::Configuration(
                "emulator noise prefix exceeds the supported limit",
            ));
        }
        if self.fragment_size == Some(0) {
            return Err(ChannelError::Configuration(
                "emulator fragment size must be greater than zero",
            ));
        }
        if self.latency > MAXIMUM_EMULATOR_LATENCY {
            return Err(ChannelError::Configuration(
                "emulator latency exceeds the supported limit",
            ));
        }
        Ok(())
    }

    fn sanitize(&mut self) {
        self.noise_prefix.truncate(MAXIMUM_EMULATOR_NOISE_PREFIX);
        if self.fragment_size == Some(0) {
            self.fragment_size = Some(1);
        }
        self.latency = self.latency.min(MAXIMUM_EMULATOR_LATENCY);
    }
}

/// One endpoint of a bidirectional in-memory link.
#[derive(Debug)]
pub struct MemoryChannel {
    outgoing: mpsc::Sender<Vec<u8>>,
    echo: mpsc::Sender<Vec<u8>>,
    incoming: mpsc::Receiver<Vec<u8>>,
    remainder: VecDeque<u8>,
    faults: Arc<Mutex<FaultPlan>>,
}

impl MemoryChannel {
    /// Creates two connected endpoints with independent fault plans.
    pub fn pair(capacity: usize) -> (Self, Self) {
        Self::pair_with_capacity(capacity.clamp(1, MAXIMUM_EMULATOR_CHANNEL_CAPACITY))
    }

    /// Creates two endpoints after strictly validating queue capacity.
    pub fn try_pair(capacity: usize) -> Result<(Self, Self), ChannelError> {
        if capacity == 0 {
            return Err(ChannelError::Configuration(
                "emulator channel capacity must be greater than zero",
            ));
        }
        if capacity > MAXIMUM_EMULATOR_CHANNEL_CAPACITY {
            return Err(ChannelError::Configuration(
                "emulator channel capacity exceeds the supported limit",
            ));
        }
        Ok(Self::pair_with_capacity(capacity))
    }

    fn pair_with_capacity(capacity: usize) -> (Self, Self) {
        let (left_to_right_tx, left_to_right_rx) = mpsc::channel(capacity);
        let (right_to_left_tx, right_to_left_rx) = mpsc::channel(capacity);
        let left = Self {
            outgoing: left_to_right_tx.clone(),
            echo: right_to_left_tx.clone(),
            incoming: right_to_left_rx,
            remainder: VecDeque::new(),
            faults: Arc::new(Mutex::new(FaultPlan::default())),
        };
        let right = Self {
            outgoing: right_to_left_tx,
            echo: left_to_right_tx,
            incoming: left_to_right_rx,
            remainder: VecDeque::new(),
            faults: Arc::new(Mutex::new(FaultPlan::default())),
        };
        (left, right)
    }

    /// Replaces the fault plan for this direction.
    pub async fn set_faults(&self, mut plan: FaultPlan) {
        plan.sanitize();
        *self.faults.lock().await = plan;
    }

    /// Replaces the fault plan after rejecting unsafe limits.
    pub async fn try_set_faults(&self, plan: FaultPlan) -> Result<(), ChannelError> {
        plan.validate()?;
        *self.faults.lock().await = plan;
        Ok(())
    }

    async fn transmit(&mut self, bytes: &[u8]) -> Result<(), ChannelError> {
        if bytes.len() > MAXIMUM_EMULATOR_TRANSMISSION_BYTES {
            return Err(ChannelError::Configuration(
                "emulator transmission exceeds the supported limit",
            ));
        }
        let mut plan = self.faults.lock().await;
        let echo = plan.echo;
        let drop_next = std::mem::take(&mut plan.drop_next);
        let corrupt_next = std::mem::take(&mut plan.corrupt_next);
        let noise = std::mem::take(&mut plan.noise_prefix);
        let fragment_size = plan.fragment_size.unwrap_or(bytes.len().max(1)).max(1);
        let latency = plan.latency;
        drop(plan);

        if echo {
            self.echo
                .send(bytes.to_vec())
                .await
                .map_err(|_| ChannelError::Closed)?;
        }
        if drop_next {
            return Ok(());
        }
        let mut output = noise;
        output.extend_from_slice(bytes);
        if corrupt_next && let Some(checksum) = output.last_mut() {
            *checksum ^= 0x01;
        }
        for fragment in output.chunks(fragment_size) {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            self.outgoing
                .send(fragment.to_vec())
                .await
                .map_err(|_| ChannelError::Closed)?;
        }
        Ok(())
    }
}

impl ByteChannel for MemoryChannel {
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()> {
        Box::pin(async move { self.transmit(bytes).await })
    }

    fn receive<'a>(&'a mut self, buffer: &'a mut [u8]) -> ChannelFuture<'a, usize> {
        Box::pin(async move {
            if buffer.is_empty() {
                return Err(ChannelError::Configuration(
                    "memory-channel receive buffer cannot be empty",
                ));
            }
            while self.remainder.is_empty() {
                let fragment = tokio::select! {
                    biased;
                    fragment = self.incoming.recv() => fragment.ok_or(ChannelError::Closed)?,
                    () = self.outgoing.closed() => return Err(ChannelError::Closed),
                };
                self.remainder.extend(fragment);
            }
            let count = buffer.len().min(self.remainder.len());
            for destination in &mut buffer[..count] {
                *destination = self.remainder.pop_front().unwrap_or_default();
            }
            Ok(count)
        })
    }

    fn flush(&mut self) -> ChannelFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

/// Identity of a simple emulated device.
#[derive(Debug, Clone, Copy)]
pub struct EmulatedIdentity {
    /// Manufacturer identifier.
    pub manufacturer_id: u16,
    /// Expanded device type.
    pub device_type: u16,
    /// Required request preamble count.
    pub request_preambles: u8,
    /// Universal-command revision.
    pub universal_revision: u8,
    /// Device revision.
    pub device_revision: u8,
    /// Software revision.
    pub software_revision: u8,
    /// Hardware revision.
    pub hardware_revision: u8,
    /// Physical-signal code.
    pub physical_signaling: u8,
    /// Capability flags.
    pub flags: u8,
    /// Unique part of the address.
    pub device_id: u32,
    /// Response preamble count.
    pub response_preambles: u8,
    /// Maximum number of device variables.
    pub maximum_device_variables: u8,
    /// Configuration-change counter.
    pub configuration_change_counter: u16,
    /// Extended status.
    pub extended_status: u8,
    /// Private-label distributor.
    pub private_label_distributor: u16,
    /// Device profile.
    pub device_profile: u8,
}

/// Minimal mutable device state for integration testing.
#[derive(Debug)]
pub struct EmulatedDevice {
    /// Device address.
    pub address: Address,
    /// Identification data.
    pub identity: EmulatedIdentity,
    /// Primary-variable unit.
    pub primary_unit: u8,
    /// Primary variable.
    pub primary_value: f32,
    /// Loop current.
    pub loop_current: f32,
    /// Percent of range.
    pub percent: f32,
    /// Device status.
    pub status: u8,
    /// Additional status flags.
    pub additional_status: Vec<u8>,
}

impl EmulatedDevice {
    /// Serves requests until the channel closes.
    pub async fn run(mut self, mut channel: MemoryChannel) -> Result<(), ChannelError> {
        let mut decoder = FrameDecoder::new(DecodeLimits::default());
        let mut buffer = vec![0; 512];
        loop {
            let read = match channel.receive(&mut buffer).await {
                Ok(read) => read,
                Err(ChannelError::Closed) => return Ok(()),
                Err(error) => return Err(error),
            };
            for event in decoder.push(&buffer[..read]) {
                let DecodeEvent::Frame(request) = event else {
                    continue;
                };
                if request.kind != FrameKind::Request || !self.accepts_address(request.address) {
                    continue;
                }
                let response = self.respond(&request);
                let encoded = response.encode().map_err(|_| {
                    ChannelError::Configuration("emulator generated an invalid frame")
                })?;
                channel.send(&encoded).await?;
            }
        }
    }

    fn accepts_address(&self, address: Address) -> bool {
        if address == self.address {
            return true;
        }
        let address_device_type = if self.identity.universal_revision >= 7 {
            self.identity.device_type & 0x3fff
        } else {
            ((self.identity.manufacturer_id & 0x3f) << 8) | (self.identity.device_type & 0xff)
        };
        let expected = Address::unique(
            address_device_type,
            self.identity.device_id,
            address.master(),
        );
        expected.is_ok_and(|expected| expected == address)
    }

    fn respond(&mut self, request: &Frame) -> Frame {
        let expanded = request.wire_command == 31 && request.payload.len() >= 2;
        let logical = if expanded {
            u16::from_be_bytes([request.payload[0], request.payload[1]])
        } else {
            u16::from(request.wire_command)
        };
        let mut data = match logical {
            0 => self.identity_payload(),
            1 => {
                let mut data = vec![self.primary_unit];
                data.extend_from_slice(&self.primary_value.to_be_bytes());
                data
            }
            2 => {
                let mut data = self.loop_current.to_be_bytes().to_vec();
                data.extend_from_slice(&self.percent.to_be_bytes());
                data
            }
            48 => self.additional_status.clone(),
            _ => Vec::new(),
        };
        let response_code = if matches!(logical, 0 | 1 | 2 | 48) {
            0
        } else {
            64
        };
        let mut payload = vec![response_code, self.status];
        if expanded {
            payload.extend_from_slice(&logical.to_be_bytes());
        }
        payload.append(&mut data);
        Frame {
            preambles: self.identity.response_preambles.max(2),
            kind: FrameKind::Response,
            physical_layer: request.physical_layer,
            address: request.address,
            expansion: request.expansion.clone(),
            wire_command: request.wire_command,
            payload,
            repair: None,
        }
    }

    fn identity_payload(&self) -> Vec<u8> {
        let device_type = self.identity.device_type.to_be_bytes();
        let mut payload = vec![
            0xfe,
            device_type[0],
            device_type[1],
            self.identity.request_preambles,
            self.identity.universal_revision,
            self.identity.device_revision,
            self.identity.software_revision,
            ((self.identity.hardware_revision & 0x1f) << 3)
                | (self.identity.physical_signaling & 0x07),
            self.identity.flags,
        ];
        payload.extend_from_slice(&self.identity.device_id.to_be_bytes()[1..]);
        if self.identity.universal_revision >= 6 {
            payload.push(self.identity.response_preambles);
            payload.push(self.identity.maximum_device_variables);
            payload.extend_from_slice(&self.identity.configuration_change_counter.to_be_bytes());
            payload.push(self.identity.extended_status);
        }
        if self.identity.universal_revision >= 7 {
            payload.extend_from_slice(&self.identity.manufacturer_id.to_be_bytes());
            payload.extend_from_slice(&self.identity.private_label_distributor.to_be_bytes());
            payload.push(self.identity.device_profile);
        }
        payload
    }
}
