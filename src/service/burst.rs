use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use thiserror::Error;
use tokio::sync::mpsc;

use crate::{Address, CommandCode, DeviceReply};

/// Largest per-subscriber backlog accepted without explicit application buffering.
pub const MAXIMUM_BURST_SUBSCRIPTION_CAPACITY: usize = 65_536;
/// Largest number of active subscriptions retained by one hub.
pub const MAXIMUM_BURST_SUBSCRIPTIONS: usize = 65_536;
/// Largest number of deduplication fingerprints retained by one hub.
pub const MAXIMUM_BURST_TRACKED_KEYS: usize = 65_536;
/// Largest aggregate number of reserved delivery slots across one hub.
pub const MAXIMUM_BURST_BUFFERED_DELIVERIES: usize = 1_048_576;

/// Routing key for an unsolicited message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BurstKey {
    /// Source address.
    pub address: Address,
    /// Command of the published value.
    pub command: CommandCode,
}

impl BurstKey {
    /// Creates an address-command routing key.
    pub const fn new(address: Address, command: CommandCode) -> Self {
        Self { address, command }
    }
}

/// Message together with its host receive time.
#[derive(Debug, Clone)]
pub struct BurstMessage {
    /// Time at which the manager received the message.
    pub received_at: SystemTime,
    /// Validated device response.
    pub reply: DeviceReply,
}

/// Aggregate delivery counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BurstSnapshot {
    /// Number of active subscriptions.
    pub subscriptions: usize,
    /// Aggregate delivery slots reserved across active subscriptions.
    pub reserved_delivery_slots: usize,
    /// Number of accepted messages.
    pub received: u64,
    /// Number of exact duplicates discarded.
    pub duplicates: u64,
    /// Number of deliveries dropped because a receiver was slow.
    pub backpressure_drops: u64,
    /// Number of source-command fingerprints currently retained for deduplication.
    pub tracked_keys: usize,
    /// Number of old fingerprints evicted to keep memory bounded.
    pub fingerprint_evictions: u64,
    /// Forged messages rejected before allocating deduplication state.
    pub invalid_messages: u64,
}

/// Memory and duplicate-suppression limits for a [`BurstHub`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BurstConfig {
    /// Maximum number of active subscriptions retained by this hub.
    pub maximum_subscriptions: usize,
    /// Maximum aggregate delivery slots across every active subscription.
    pub maximum_buffered_deliveries: usize,
    /// Maximum number of address-command fingerprints retained at once.
    pub maximum_tracked_keys: usize,
    /// Interval during which an unchanged publication is considered a duplicate.
    ///
    /// A zero duration disables duplicate suppression while retaining bounded routing state.
    pub duplicate_window: Duration,
}

impl Default for BurstConfig {
    fn default() -> Self {
        Self {
            maximum_subscriptions: 4096,
            maximum_buffered_deliveries: 65_536,
            maximum_tracked_keys: 1024,
            duplicate_window: Duration::from_millis(250),
        }
    }
}

impl BurstConfig {
    /// Sets the active subscription limit.
    pub const fn with_maximum_subscriptions(mut self, maximum_subscriptions: usize) -> Self {
        self.maximum_subscriptions = maximum_subscriptions;
        self
    }

    /// Sets the aggregate delivery backlog limit.
    pub const fn with_maximum_buffered_deliveries(
        mut self,
        maximum_buffered_deliveries: usize,
    ) -> Self {
        self.maximum_buffered_deliveries = maximum_buffered_deliveries;
        self
    }

    /// Sets the deduplication fingerprint limit.
    pub const fn with_maximum_tracked_keys(mut self, maximum_tracked_keys: usize) -> Self {
        self.maximum_tracked_keys = maximum_tracked_keys;
        self
    }

    /// Sets the exact-payload duplicate suppression window.
    pub const fn with_duplicate_window(mut self, duplicate_window: Duration) -> Self {
        self.duplicate_window = duplicate_window;
        self
    }
}

/// Invalid [`BurstHub`] configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BurstConfigError {
    /// At least one subscription slot is required.
    #[error("maximum_subscriptions must be greater than zero")]
    NoSubscriptions,
    /// The configured active subscription count is implausibly large.
    #[error("maximum_subscriptions {0} exceeds the supported limit")]
    SubscriptionLimit(usize),
    /// At least one aggregate delivery slot is required.
    #[error("maximum_buffered_deliveries must be greater than zero")]
    NoBufferedDeliveries,
    /// The configured aggregate delivery backlog is implausibly large.
    #[error("maximum_buffered_deliveries {0} exceeds the supported limit")]
    BufferedDeliveryLimit(usize),
    /// At least one fingerprint slot is required.
    #[error("maximum_tracked_keys must be greater than zero")]
    NoTrackedKeys,
    /// The configured fingerprint count is implausibly large.
    #[error("maximum_tracked_keys {0} exceeds the supported limit")]
    TrackedKeyLimit(usize),
}

/// Invalid input supplied directly to [`BurstHub::try_publish`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BurstPublishError {
    /// A response data field cannot exceed the frame payload space.
    #[error("Burst response data is too large: {0} bytes")]
    DataLength(usize),
    /// Link-layer header expansion is limited to three bytes.
    #[error("Burst frame expansion is too large: {0} bytes")]
    ExpansionLength(usize),
}

/// Invalid per-subscriber buffering settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum BurstSubscriptionError {
    /// A subscription must retain at least one message.
    #[error("Burst subscription capacity must be greater than zero")]
    ZeroCapacity,
    /// A subscription backlog is too large.
    #[error("Burst subscription capacity {0} exceeds the supported limit")]
    CapacityLimit(usize),
    /// The hub reached its configured active subscription limit.
    #[error("Burst hub reached its {0}-subscription limit")]
    SubscriptionLimit(usize),
    /// The internal subscription identifier space was exhausted.
    #[error("Burst subscription identifier space exhausted")]
    IdentifierSpace,
    /// A new subscription would exceed the aggregate delivery backlog.
    #[error("Burst hub reached its {0}-slot aggregate delivery limit")]
    AggregateCapacity(usize),
}

#[derive(Debug)]
struct SubscriptionEntry {
    key: BurstKey,
    sender: mpsc::Sender<BurstMessage>,
    capacity: usize,
}

#[derive(Debug)]
struct Fingerprint {
    bytes: Vec<u8>,
    last_delivered_at: Instant,
}

#[derive(Debug)]
struct BurstState {
    next_id: u64,
    subscriptions: BTreeMap<u64, SubscriptionEntry>,
    fingerprints: BTreeMap<BurstKey, Fingerprint>,
    fingerprint_order: VecDeque<BurstKey>,
    config: BurstConfig,
    snapshot: BurstSnapshot,
}

/// Bounded subscription to one address and command.
#[derive(Debug)]
pub struct BurstSubscription {
    id: u64,
    receiver: mpsc::Receiver<BurstMessage>,
    state: Arc<Mutex<BurstState>>,
}

impl BurstSubscription {
    /// Receives the next message.
    pub async fn receive(&mut self) -> Option<BurstMessage> {
        self.receiver.recv().await
    }

    /// Attempts to receive immediately without waiting.
    pub fn try_receive(&mut self) -> Result<BurstMessage, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

impl Drop for BurstSubscription {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(entry) = state.subscriptions.remove(&self.id) {
                state.snapshot.reserved_delivery_slots = state
                    .snapshot
                    .reserved_delivery_slots
                    .saturating_sub(entry.capacity);
            }
            state.snapshot.subscriptions = state.subscriptions.len();
        }
    }
}

/// Unsolicited-message router with deduplication and backpressure.
#[derive(Debug, Clone)]
pub struct BurstHub {
    state: Arc<Mutex<BurstState>>,
}

impl Default for BurstHub {
    fn default() -> Self {
        Self::from_valid_config(BurstConfig::default())
    }
}

impl BurstHub {
    /// Creates a router with explicit bounded deduplication settings.
    pub fn new(config: BurstConfig) -> Result<Self, BurstConfigError> {
        if config.maximum_subscriptions == 0 {
            return Err(BurstConfigError::NoSubscriptions);
        }
        if config.maximum_subscriptions > MAXIMUM_BURST_SUBSCRIPTIONS {
            return Err(BurstConfigError::SubscriptionLimit(
                config.maximum_subscriptions,
            ));
        }
        if config.maximum_buffered_deliveries == 0 {
            return Err(BurstConfigError::NoBufferedDeliveries);
        }
        if config.maximum_buffered_deliveries > MAXIMUM_BURST_BUFFERED_DELIVERIES {
            return Err(BurstConfigError::BufferedDeliveryLimit(
                config.maximum_buffered_deliveries,
            ));
        }
        if config.maximum_tracked_keys == 0 {
            return Err(BurstConfigError::NoTrackedKeys);
        }
        if config.maximum_tracked_keys > MAXIMUM_BURST_TRACKED_KEYS {
            return Err(BurstConfigError::TrackedKeyLimit(
                config.maximum_tracked_keys,
            ));
        }
        Ok(Self::from_valid_config(config))
    }

    fn from_valid_config(config: BurstConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(BurstState {
                next_id: 0,
                subscriptions: BTreeMap::new(),
                fingerprints: BTreeMap::new(),
                fingerprint_order: VecDeque::new(),
                config,
                snapshot: BurstSnapshot::default(),
            })),
        }
    }

    /// Creates a subscription with bounded capacity.
    pub fn subscribe(
        &self,
        key: BurstKey,
        capacity: usize,
    ) -> Result<BurstSubscription, BurstSubscriptionError> {
        self.try_subscribe(key, capacity)
    }

    /// Creates a subscription with a small default backlog.
    pub fn subscribe_default(
        &self,
        key: BurstKey,
    ) -> Result<BurstSubscription, BurstSubscriptionError> {
        self.try_subscribe(key, 64)
    }

    /// Creates a subscription after rejecting zero or implausibly large backlogs.
    pub fn try_subscribe(
        &self,
        key: BurstKey,
        capacity: usize,
    ) -> Result<BurstSubscription, BurstSubscriptionError> {
        if capacity == 0 {
            return Err(BurstSubscriptionError::ZeroCapacity);
        }
        if capacity > MAXIMUM_BURST_SUBSCRIPTION_CAPACITY {
            return Err(BurstSubscriptionError::CapacityLimit(capacity));
        }
        self.subscribe_validated(key, capacity)
    }

    fn subscribe_validated(
        &self,
        key: BurstKey,
        capacity: usize,
    ) -> Result<BurstSubscription, BurstSubscriptionError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.subscriptions.len() >= state.config.maximum_subscriptions {
            return Err(BurstSubscriptionError::SubscriptionLimit(
                state.config.maximum_subscriptions,
            ));
        }
        let aggregate_capacity = state
            .snapshot
            .reserved_delivery_slots
            .checked_add(capacity)
            .ok_or(BurstSubscriptionError::AggregateCapacity(
                state.config.maximum_buffered_deliveries,
            ))?;
        if aggregate_capacity > state.config.maximum_buffered_deliveries {
            return Err(BurstSubscriptionError::AggregateCapacity(
                state.config.maximum_buffered_deliveries,
            ));
        }
        state.next_id = state
            .next_id
            .checked_add(1)
            .ok_or(BurstSubscriptionError::IdentifierSpace)?;
        let id = state.next_id;
        let (sender, receiver) = mpsc::channel(capacity);
        state.subscriptions.insert(
            id,
            SubscriptionEntry {
                key,
                sender,
                capacity,
            },
        );
        state.snapshot.subscriptions = state.subscriptions.len();
        state.snapshot.reserved_delivery_slots = aggregate_capacity;
        Ok(BurstSubscription {
            id,
            receiver,
            state: self.state.clone(),
        })
    }

    /// Publishes a validated message, silently rejecting forged oversized values.
    pub fn publish(&self, reply: DeviceReply) {
        let _ = self.try_publish(reply);
    }

    /// Publishes a message and reports malformed values constructed outside the wire decoder.
    pub fn try_publish(&self, reply: DeviceReply) -> Result<(), BurstPublishError> {
        if reply.data.len() > usize::from(u8::MAX) {
            self.mark_invalid();
            return Err(BurstPublishError::DataLength(reply.data.len()));
        }
        if reply.frame_expansion.len() > 3 {
            self.mark_invalid();
            return Err(BurstPublishError::ExpansionLength(
                reply.frame_expansion.len(),
            ));
        }
        let key = BurstKey {
            address: reply.address,
            command: reply.command,
        };
        let mut fingerprint = vec![reply.response_code, reply.device_status];
        fingerprint.extend_from_slice(&reply.data);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.snapshot.received = state.snapshot.received.saturating_add(1);
        let now = Instant::now();
        let is_duplicate = state.fingerprints.get(&key).is_some_and(|previous| {
            !state.config.duplicate_window.is_zero()
                && previous.bytes == fingerprint
                && now.duration_since(previous.last_delivered_at) < state.config.duplicate_window
        });
        if is_duplicate {
            state.snapshot.duplicates = state.snapshot.duplicates.saturating_add(1);
            return Ok(());
        }
        if state.fingerprints.contains_key(&key) {
            state
                .fingerprint_order
                .retain(|candidate| *candidate != key);
        } else if state.fingerprints.len() == state.config.maximum_tracked_keys
            && let Some(oldest) = state.fingerprint_order.pop_front()
        {
            state.fingerprints.remove(&oldest);
            state.snapshot.fingerprint_evictions =
                state.snapshot.fingerprint_evictions.saturating_add(1);
        }
        state.fingerprints.insert(
            key,
            Fingerprint {
                bytes: fingerprint,
                last_delivered_at: now,
            },
        );
        state.fingerprint_order.push_back(key);
        state.snapshot.tracked_keys = state.fingerprints.len();
        let message = BurstMessage {
            received_at: SystemTime::now(),
            reply,
        };
        let mut closed = Vec::new();
        let mut dropped = 0u64;
        for (id, subscription) in &state.subscriptions {
            if subscription.key != key {
                continue;
            }
            match subscription.sender.try_send(message.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => dropped = dropped.saturating_add(1),
                Err(mpsc::error::TrySendError::Closed(_)) => closed.push(*id),
            }
        }
        state.snapshot.backpressure_drops =
            state.snapshot.backpressure_drops.saturating_add(dropped);
        for id in closed {
            if let Some(entry) = state.subscriptions.remove(&id) {
                state.snapshot.reserved_delivery_slots = state
                    .snapshot
                    .reserved_delivery_slots
                    .saturating_sub(entry.capacity);
            }
        }
        state.snapshot.subscriptions = state.subscriptions.len();
        Ok(())
    }

    /// Returns the current summary.
    pub fn snapshot(&self) -> BurstSnapshot {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
    }

    /// Removes the remembered payload for one key so its next message is always delivered.
    pub fn reset_key(&self, key: BurstKey) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.fingerprints.remove(&key);
        state
            .fingerprint_order
            .retain(|candidate| *candidate != key);
        state.snapshot.tracked_keys = state.fingerprints.len();
    }

    /// Removes every remembered payload without affecting subscriptions or counters.
    pub fn reset_all(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.fingerprints.clear();
        state.fingerprint_order.clear();
        state.snapshot.tracked_keys = 0;
    }

    fn mark_invalid(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.snapshot.invalid_messages = state.snapshot.invalid_messages.saturating_add(1);
    }
}
