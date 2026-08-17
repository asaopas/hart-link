use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    num::NonZeroU16,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
    },
};

use thiserror::Error;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError},
    time::{Instant, timeout_at},
};

/// Maximum capacity of one request queue.
pub const MAXIMUM_QUEUE_CAPACITY: usize = 1_048_576;
/// Maximum total capacity reserved by all queues of one link.
pub const MAXIMUM_TOTAL_QUEUE_CAPACITY: usize = 1_048_576;
/// Maximum number of independently scheduled queues on one physical link.
pub const MAXIMUM_LINK_QUEUES: usize = 256;

/// Stable application-defined identifier of one queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueId(u16);

impl QueueId {
    /// Identifier used by the default one-queue configuration.
    pub const DEFAULT: Self = Self(0);

    /// Creates an identifier without allocating or registering global names.
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the numeric representation.
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl From<u16> for QueueId {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for QueueId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Scheduling policy of one queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePriority {
    /// Starvation-free weighted rotation among all ready weighted queues.
    Weighted(NonZeroU16),
    /// Absolute priority over every weighted queue. Larger ranks are selected first.
    ///
    /// Strict queues of equal rank rotate fairly. A continuously ready strict queue can
    /// intentionally starve weighted queues, so this mode must be selected explicitly.
    Strict(u16),
}

impl QueuePriority {
    /// Creates a starvation-free weighted priority.
    pub const fn weighted(weight: u16) -> Result<Self, QueueConfigError> {
        match NonZeroU16::new(weight) {
            Some(weight) => Ok(Self::Weighted(weight)),
            None => Err(QueueConfigError::ZeroWeight),
        }
    }

    /// Creates an absolute priority. Larger ranks win over smaller strict ranks.
    pub const fn strict(rank: u16) -> Self {
        Self::Strict(rank)
    }

    /// Returns the weighted quota, or `None` for an absolute priority.
    pub const fn weight(self) -> Option<u16> {
        match self {
            Self::Weighted(weight) => Some(weight.get()),
            Self::Strict(_) => None,
        }
    }

    /// Returns the strict rank, or `None` for a weighted priority.
    pub const fn strict_rank(self) -> Option<u16> {
        match self {
            Self::Weighted(_) => None,
            Self::Strict(rank) => Some(rank),
        }
    }

    const fn encode(self) -> u32 {
        match self {
            Self::Weighted(weight) => weight.get() as u32,
            Self::Strict(rank) => (1_u32 << 31) | rank as u32,
        }
    }

    const fn decode(value: u32) -> Self {
        let bytes = value.to_le_bytes();
        let lower = u16::from_le_bytes([bytes[0], bytes[1]]);
        if value & (1_u32 << 31) != 0 {
            Self::Strict(lower)
        } else {
            let weight = match NonZeroU16::new(lower) {
                Some(weight) => weight,
                None => NonZeroU16::MIN,
            };
            Self::Weighted(weight)
        }
    }
}

impl Default for QueuePriority {
    fn default() -> Self {
        Self::Weighted(NonZeroU16::MIN)
    }
}

/// Validated configuration of one independently bounded queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueConfig {
    id: QueueId,
    capacity: usize,
    priority: QueuePriority,
}

impl QueueConfig {
    /// Creates a queue participating in starvation-free weighted scheduling.
    pub fn weighted(
        id: impl Into<QueueId>,
        capacity: usize,
        weight: u16,
    ) -> Result<Self, QueueConfigError> {
        Self::new(id.into(), capacity, QueuePriority::weighted(weight)?)
    }

    /// Creates a queue with absolute priority over weighted queues.
    pub fn strict(
        id: impl Into<QueueId>,
        capacity: usize,
        rank: u16,
    ) -> Result<Self, QueueConfigError> {
        Self::new(id.into(), capacity, QueuePriority::strict(rank))
    }

    /// Creates a queue with an explicit scheduling policy.
    pub fn new(
        id: QueueId,
        capacity: usize,
        priority: QueuePriority,
    ) -> Result<Self, QueueConfigError> {
        if capacity == 0 {
            return Err(QueueConfigError::ZeroCapacity(id));
        }
        if capacity > MAXIMUM_QUEUE_CAPACITY {
            return Err(QueueConfigError::CapacityLimit {
                queue: id,
                capacity,
            });
        }
        Ok(Self {
            id,
            capacity,
            priority,
        })
    }

    /// Returns the queue identifier.
    pub const fn id(self) -> QueueId {
        self.id
    }

    /// Returns the independently enforced capacity.
    pub const fn capacity(self) -> usize {
        self.capacity
    }

    /// Returns the initial scheduling policy.
    pub const fn priority(self) -> QueuePriority {
        self.priority
    }
}

/// Invalid queue layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QueueConfigError {
    /// Every queue must admit at least one request.
    #[error("queue {0} capacity must be greater than zero")]
    ZeroCapacity(QueueId),
    /// One queue is too large for the runtime safety bound.
    #[error("queue {queue} capacity {capacity} exceeds the supported limit")]
    CapacityLimit {
        /// Invalid queue.
        queue: QueueId,
        /// Requested capacity.
        capacity: usize,
    },
    /// A zero weighted quota would prevent progress.
    #[error("weighted queue priority must be greater than zero")]
    ZeroWeight,
    /// The same identifier was configured more than once.
    #[error("queue id {0} is configured more than once")]
    DuplicateId(QueueId),
    /// A link must contain at least one queue.
    #[error("a link requires at least one queue")]
    Empty,
    /// One link contains too many scheduler classes.
    #[error("a link supports at most {MAXIMUM_LINK_QUEUES} queues")]
    TooManyQueues,
    /// Aggregate bounded capacity is excessive.
    #[error("total queue capacity {0} exceeds the supported limit")]
    TotalCapacity(usize),
    /// The selected default queue does not exist in the layout.
    #[error("default queue {0} is not configured")]
    MissingDefault(QueueId),
}

/// Runtime state of one configured queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct QueueSnapshot {
    /// Queue identifier.
    pub id: QueueId,
    /// Fixed queue capacity.
    pub capacity: usize,
    /// Requests currently occupying this queue.
    pub queued: usize,
    /// Current scheduling policy.
    pub priority: QueuePriority,
}

/// Error from selecting or reconfiguring one queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QueueError {
    /// The identifier is not part of this link.
    #[error("queue {0} is not configured on this link")]
    Unknown(QueueId),
    /// A zero weighted quota would prevent progress.
    #[error("weighted queue priority must be greater than zero")]
    ZeroWeight,
}

pub(crate) fn validate_layout(
    definitions: &[QueueConfig],
    default: QueueId,
) -> Result<(), QueueConfigError> {
    if definitions.is_empty() {
        return Err(QueueConfigError::Empty);
    }
    if definitions.len() > MAXIMUM_LINK_QUEUES {
        return Err(QueueConfigError::TooManyQueues);
    }
    let mut ids = BTreeSet::new();
    let mut total = 0_usize;
    for definition in definitions {
        if !ids.insert(definition.id()) {
            return Err(QueueConfigError::DuplicateId(definition.id()));
        }
        total = total
            .checked_add(definition.capacity())
            .ok_or(QueueConfigError::TotalCapacity(usize::MAX))?;
        if total > MAXIMUM_TOTAL_QUEUE_CAPACITY {
            return Err(QueueConfigError::TotalCapacity(total));
        }
    }
    if !ids.contains(&default) {
        return Err(QueueConfigError::MissingDefault(default));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueuePushError {
    Full,
    Closed,
    Deadline,
    Unknown(QueueId),
}

pub(crate) struct QueueEntry<T> {
    item: T,
    _permit: OwnedSemaphorePermit,
}

impl<T> QueueEntry<T> {
    pub(crate) const fn item(&self) -> &T {
        &self.item
    }

    fn into_item(self) -> T {
        self.item
    }
}

struct QueueSlot<T> {
    id: QueueId,
    capacity: usize,
    priority: AtomicU32,
    permits: Arc<Semaphore>,
    queued: AtomicUsize,
    entries: Mutex<VecDeque<QueueEntry<T>>>,
}

impl<T> QueueSlot<T> {
    fn new(config: QueueConfig) -> Self {
        Self {
            id: config.id(),
            capacity: config.capacity(),
            priority: AtomicU32::new(config.priority().encode()),
            permits: Arc::new(Semaphore::new(config.capacity())),
            queued: AtomicUsize::new(0),
            entries: Mutex::new(VecDeque::new()),
        }
    }

    fn entries(&self) -> MutexGuard<'_, VecDeque<QueueEntry<T>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn priority(&self) -> QueuePriority {
        QueuePriority::decode(self.priority.load(Ordering::Acquire))
    }

    fn pop_front(&self) -> Option<QueueEntry<T>> {
        self.entries().pop_front()
    }

    fn push_front(&self, entry: QueueEntry<T>) {
        self.entries().push_front(entry);
    }

    fn close(&self) {
        self.permits.close();
    }

    fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            id: self.id,
            capacity: self.capacity,
            queued: self.queued.load(Ordering::Acquire),
            priority: self.priority(),
        }
    }
}

pub(crate) struct QueueCursor {
    weighted_index: usize,
    weighted_used: u16,
    strict_index: usize,
}

impl QueueCursor {
    pub(crate) const fn new() -> Self {
        Self {
            weighted_index: 0,
            weighted_used: 0,
            strict_index: 0,
        }
    }
}

pub(crate) struct QueueSet<T> {
    slots: Box<[QueueSlot<T>]>,
    indices: BTreeMap<QueueId, usize>,
    notify: Notify,
    input_closed: AtomicBool,
    runner_closed: AtomicBool,
}

impl<T> QueueSet<T> {
    pub(crate) fn new(definitions: &[QueueConfig]) -> Self {
        let slots = definitions
            .iter()
            .copied()
            .map(QueueSlot::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let indices = slots
            .iter()
            .enumerate()
            .map(|(index, slot)| (slot.id, index))
            .collect();
        Self {
            slots,
            indices,
            notify: Notify::new(),
            input_closed: AtomicBool::new(false),
            runner_closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn contains(&self, id: QueueId) -> bool {
        self.indices.contains_key(&id)
    }

    pub(crate) fn priority(&self, id: QueueId) -> Result<QueuePriority, QueueError> {
        Ok(self.slot(id)?.priority())
    }

    pub(crate) fn set_priority(
        &self,
        id: QueueId,
        priority: QueuePriority,
    ) -> Result<(), QueueError> {
        self.slot(id)?
            .priority
            .store(priority.encode(), Ordering::Release);
        self.notify.notify_one();
        Ok(())
    }

    pub(crate) fn set_weight(&self, id: QueueId, weight: u16) -> Result<(), QueueError> {
        let priority = QueuePriority::weighted(weight).map_err(|_| QueueError::ZeroWeight)?;
        self.set_priority(id, priority)
    }

    pub(crate) fn snapshot(&self, id: QueueId) -> Result<QueueSnapshot, QueueError> {
        Ok(self.slot(id)?.snapshot())
    }

    pub(crate) fn snapshots(&self) -> Vec<QueueSnapshot> {
        self.slots.iter().map(QueueSlot::snapshot).collect()
    }

    pub(crate) fn pending_total(&self) -> usize {
        self.slots
            .iter()
            .map(|slot| slot.queued.load(Ordering::Acquire))
            .sum()
    }

    pub(crate) async fn push(
        &self,
        id: QueueId,
        item: T,
        deadline: Instant,
    ) -> Result<(), QueuePushError> {
        let slot = self.slot(id).map_err(|_| QueuePushError::Unknown(id))?;
        if self.runner_closed.load(Ordering::Acquire) {
            return Err(QueuePushError::Closed);
        }
        let permit = timeout_at(deadline, Arc::clone(&slot.permits).acquire_owned())
            .await
            .map_err(|_| QueuePushError::Deadline)?
            .map_err(|_| QueuePushError::Closed)?;
        self.finish_push(slot, item, permit)
    }

    pub(crate) fn try_push(&self, id: QueueId, item: T) -> Result<(), QueuePushError> {
        let slot = self.slot(id).map_err(|_| QueuePushError::Unknown(id))?;
        if self.runner_closed.load(Ordering::Acquire) {
            return Err(QueuePushError::Closed);
        }
        let permit =
            Arc::clone(&slot.permits)
                .try_acquire_owned()
                .map_err(|error| match error {
                    TryAcquireError::NoPermits => QueuePushError::Full,
                    TryAcquireError::Closed => QueuePushError::Closed,
                })?;
        self.finish_push(slot, item, permit)
    }

    fn finish_push(
        &self,
        slot: &QueueSlot<T>,
        item: T,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), QueuePushError> {
        if self.runner_closed.load(Ordering::Acquire) {
            return Err(QueuePushError::Closed);
        }
        slot.entries().push_back(QueueEntry {
            item,
            _permit: permit,
        });
        slot.queued.fetch_add(1, Ordering::Release);
        self.notify.notify_one();
        Ok(())
    }

    pub(crate) fn pop_scheduled(
        &self,
        cursor: &mut QueueCursor,
    ) -> Option<(QueueId, QueueEntry<T>)> {
        self.pop_strict(cursor)
            .or_else(|| self.pop_weighted(cursor))
    }

    fn pop_strict(&self, cursor: &mut QueueCursor) -> Option<(QueueId, QueueEntry<T>)> {
        let rank = self
            .slots
            .iter()
            .filter_map(|slot| match slot.priority() {
                QueuePriority::Strict(rank) if slot.queued.load(Ordering::Acquire) > 0 => {
                    Some(rank)
                }
                QueuePriority::Weighted(_) | QueuePriority::Strict(_) => None,
            })
            .max()?;
        for offset in 1..=self.slots.len() {
            let index = (cursor.strict_index + offset) % self.slots.len();
            let slot = &self.slots[index];
            if slot.priority() == QueuePriority::Strict(rank)
                && let Some(entry) = slot.pop_front()
            {
                cursor.strict_index = index;
                return Some((slot.id, entry));
            }
        }
        None
    }

    fn pop_weighted(&self, cursor: &mut QueueCursor) -> Option<(QueueId, QueueEntry<T>)> {
        if self.slots.is_empty() {
            return None;
        }
        if cursor.weighted_index >= self.slots.len() {
            cursor.weighted_index = 0;
            cursor.weighted_used = 0;
        }
        let current = &self.slots[cursor.weighted_index];
        if let QueuePriority::Weighted(weight) = current.priority()
            && cursor.weighted_used < weight.get()
            && let Some(entry) = current.pop_front()
        {
            cursor.weighted_used = cursor.weighted_used.saturating_add(1);
            return Some((current.id, entry));
        }

        for offset in 1..self.slots.len() {
            let index = (cursor.weighted_index + offset) % self.slots.len();
            let slot = &self.slots[index];
            if matches!(slot.priority(), QueuePriority::Weighted(_))
                && let Some(entry) = slot.pop_front()
            {
                cursor.weighted_index = index;
                cursor.weighted_used = 1;
                return Some((slot.id, entry));
            }
        }

        if matches!(current.priority(), QueuePriority::Weighted(_))
            && let Some(entry) = current.pop_front()
        {
            cursor.weighted_used = current.priority().weight().unwrap_or(1);
            return Some((current.id, entry));
        }
        None
    }

    pub(crate) fn pop_from(&self, id: QueueId) -> Option<QueueEntry<T>> {
        self.slot(id).ok()?.pop_front()
    }

    pub(crate) fn push_front(&self, id: QueueId, entry: QueueEntry<T>) {
        if let Ok(slot) = self.slot(id) {
            slot.push_front(entry);
            self.notify.notify_one();
        }
    }

    pub(crate) fn finish_entry(&self, id: QueueId, entry: QueueEntry<T>) -> T {
        let item = entry.into_item();
        if let Ok(slot) = self.slot(id) {
            slot.queued.fetch_sub(1, Ordering::Release);
        }
        item
    }

    pub(crate) fn input_closed(&self) -> bool {
        self.input_closed.load(Ordering::Acquire)
    }

    pub(crate) fn close_input(&self) {
        self.input_closed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    pub(crate) fn close_runner(&self) {
        if !self.runner_closed.swap(true, Ordering::AcqRel) {
            for slot in &self.slots {
                slot.close();
            }
            self.notify.notify_waiters();
        }
    }

    pub(crate) fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    fn slot(&self, id: QueueId) -> Result<&QueueSlot<T>, QueueError> {
        self.indices
            .get(&id)
            .map(|index| &self.slots[*index])
            .ok_or(QueueError::Unknown(id))
    }
}
