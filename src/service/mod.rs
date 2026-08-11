//! Command execution through a bounded priority queue.

mod burst;
mod discovery;
mod health;
mod link;
mod planner;
mod recovery;
mod session;
mod snapshot;
mod transfer;

pub use burst::{
    BurstConfig, BurstConfigError, BurstHub, BurstKey, BurstMessage, BurstPublishError,
    BurstSnapshot, BurstSubscription, BurstSubscriptionError, MAXIMUM_BURST_BUFFERED_DELIVERIES,
    MAXIMUM_BURST_SUBSCRIPTION_CAPACITY, MAXIMUM_BURST_SUBSCRIPTIONS, MAXIMUM_BURST_TRACKED_KEYS,
};
pub use discovery::{
    DiscoveredDevice, DiscoveryError, DiscoveryHintError, DiscoveryHints, DiscoveryOptions,
    DiscoveryReport, MAXIMUM_DISCOVERY_HINTS, discover_line, discover_line_with_options,
};
pub use health::{
    AdaptiveTiming, DeviceHealthConfigError, DeviceHealthOptions, DeviceHealthSnapshot,
    ManagedDeviceSession, ManagedSessionError,
};
pub use link::{
    CommandContext, CommandPolicy, CommandRouting, DEFAULT_LATE_RESPONSE_GUARD, ExchangeError,
    LinkBuildError, LinkBuilder, LinkClient, LinkConfig, LinkConfigError, LinkEvent, LinkRunner,
    LinkSnapshot, MAXIMUM_LATE_RESPONSE_GUARD, MAXIMUM_LINK_DECODER_BUFFER,
    MAXIMUM_LINK_EVENT_CAPACITY, MAXIMUM_LINK_QUEUE_CAPACITY, MAXIMUM_LINK_READ_BUFFER,
    MAXIMUM_RETRY_DURATION, MAXIMUM_TRANSMIT_PREFIX, PendingReply, Priority, PriorityClient,
    QueueMode, QueueScheduling, QueueSchedulingError, RetryCause, RetryPolicy, RetryPolicyError,
    StartError, create_link, try_create_link,
};
pub use planner::{Plan, PlanExpectation, PlanReport, PlanStep, StepReport};
pub use recovery::{KnownDevice, RecoveryReport, RecoveryStatus, reconcile_devices};
pub use session::{DeviceSession, SessionError, SessionProfile};
pub use snapshot::{DeviceSnapshot, SnapshotError, SnapshotField, SnapshotOptions};
pub use transfer::{
    BlockReceiver, BlockSender, MAXIMUM_TRANSFER_BYTES, TransferBlock, TransferError,
    TransferProgress,
};
