use std::{
    collections::BTreeSet,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio::{
    sync::{broadcast, oneshot},
    time::{Instant, timeout_at},
};

use crate::{
    catalog::{OperationSafety, command_descriptor},
    channel::{ByteChannel, ChannelError},
    operation::{CommandCode, CommandOutcome, DeviceReply, Operation, OperationError, Request},
    service::{
        BurstHub, QueueConfig, QueueConfigError, QueueError, QueueId, QueuePriority, QueueSnapshot,
        queue_set::{QueueCursor, QueueEntry, QueuePushError, QueueSet, validate_layout},
    },
    wire::{
        ChecksumPolicy, DecodeEvent, DecodeLimits, Frame, FrameDecoder, FrameKind,
        MAX_ENCODED_FRAME_SIZE, MAXIMUM_DECODE_BUFFER_CAPACITY, WireError,
    },
};

/// Largest transport-read allocation accepted by validated construction.
pub const MAXIMUM_LINK_READ_BUFFER: usize = 1_048_576;
/// Largest retained streaming-decoder tail accepted by validated construction.
pub const MAXIMUM_LINK_DECODER_BUFFER: usize = MAXIMUM_DECODE_BUFFER_CAPACITY;
/// Largest observer event channel accepted by [`LinkBuilder`].
pub const MAXIMUM_LINK_EVENT_CAPACITY: usize = 1_048_576;
/// Largest modem or gateway wake sequence accepted by the queue API.
pub const MAXIMUM_TRANSMIT_PREFIX: usize = 4096;
/// Default drain period after a response timeout on a protocol without transaction identifiers.
pub const DEFAULT_LATE_RESPONSE_GUARD: Duration = Duration::from_millis(25);
/// Largest accepted post-timeout quarantine interval.
pub const MAXIMUM_LATE_RESPONSE_GUARD: Duration = Duration::from_mins(1);
/// Hard upper bound for any per-request timeout or retry delay.
pub const MAXIMUM_RETRY_DURATION: Duration = Duration::from_hours(24);

/// Immutable command information supplied to a custom admission policy.
#[derive(Debug, Clone, Copy)]
pub struct CommandContext<'a> {
    /// Complete request before it is accepted by a queue.
    pub request: &'a Request,
    /// Effective retry-safety classification, including an explicit request declaration.
    pub safety: OperationSafety,
    /// Safety from the built-in registry, or `None` for an unknown command.
    pub catalog_safety: Option<OperationSafety>,
    /// Queue selected after command routing.
    pub queue: QueueId,
}

type CommandGuard = dyn for<'a> Fn(CommandContext<'a>) -> bool + Send + Sync + 'static;
type CommandRouter = dyn for<'a> Fn(CommandContext<'a>) -> QueueId + Send + Sync + 'static;

/// Cloneable admission policy evaluated before a request occupies queue capacity.
#[derive(Clone)]
pub struct CommandPolicy {
    guard: Arc<CommandGuard>,
    name: &'static str,
}

impl fmt::Debug for CommandPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandPolicy")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Default for CommandPolicy {
    fn default() -> Self {
        Self::allow_all()
    }
}

impl CommandPolicy {
    /// Preserves the compatibility behavior and admits every valid request.
    pub fn allow_all() -> Self {
        Self::custom_named("allow-all", |_| true)
    }

    /// Admits only commands registered by the library as read-only.
    ///
    /// An explicit retry-safety override on [`Request`] cannot bypass this policy. Unknown
    /// vendor commands require a separate explicit allow-list or custom policy.
    pub fn read_only() -> Self {
        Self::custom_named("read-only", |context| {
            context.catalog_safety == Some(OperationSafety::ReadOnly)
        })
    }

    /// Admits commands registered as read-only or idempotent writes, but rejects actions and
    /// unknown commands regardless of their caller-declared retry safety.
    pub fn retry_safe() -> Self {
        Self::custom_named("retry-safe", |context| {
            context
                .catalog_safety
                .is_some_and(|safety| safety != OperationSafety::Action)
        })
    }

    /// Admits only the explicitly listed logical command numbers.
    pub fn allow_commands(commands: impl IntoIterator<Item = CommandCode>) -> Self {
        let commands = commands.into_iter().collect::<BTreeSet<_>>();
        Self::custom_named("allow-list", move |context| {
            commands.contains(&context.request.command)
        })
    }

    /// Rejects the explicitly listed logical command numbers and admits every other command.
    pub fn deny_commands(commands: impl IntoIterator<Item = CommandCode>) -> Self {
        let commands = commands.into_iter().collect::<BTreeSet<_>>();
        Self::custom_named("deny-list", move |context| {
            !commands.contains(&context.request.command)
        })
    }

    /// Restricts one effective queue to an allow-list and leaves every other queue open.
    ///
    /// Routing is evaluated first, so the policy observes the queue that will actually receive
    /// the request.
    pub fn queue_allowlist(
        queue: QueueId,
        commands: impl IntoIterator<Item = CommandCode>,
    ) -> Self {
        let commands = commands.into_iter().collect::<BTreeSet<_>>();
        Self::custom_named("queue-allow-list", move |context| {
            context.queue != queue || commands.contains(&context.request.command)
        })
    }

    /// Requires both admission policies to accept a request.
    pub fn and(self, other: Self) -> Self {
        Self::custom_named("and", move |context| {
            self.admits(context) && other.admits(context)
        })
    }

    /// Accepts a request when either admission policy accepts it.
    pub fn or(self, other: Self) -> Self {
        Self::custom_named("or", move |context| {
            self.admits(context) || other.admits(context)
        })
    }

    /// Creates an application-defined policy without exposing queue internals.
    pub fn custom(
        guard: impl for<'a> Fn(CommandContext<'a>) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self::custom_named("custom", guard)
    }

    fn custom_named(
        name: &'static str,
        guard: impl for<'a> Fn(CommandContext<'a>) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            guard: Arc::new(guard),
            name,
        }
    }

    fn admits(&self, context: CommandContext<'_>) -> bool {
        (self.guard)(context)
    }
}

/// Cloneable command-to-queue routing applied before admission checks.
#[derive(Clone)]
pub struct CommandRouting {
    router: Arc<CommandRouter>,
    name: &'static str,
}

impl fmt::Debug for CommandRouting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandRouting")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Default for CommandRouting {
    fn default() -> Self {
        Self::preserve_requested()
    }
}

impl CommandRouting {
    /// Preserves the queue supplied by the caller.
    pub fn preserve_requested() -> Self {
        Self::custom_named("preserve-requested", |context| context.queue)
    }

    /// Routes listed commands to one queue and leaves every other request in its selected queue.
    pub fn commands_to(queue: QueueId, commands: impl IntoIterator<Item = CommandCode>) -> Self {
        let commands = commands.into_iter().collect::<BTreeSet<_>>();
        Self::custom_named("command-list", move |context| {
            if commands.contains(&context.request.command) {
                queue
            } else {
                context.queue
            }
        })
    }

    /// Creates application-defined routing from immutable request information.
    pub fn custom(
        router: impl for<'a> Fn(CommandContext<'a>) -> QueueId + Send + Sync + 'static,
    ) -> Self {
        Self::custom_named("custom", router)
    }

    fn custom_named(
        name: &'static str,
        router: impl for<'a> Fn(CommandContext<'a>) -> QueueId + Send + Sync + 'static,
    ) -> Self {
        Self {
            router: Arc::new(router),
            name,
        }
    }

    fn route(&self, context: CommandContext<'_>) -> QueueId {
        (self.router)(context)
    }
}

/// Independent retry limits for one exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Retries after a complete request was sent but no response arrived.
    pub transport_retries: u8,
    /// Retries after a transient device rejection.
    pub busy_retries: u8,
    /// Polls after a delayed-response indication from the device.
    pub delayed_response_polls: u8,
    /// Delay before polling a transient device state again.
    pub retry_delay: Duration,
    /// Timeout for one response.
    pub response_timeout: Duration,
    /// End-to-end deadline from enqueue to result.
    pub total_timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            transport_retries: 1,
            busy_retries: 2,
            delayed_response_polls: 8,
            retry_delay: Duration::from_millis(50),
            response_timeout: Duration::from_millis(1600),
            total_timeout: Duration::from_secs(8),
        }
    }
}

impl RetryPolicy {
    /// Creates a policy with no automatic repeats or delayed-response polling.
    pub const fn single_attempt(response_timeout: Duration, total_timeout: Duration) -> Self {
        Self {
            transport_retries: 0,
            busy_retries: 0,
            delayed_response_polls: 0,
            retry_delay: Duration::ZERO,
            response_timeout,
            total_timeout,
        }
    }

    /// Verifies that both timeout layers can make forward progress.
    pub const fn validate(&self) -> Result<(), RetryPolicyError> {
        if self.response_timeout.is_zero() {
            return Err(RetryPolicyError::ResponseTimeout);
        }
        if self.total_timeout.is_zero() {
            return Err(RetryPolicyError::TotalTimeout);
        }
        Ok(())
    }

    /// Verifies that every configured duration fits the runtime clock.
    pub fn validate_runtime(&self) -> Result<(), RetryPolicyError> {
        self.validate()?;
        if self.response_timeout > MAXIMUM_RETRY_DURATION {
            return Err(RetryPolicyError::ResponseTimeoutLimit);
        }
        if self.total_timeout > MAXIMUM_RETRY_DURATION {
            return Err(RetryPolicyError::TotalTimeoutLimit);
        }
        if self.retry_delay > MAXIMUM_RETRY_DURATION {
            return Err(RetryPolicyError::RetryDelayLimit);
        }
        let now = Instant::now();
        if now.checked_add(self.response_timeout).is_none() {
            return Err(RetryPolicyError::ResponseDeadlineOverflow);
        }
        if now.checked_add(self.total_timeout).is_none() {
            return Err(RetryPolicyError::DeadlineOverflow);
        }
        if now.checked_add(self.retry_delay).is_none() {
            return Err(RetryPolicyError::RetryDelayOverflow);
        }
        Ok(())
    }

    /// Sets retries after an uncertain no-response result.
    pub const fn with_transport_retries(mut self, retries: u8) -> Self {
        self.transport_retries = retries;
        self
    }

    /// Sets retries after a Busy response.
    pub const fn with_busy_retries(mut self, retries: u8) -> Self {
        self.busy_retries = retries;
        self
    }

    /// Sets polls after a delayed-response indication.
    pub const fn with_delayed_response_polls(mut self, polls: u8) -> Self {
        self.delayed_response_polls = polls;
        self
    }

    /// Sets the pause between transient device states.
    pub const fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Sets the maximum wait for one response.
    pub const fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Sets the complete queue-and-exchange deadline.
    pub const fn with_total_timeout(mut self, timeout: Duration) -> Self {
        self.total_timeout = timeout;
        self
    }
}

/// Invalid retry or deadline settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RetryPolicyError {
    /// An individual exchange must have time to receive a response.
    #[error("response_timeout must be greater than zero")]
    ResponseTimeout,
    /// Queue waiting and exchange execution need a nonzero total budget.
    #[error("total_timeout must be greater than zero")]
    TotalTimeout,
    /// The requested total deadline cannot be represented by the runtime clock.
    #[error("total_timeout is too large for the runtime clock")]
    DeadlineOverflow,
    /// The requested per-response deadline cannot be represented by the runtime clock.
    #[error("response_timeout is too large for the runtime clock")]
    ResponseDeadlineOverflow,
    /// The retry pause cannot be represented by the runtime clock.
    #[error("retry_delay is too large for the runtime clock")]
    RetryDelayOverflow,
    /// A response wait cannot reserve the serialized link for longer than the hard bound.
    #[error("response_timeout exceeds the supported 24-hour limit")]
    ResponseTimeoutLimit,
    /// An end-to-end request cannot remain queued or in flight beyond the hard bound.
    #[error("total_timeout exceeds the supported 24-hour limit")]
    TotalTimeoutLimit,
    /// A transient retry delay cannot reserve a request beyond the hard bound.
    #[error("retry_delay exceeds the supported 24-hour limit")]
    RetryDelayLimit,
}

/// Baseline settings for the default queue and streaming decoder.
#[derive(Debug, Clone, Copy)]
pub struct LinkConfig {
    /// Capacity used by the default one-queue layout.
    pub queue_capacity: usize,
    /// Maximum transport-read chunk.
    pub read_buffer_size: usize,
    /// Streaming-decoder limits.
    pub decoder: DecodeLimits,
}

impl Default for LinkConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            read_buffer_size: 512,
            decoder: DecodeLimits::default(),
        }
    }
}

impl LinkConfig {
    /// Verifies that every capacity and decoder limit is explicit and usable.
    pub const fn validate(&self) -> Result<(), LinkConfigError> {
        if self.queue_capacity == 0 {
            return Err(LinkConfigError::QueueCapacity);
        }
        if self.queue_capacity > super::MAXIMUM_QUEUE_CAPACITY {
            return Err(LinkConfigError::QueueCapacityLimit(self.queue_capacity));
        }
        if self.read_buffer_size == 0 {
            return Err(LinkConfigError::ReadBufferSize);
        }
        if self.read_buffer_size > MAXIMUM_LINK_READ_BUFFER {
            return Err(LinkConfigError::ReadBufferLimit(self.read_buffer_size));
        }
        if self.decoder.buffer_capacity == 0 {
            return Err(LinkConfigError::DecoderCapacity);
        }
        if self.decoder.minimum_preambles == 0 {
            return Err(LinkConfigError::MinimumPreambles);
        }
        if matches!(
            self.decoder.checksum_policy,
            ChecksumPolicy::KnownGateway {
                checksum_source_node: Some(node),
                ..
            } if node > 63
        ) {
            return Err(LinkConfigError::ChecksumSourceNode);
        }
        if self.decoder.buffer_capacity > MAXIMUM_LINK_DECODER_BUFFER {
            return Err(LinkConfigError::DecoderCapacityLimit(
                self.decoder.buffer_capacity,
            ));
        }
        let required = required_decoder_capacity(self.decoder.minimum_preambles);
        if self.decoder.buffer_capacity < required {
            return Err(LinkConfigError::DecoderFrameCapacity {
                actual: self.decoder.buffer_capacity,
                required,
            });
        }
        Ok(())
    }

    /// Sets the capacity used by the default one-queue layout.
    pub const fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the maximum size of one transport read.
    pub const fn with_read_buffer_size(mut self, bytes: usize) -> Self {
        self.read_buffer_size = bytes;
        self
    }

    /// Replaces streaming decoder limits.
    pub const fn with_decoder(mut self, decoder: DecodeLimits) -> Self {
        self.decoder = decoder;
        self
    }
}

/// Invalid queue or streaming-decoder configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LinkConfigError {
    /// The default queue cannot have zero capacity.
    #[error("queue_capacity must be greater than zero")]
    QueueCapacity,
    /// The default queue is implausibly large and could exhaust runtime resources.
    #[error("queue_capacity {0} exceeds the supported limit")]
    QueueCapacityLimit(usize),
    /// A transport read must have at least one output byte.
    #[error("read_buffer_size must be greater than zero")]
    ReadBufferSize,
    /// A single transport-read allocation is too large.
    #[error("read_buffer_size {0} exceeds the supported limit")]
    ReadBufferLimit(usize),
    /// The streaming decoder must be able to retain input.
    #[error("decoder buffer_capacity must be greater than zero")]
    DecoderCapacity,
    /// A decoder tail is too large for accidental-allocation protection.
    #[error("decoder buffer_capacity {0} exceeds the supported limit")]
    DecoderCapacityLimit(usize),
    /// A decoder tail cannot hold the largest legal frame at the configured preamble count.
    #[error("decoder buffer_capacity {actual} is smaller than the required {required} bytes")]
    DecoderFrameCapacity {
        /// Configured size.
        actual: usize,
        /// Smallest complete-frame size.
        required: usize,
    },
    /// A HART candidate must require at least one preamble.
    #[error("decoder minimum_preambles must be greater than zero")]
    MinimumPreambles,
    /// A compatibility checksum source must be a valid short address.
    #[error("checksum_source_node must be within 0..=63")]
    ChecksumSourceNode,
}

/// Complete construction error for a configured link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LinkBuildError {
    /// Queue or decoder configuration is invalid.
    #[error(transparent)]
    Config(#[from] LinkConfigError),
    /// The queue layout is empty, ambiguous, or exceeds a safety bound.
    #[error(transparent)]
    Queues(#[from] QueueConfigError),
    /// Default retry configuration is invalid.
    #[error(transparent)]
    RetryPolicy(#[from] RetryPolicyError),
    /// The event broadcast channel cannot have zero capacity.
    #[error("event_capacity must be greater than zero")]
    EventCapacity,
    /// The observer channel is implausibly large.
    #[error("event_capacity {0} exceeds the supported limit")]
    EventCapacityLimit(usize),
    /// A post-timeout quarantine longer than this would stall the complete queue.
    #[error("late_response_guard exceeds the supported 60-second limit")]
    LateResponseGuard,
}

/// Observable queue and link event.
#[derive(Debug, Clone)]
pub enum LinkEvent {
    /// A request was accepted by the queue.
    Queued {
        /// Unique request identifier.
        id: u64,
        /// Logical command number.
        command: CommandCode,
        /// Queue selected after routing.
        queue: QueueId,
    },
    /// A request was rejected by admission control before receiving an identifier.
    Denied {
        /// Rejected logical command number.
        command: u16,
        /// Resolved retry-safety classification.
        safety: OperationSafety,
        /// Queue selected after command routing.
        queue: QueueId,
    },
    /// The runner started an exchange.
    Started {
        /// Unique request identifier.
        id: u64,
        /// Logical command number.
        command: CommandCode,
    },
    /// A local request echo was removed from the input stream.
    EchoDiscarded {
        /// Unique request identifier.
        id: u64,
    },
    /// An unsolicited frame was received.
    Unsolicited(DeviceReply),
    /// A valid response unrelated to the current request was skipped.
    StaleFrame {
        /// Identifier of the current request.
        id: u64,
        /// Command of the skipped response.
        command: CommandCode,
    },
    /// A valid solicited response arrived while no request was in flight.
    LateResponse(DeviceReply),
    /// A complete candidate frame was rejected by link-layer validation.
    RejectedFrame {
        /// Identifier of the current request.
        id: u64,
        /// Validation error retained for diagnostics.
        error: WireError,
    },
    /// A frame received while idle failed link-layer validation.
    IdleRejectedFrame {
        /// Validation error retained for diagnostics.
        error: WireError,
    },
    /// The physical channel stopped while the runner was idle.
    ChannelStopped {
        /// Stable reason suitable for a metric label.
        reason: &'static str,
    },
    /// The last waiter disappeared before the exchange completed.
    Cancelled {
        /// Unique request identifier. An already started exchange is drained safely.
        id: u64,
    },
    /// An identical read-only request joined an already selected exchange.
    Coalesced {
        /// Identifier of the joined request.
        id: u64,
        /// Identifier of the exchange that will provide its result.
        leader_id: u64,
    },
    /// An allowed retry is being performed.
    Retrying {
        /// Unique request identifier.
        id: u64,
        /// Attempt number, starting with two.
        attempt: u8,
        /// Reason the previous attempt did not finish the exchange.
        cause: RetryCause,
    },
    /// A request completed successfully.
    Completed {
        /// Unique request identifier.
        id: u64,
    },
    /// A request failed.
    Failed {
        /// Unique request identifier.
        id: u64,
        /// Short explanation.
        reason: &'static str,
    },
}

/// Observable reason for retrying an exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryCause {
    /// No response arrived before the per-attempt timeout.
    Transport,
    /// The device returned Busy.
    Busy,
    /// The device reported an initiated or running delayed response.
    DelayedResponse,
}

/// State snapshot obtained without locking the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinkSnapshot {
    /// Number of pending requests across every configured queue.
    pub queued: usize,
    /// Total number of physical exchanges started after read-only coalescing.
    pub started: u64,
    /// Total number of successful requests.
    pub completed: u64,
    /// Total number of failures.
    pub failed: u64,
    /// Total number of requests cancelled by their waiters.
    pub cancelled: u64,
    /// Total number of requests joined to an identical read-only exchange.
    pub coalesced: u64,
    /// Total number of requests rejected before occupying queue capacity.
    pub denied: u64,
}

#[derive(Debug, Default)]
struct Counters {
    started: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    cancelled: AtomicU64,
    coalesced: AtomicU64,
    denied: AtomicU64,
    next_id: AtomicU64,
}

/// Error returned when enqueueing without waiting for capacity.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum StartError {
    /// The configured command policy rejected the request before enqueueing.
    #[error(
        "command {command} with safety {safety:?} is denied for queue {queue} by the admission policy"
    )]
    CommandDenied {
        /// Rejected logical command number.
        command: u16,
        /// Resolved retry-safety classification.
        safety: OperationSafety,
        /// Queue selected after command routing.
        queue: QueueId,
    },
    /// The selected queue is not part of this link.
    #[error("queue {0} is not configured on this link")]
    UnknownQueue(QueueId),
    /// The selected queue is full.
    #[error("queue is full")]
    Full,
    /// The link runner has already stopped.
    #[error("link runner has stopped")]
    Closed,
    /// The end-to-end deadline expired before queue capacity became available.
    #[error("request deadline expired while waiting for queue capacity")]
    Deadline,
    /// Retry or timeout settings are invalid.
    #[error(transparent)]
    RetryPolicy(#[from] RetryPolicyError),
    /// The request cannot be represented by a HART frame.
    #[error(transparent)]
    Operation(#[from] OperationError),
    /// A modem or gateway wake sequence is implausibly large.
    #[error("transmit prefix contains {0} bytes and exceeds the supported limit")]
    TransmitPrefix(usize),
}

/// Device-exchange error.
#[derive(Debug, Error)]
pub enum ExchangeError {
    /// The configured command policy rejected the request before enqueueing.
    #[error(
        "command {command} with safety {safety:?} is denied for queue {queue} by the admission policy"
    )]
    CommandDenied {
        /// Rejected logical command number.
        command: u16,
        /// Resolved retry-safety classification.
        safety: OperationSafety,
        /// Queue selected after command routing.
        queue: QueueId,
    },
    /// The selected queue is not part of this link.
    #[error("queue {0} is not configured on this link")]
    UnknownQueue(QueueId),
    /// The request did not complete before its end-to-end deadline.
    #[error("request deadline expired")]
    Deadline,
    /// An individual attempt received no response.
    #[error("response timeout expired")]
    ResponseTimeout,
    /// Transmission did not finish, so the byte stream cannot be reused safely.
    #[error("request transmission timed out and the channel must be reopened")]
    SendTimeout,
    /// The device continued returning a transient rejection.
    #[error("transient-device retries exhausted; last response code was {0}")]
    Busy(u8),
    /// The device did not complete a delayed response within the polling limit.
    #[error("delayed-response polling exhausted; last response code was {0}")]
    DelayedResponse(u8),
    /// The channel closed or returned an error.
    #[error(transparent)]
    Channel(#[from] ChannelError),
    /// A frame could not be built or decoded.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// The application response does not match its format.
    #[error(transparent)]
    Operation(#[from] OperationError),
    /// The runner stopped before returning a result.
    #[error("runner stopped before the request completed")]
    RunnerStopped,
    /// Retry or timeout settings are invalid.
    #[error(transparent)]
    RetryPolicy(#[from] RetryPolicyError),
    /// A modem or gateway wake sequence is implausibly large.
    #[error("transmit prefix contains {0} bytes and exceeds the supported limit")]
    TransmitPrefix(usize),
}

struct Job {
    id: u64,
    request: Request,
    transmit_prefix: Vec<u8>,
    policy: RetryPolicy,
    deadline: Instant,
    reply: Option<oneshot::Sender<Result<DeviceReply, ExchangeError>>>,
}

/// Cancelable wait for an enqueued request.
#[derive(Debug)]
pub struct PendingReply {
    id: u64,
    receiver: oneshot::Receiver<Result<DeviceReply, ExchangeError>>,
}

impl PendingReply {
    /// Returns the unique request identifier.
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Waits for the result. Dropping this value cancels the caller's interest.
    pub async fn wait(self) -> Result<DeviceReply, ExchangeError> {
        self.receiver
            .await
            .map_err(|_| ExchangeError::RunnerStopped)?
    }
}

struct ClientLifetime {
    queues: Arc<QueueSet<Job>>,
}

impl Drop for ClientLifetime {
    fn drop(&mut self) {
        self.queues.close_input();
    }
}

/// Cloneable entry point to all configured queues of one physical link.
#[derive(Clone)]
pub struct LinkClient {
    queues: Arc<QueueSet<Job>>,
    _lifetime: Arc<ClientLifetime>,
    events: broadcast::Sender<LinkEvent>,
    counters: Arc<Counters>,
    default_queue: QueueId,
    default_retry: RetryPolicy,
    command_policy: CommandPolicy,
    command_routing: CommandRouting,
}

impl fmt::Debug for LinkClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LinkClient")
            .field("default_queue", &self.default_queue)
            .field("default_retry", &self.default_retry)
            .field("queues", &self.queues.snapshots())
            .finish_non_exhaustive()
    }
}

impl LinkClient {
    /// Returns a cloneable handle permanently bound to one configured queue.
    pub fn queue(&self, queue: QueueId) -> Result<QueueClient, QueueError> {
        if !self.queues.contains(queue) {
            return Err(QueueError::Unknown(queue));
        }
        Ok(QueueClient::new(self.clone(), queue))
    }

    /// Returns a handle bound to the queue selected as the link default.
    pub fn default_queue(&self) -> QueueClient {
        QueueClient::new(self.clone(), self.default_queue)
    }

    /// Returns the default queue identifier.
    pub const fn default_queue_id(&self) -> QueueId {
        self.default_queue
    }

    /// Returns the retry policy used by convenience operations.
    pub const fn default_retry(&self) -> RetryPolicy {
        self.default_retry
    }

    /// Returns one queue's current scheduling policy.
    pub fn queue_priority(&self, queue: QueueId) -> Result<QueuePriority, QueueError> {
        self.queues.priority(queue)
    }

    /// Atomically changes one queue's scheduling policy.
    ///
    /// The in-flight exchange is not interrupted. The next scheduling decision observes the
    /// new policy. Strict priority is intentionally allowed to starve weighted queues.
    pub fn set_queue_priority(
        &self,
        queue: QueueId,
        priority: QueuePriority,
    ) -> Result<(), QueueError> {
        self.queues.set_priority(queue, priority)
    }

    /// Atomically assigns a starvation-free weighted quota to one queue.
    pub fn set_queue_weight(&self, queue: QueueId, weight: u16) -> Result<(), QueueError> {
        self.queues.set_weight(queue, weight)
    }

    /// Returns one independently bounded queue snapshot.
    pub fn queue_snapshot(&self, queue: QueueId) -> Result<QueueSnapshot, QueueError> {
        self.queues.snapshot(queue)
    }

    /// Returns snapshots for every configured queue in scheduling order.
    pub fn queue_snapshots(&self) -> Vec<QueueSnapshot> {
        self.queues.snapshots()
    }

    /// Enqueues a request into the default queue.
    pub async fn start_default(&self, request: Request) -> Result<PendingReply, StartError> {
        self.start(request, self.default_queue, self.default_retry)
            .await
    }

    /// Enqueues a complete request and returns a cancelable wait handle.
    pub async fn start(
        &self,
        request: Request,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<PendingReply, StartError> {
        self.start_with_prefix(request, Vec::new(), queue, policy)
            .await
    }

    /// Enqueues a request with bytes transmitted immediately before its HART frame.
    pub async fn start_with_prefix(
        &self,
        request: Request,
        transmit_prefix: Vec<u8>,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<PendingReply, StartError> {
        let queue = self.resolve_queue(&request, queue)?;
        let (job, receiver) = self.make_job(request, transmit_prefix, queue, policy)?;
        let id = job.id;
        let command = job.request.command;
        let deadline = job.deadline;
        self.queues
            .push(queue, job, deadline)
            .await
            .map_err(map_queue_push_error)?;
        let _ = self.events.send(LinkEvent::Queued { id, command, queue });
        Ok(PendingReply { id, receiver })
    }

    /// Attempts to enqueue without waiting for queue capacity.
    pub fn try_start(
        &self,
        request: Request,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<PendingReply, StartError> {
        let queue = self.resolve_queue(&request, queue)?;
        let (job, receiver) = self.make_job(request, Vec::new(), queue, policy)?;
        let id = job.id;
        let command = job.request.command;
        self.queues
            .try_push(queue, job)
            .map_err(map_queue_push_error)?;
        let _ = self.events.send(LinkEvent::Queued { id, command, queue });
        Ok(PendingReply { id, receiver })
    }

    /// Attempts to enqueue immediately using the configured defaults.
    pub fn try_start_default(&self, request: Request) -> Result<PendingReply, StartError> {
        self.try_start(request, self.default_queue, self.default_retry)
    }

    /// Executes a complete request through the selected queue.
    pub async fn request(
        &self,
        request: Request,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<DeviceReply, ExchangeError> {
        self.request_with_prefix(request, Vec::new(), queue, policy)
            .await
    }

    /// Executes a request through the default queue.
    pub async fn request_default(&self, request: Request) -> Result<DeviceReply, ExchangeError> {
        self.request(request, self.default_queue, self.default_retry)
            .await
    }

    /// Executes a prefixed request through the default queue.
    pub async fn request_default_with_prefix(
        &self,
        request: Request,
        transmit_prefix: Vec<u8>,
    ) -> Result<DeviceReply, ExchangeError> {
        self.request_with_prefix(
            request,
            transmit_prefix,
            self.default_queue,
            self.default_retry,
        )
        .await
    }

    /// Executes a request after an explicit gateway or modem wake-up sequence.
    pub async fn request_with_prefix(
        &self,
        request: Request,
        transmit_prefix: Vec<u8>,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<DeviceReply, ExchangeError> {
        self.start_with_prefix(request, transmit_prefix, queue, policy)
            .await
            .map_err(start_to_exchange_error)?
            .wait()
            .await
    }

    /// Executes and decodes a typed operation through the selected queue.
    pub async fn execute<O: Operation>(
        &self,
        address: crate::Address,
        operation: &O,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<O::Output, ExchangeError> {
        let request = operation.request(address)?;
        let reply = self.request(request, queue, policy).await?;
        operation.decode_reply(&reply).map_err(ExchangeError::from)
    }

    /// Executes a typed operation using the configured defaults.
    pub async fn execute_default<O: Operation>(
        &self,
        address: crate::Address,
        operation: &O,
    ) -> Result<O::Output, ExchangeError> {
        self.execute(address, operation, self.default_queue, self.default_retry)
            .await
    }

    /// Executes a typed operation while accepting explicitly listed warning codes.
    pub async fn execute_accepting<O: Operation>(
        &self,
        address: crate::Address,
        operation: &O,
        accepted_warnings: &[u8],
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<CommandOutcome<O::Output>, ExchangeError> {
        let request = operation.request(address)?;
        let reply = self.request(request, queue, policy).await?;
        operation
            .decode_reply_accepting(&reply, accepted_warnings)
            .map_err(ExchangeError::from)
    }

    /// Executes with configured defaults while accepting explicit warning codes.
    pub async fn execute_accepting_default<O: Operation>(
        &self,
        address: crate::Address,
        operation: &O,
        accepted_warnings: &[u8],
    ) -> Result<CommandOutcome<O::Output>, ExchangeError> {
        self.execute_accepting(
            address,
            operation,
            accepted_warnings,
            self.default_queue,
            self.default_retry,
        )
        .await
    }

    /// Subscribes an observer to events without affecting scheduling.
    pub fn subscribe(&self) -> broadcast::Receiver<LinkEvent> {
        self.events.subscribe()
    }

    /// Returns aggregate counters without locking the physical channel.
    pub fn snapshot(&self) -> LinkSnapshot {
        LinkSnapshot {
            queued: self.queues.pending_total(),
            started: self.counters.started.load(Ordering::Relaxed),
            completed: self.counters.completed.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
            cancelled: self.counters.cancelled.load(Ordering::Relaxed),
            coalesced: self.counters.coalesced.load(Ordering::Relaxed),
            denied: self.counters.denied.load(Ordering::Relaxed),
        }
    }

    fn make_job(
        &self,
        request: Request,
        transmit_prefix: Vec<u8>,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<(Job, oneshot::Receiver<Result<DeviceReply, ExchangeError>>), StartError> {
        policy.validate_runtime()?;
        request.validate()?;
        let safety = resolved_safety(&request);
        if !self.command_policy.admits(CommandContext {
            request: &request,
            safety,
            catalog_safety: catalog_safety(&request),
            queue,
        }) {
            self.counters.denied.fetch_add(1, Ordering::Relaxed);
            let _ = self.events.send(LinkEvent::Denied {
                command: request.command.get(),
                safety,
                queue,
            });
            return Err(StartError::CommandDenied {
                command: request.command.get(),
                safety,
                queue,
            });
        }
        if transmit_prefix.len() > MAXIMUM_TRANSMIT_PREFIX {
            return Err(StartError::TransmitPrefix(transmit_prefix.len()));
        }
        let id = self
            .counters
            .next_id
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let deadline = Instant::now()
            .checked_add(policy.total_timeout)
            .ok_or(RetryPolicyError::DeadlineOverflow)?;
        let (reply, receiver) = oneshot::channel();
        Ok((
            Job {
                id,
                request,
                transmit_prefix,
                policy,
                deadline,
                reply: Some(reply),
            },
            receiver,
        ))
    }

    fn resolve_queue(&self, request: &Request, requested: QueueId) -> Result<QueueId, StartError> {
        if !self.queues.contains(requested) {
            return Err(StartError::UnknownQueue(requested));
        }
        let queue = self.command_routing.route(CommandContext {
            request,
            safety: resolved_safety(request),
            catalog_safety: catalog_safety(request),
            queue: requested,
        });
        if !self.queues.contains(queue) {
            return Err(StartError::UnknownQueue(queue));
        }
        Ok(queue)
    }
}

fn map_queue_push_error(error: QueuePushError) -> StartError {
    match error {
        QueuePushError::Full => StartError::Full,
        QueuePushError::Closed => StartError::Closed,
        QueuePushError::Deadline => StartError::Deadline,
        QueuePushError::Unknown(queue) => StartError::UnknownQueue(queue),
    }
}

fn start_to_exchange_error(error: StartError) -> ExchangeError {
    match error {
        StartError::CommandDenied {
            command,
            safety,
            queue,
        } => ExchangeError::CommandDenied {
            command,
            safety,
            queue,
        },
        StartError::UnknownQueue(queue) => ExchangeError::UnknownQueue(queue),
        StartError::Deadline => ExchangeError::Deadline,
        StartError::RetryPolicy(error) => ExchangeError::RetryPolicy(error),
        StartError::Operation(error) => ExchangeError::Operation(error),
        StartError::TransmitPrefix(length) => ExchangeError::TransmitPrefix(length),
        StartError::Closed | StartError::Full => ExchangeError::RunnerStopped,
    }
}

/// Cloneable handle permanently bound to one configured queue.
#[derive(Debug, Clone)]
pub struct QueueClient {
    link: LinkClient,
    queue: QueueId,
}

impl QueueClient {
    fn new(link: LinkClient, queue: QueueId) -> Self {
        Self { link, queue }
    }

    /// Returns this handle's queue identifier.
    pub const fn id(&self) -> QueueId {
        self.queue
    }

    /// Returns the underlying shared link client.
    pub const fn link(&self) -> &LinkClient {
        &self.link
    }

    /// Returns this queue's live state.
    pub fn snapshot(&self) -> QueueSnapshot {
        self.link
            .queue_snapshot(self.queue)
            .expect("QueueClient is only created for configured queues")
    }

    /// Atomically replaces this queue's scheduling policy.
    pub fn set_priority(&self, priority: QueuePriority) -> Result<(), QueueError> {
        self.link.set_queue_priority(self.queue, priority)
    }

    /// Atomically moves this queue into starvation-free weighted scheduling.
    pub fn set_weight(&self, weight: u16) -> Result<(), QueueError> {
        self.link.set_queue_weight(self.queue, weight)
    }

    /// Enqueues with an explicit retry policy.
    pub async fn start(
        &self,
        request: Request,
        policy: RetryPolicy,
    ) -> Result<PendingReply, StartError> {
        self.link.start(request, self.queue, policy).await
    }

    /// Enqueues immediately without waiting for capacity.
    pub fn try_start(
        &self,
        request: Request,
        policy: RetryPolicy,
    ) -> Result<PendingReply, StartError> {
        self.link.try_start(request, self.queue, policy)
    }

    /// Executes a request with an explicit retry policy.
    pub async fn request(
        &self,
        request: Request,
        policy: RetryPolicy,
    ) -> Result<DeviceReply, ExchangeError> {
        self.link.request(request, self.queue, policy).await
    }

    /// Executes a request with the link's default retry policy.
    pub async fn request_default(&self, request: Request) -> Result<DeviceReply, ExchangeError> {
        self.request(request, self.link.default_retry()).await
    }

    /// Executes and decodes a typed operation with an explicit retry policy.
    pub async fn execute<O: Operation>(
        &self,
        address: crate::Address,
        operation: &O,
        policy: RetryPolicy,
    ) -> Result<O::Output, ExchangeError> {
        self.link
            .execute(address, operation, self.queue, policy)
            .await
    }

    /// Executes and decodes a typed operation with the link's default retry policy.
    pub async fn execute_default<O: Operation>(
        &self,
        address: crate::Address,
        operation: &O,
    ) -> Result<O::Output, ExchangeError> {
        self.execute(address, operation, self.link.default_retry())
            .await
    }
}

/// Sole owner of the physical channel and link decoder.
pub struct LinkRunner<C> {
    channel: C,
    queues: Arc<QueueSet<Job>>,
    events: broadcast::Sender<LinkEvent>,
    counters: Arc<Counters>,
    decoder: FrameDecoder,
    read_buffer: Vec<u8>,
    burst_hub: Option<BurstHub>,
    channel_usable: bool,
    maximum_coalesced: usize,
    late_response_guard: Duration,
}

/// Fluent construction of a queue, runner, defaults, and optional Burst router.
pub struct LinkBuilder<C> {
    channel: C,
    config: LinkConfig,
    event_capacity: usize,
    maximum_coalesced: usize,
    queue_layout: Option<Vec<QueueConfig>>,
    default_queue: QueueId,
    default_retry: RetryPolicy,
    burst_hub: Option<BurstHub>,
    late_response_guard: Duration,
    command_policy: CommandPolicy,
    command_routing: CommandRouting,
}

struct LinkRuntimeOptions {
    event_capacity: usize,
    maximum_coalesced: usize,
    queue_layout: Vec<QueueConfig>,
    default_queue: QueueId,
    default_retry: RetryPolicy,
    late_response_guard: Duration,
    command_policy: CommandPolicy,
    command_routing: CommandRouting,
}

impl Default for LinkRuntimeOptions {
    fn default() -> Self {
        Self {
            event_capacity: 512,
            maximum_coalesced: 64,
            queue_layout: vec![
                QueueConfig::weighted(QueueId::DEFAULT, 256, 1)
                    .expect("the built-in queue layout is valid"),
            ],
            default_queue: QueueId::DEFAULT,
            default_retry: RetryPolicy::default(),
            late_response_guard: DEFAULT_LATE_RESPONSE_GUARD,
            command_policy: CommandPolicy::allow_all(),
            command_routing: CommandRouting::preserve_requested(),
        }
    }
}

impl<C: ByteChannel> LinkBuilder<C> {
    /// Starts with conservative production defaults.
    pub fn new(channel: C) -> Self {
        Self {
            channel,
            config: LinkConfig::default(),
            event_capacity: 512,
            maximum_coalesced: 64,
            queue_layout: None,
            default_queue: QueueId::DEFAULT,
            default_retry: RetryPolicy::default(),
            burst_hub: None,
            late_response_guard: DEFAULT_LATE_RESPONSE_GUARD,
            command_policy: CommandPolicy::default(),
            command_routing: CommandRouting::default(),
        }
    }

    /// Replaces queue and decoder settings.
    pub const fn config(mut self, config: LinkConfig) -> Self {
        self.config = config;
        self
    }

    /// Replaces the complete queue layout.
    ///
    /// Queue order is also the deterministic rotation order for weighted peers. Validation is
    /// deferred to [`Self::build`] so a layout can be assembled without partial runtime state.
    pub fn queues(mut self, queues: impl IntoIterator<Item = QueueConfig>) -> Self {
        self.queue_layout = Some(queues.into_iter().collect());
        self
    }

    /// Sets the maximum transport-read allocation.
    pub const fn read_buffer_size(mut self, read_buffer_size: usize) -> Self {
        self.config.read_buffer_size = read_buffer_size;
        self
    }

    /// Replaces streaming decoder limits.
    pub const fn decoder(mut self, decoder: DecodeLimits) -> Self {
        self.config.decoder = decoder;
        self
    }

    /// Sets the number of buffered observer events.
    pub const fn event_capacity(mut self, event_capacity: usize) -> Self {
        self.event_capacity = event_capacity;
        self
    }

    /// Limits followers joined to one identical read-only exchange; zero disables coalescing.
    pub const fn maximum_coalesced(mut self, maximum_coalesced: usize) -> Self {
        self.maximum_coalesced = maximum_coalesced;
        self
    }

    /// Selects the queue used by convenience methods on [`LinkClient`].
    pub const fn default_queue(mut self, default_queue: QueueId) -> Self {
        self.default_queue = default_queue;
        self
    }

    /// Sets the retry policy used by the convenience methods on [`LinkClient`].
    pub const fn default_retry(mut self, default_retry: RetryPolicy) -> Self {
        self.default_retry = default_retry;
        self
    }

    /// Applies admission control before a request occupies queue capacity.
    pub fn command_policy(mut self, command_policy: CommandPolicy) -> Self {
        self.command_policy = command_policy;
        self
    }

    /// Applies automatic command-to-queue routing before admission control.
    pub fn command_routing(mut self, command_routing: CommandRouting) -> Self {
        self.command_routing = command_routing;
        self
    }

    /// Routes unsolicited Burst frames through the supplied hub.
    pub fn burst_hub(mut self, burst_hub: BurstHub) -> Self {
        self.burst_hub = Some(burst_hub);
        self
    }

    /// Sets the input-drain period after a response timeout; zero disables the guard.
    pub const fn late_response_guard(mut self, late_response_guard: Duration) -> Self {
        self.late_response_guard = late_response_guard;
        self
    }

    /// Validates all settings and creates the connected client-runner pair.
    pub fn build(self) -> Result<(LinkClient, LinkRunner<C>), LinkBuildError> {
        self.config.validate()?;
        self.default_retry.validate_runtime()?;
        if self.event_capacity == 0 {
            return Err(LinkBuildError::EventCapacity);
        }
        if self.event_capacity > MAXIMUM_LINK_EVENT_CAPACITY {
            return Err(LinkBuildError::EventCapacityLimit(self.event_capacity));
        }
        if self.late_response_guard > MAXIMUM_LATE_RESPONSE_GUARD {
            return Err(LinkBuildError::LateResponseGuard);
        }
        let queue_layout = self.queue_layout.unwrap_or_else(|| {
            vec![
                QueueConfig::weighted(QueueId::DEFAULT, self.config.queue_capacity, 1)
                    .expect("LinkConfig validation guarantees the default queue"),
            ]
        });
        validate_layout(&queue_layout, self.default_queue)?;
        let options = LinkRuntimeOptions {
            event_capacity: self.event_capacity,
            maximum_coalesced: self.maximum_coalesced,
            queue_layout,
            default_queue: self.default_queue,
            default_retry: self.default_retry,
            late_response_guard: self.late_response_guard,
            command_policy: self.command_policy,
            command_routing: self.command_routing,
        };
        let (client, mut runner) = build_link(self.channel, self.config, options);
        runner.burst_hub = self.burst_hub;
        Ok((client, runner))
    }
}

/// Creates a connected client-runner pair using compatibility normalization.
///
/// Invalid capacities are clamped to safe compatibility bounds. New applications that want
/// invalid configuration reported explicitly should call [`try_create_link`] or [`LinkBuilder`].
pub fn create_link<C: ByteChannel>(channel: C, config: LinkConfig) -> (LinkClient, LinkRunner<C>) {
    let config = LinkConfig {
        queue_capacity: config
            .queue_capacity
            .clamp(1, super::MAXIMUM_QUEUE_CAPACITY),
        read_buffer_size: config.read_buffer_size.clamp(64, MAXIMUM_LINK_READ_BUFFER),
        decoder: DecodeLimits {
            buffer_capacity: config.decoder.buffer_capacity.clamp(
                required_decoder_capacity(config.decoder.minimum_preambles.max(1)),
                MAXIMUM_LINK_DECODER_BUFFER,
            ),
            minimum_preambles: config.decoder.minimum_preambles.max(1),
            checksum_policy: normalized_checksum_policy(config.decoder.checksum_policy),
        },
    };
    build_link(channel, config, default_runtime_options(config))
}

const fn required_decoder_capacity(minimum_preambles: u8) -> usize {
    MAX_ENCODED_FRAME_SIZE - u8::MAX as usize + minimum_preambles as usize
}

const fn normalized_checksum_policy(policy: ChecksumPolicy) -> ChecksumPolicy {
    match policy {
        ChecksumPolicy::KnownGateway {
            delimiter_bit,
            checksum_source_node: Some(node),
        } if node > 63 => ChecksumPolicy::KnownGateway {
            delimiter_bit,
            checksum_source_node: None,
        },
        policy => policy,
    }
}

/// Validates the complete configuration and creates a connected client-runner pair.
pub fn try_create_link<C: ByteChannel>(
    channel: C,
    config: LinkConfig,
) -> Result<(LinkClient, LinkRunner<C>), LinkConfigError> {
    config.validate()?;
    Ok(build_link(channel, config, default_runtime_options(config)))
}

fn default_runtime_options(config: LinkConfig) -> LinkRuntimeOptions {
    LinkRuntimeOptions {
        queue_layout: vec![
            QueueConfig::weighted(QueueId::DEFAULT, config.queue_capacity, 1)
                .expect("validated LinkConfig always creates a valid default queue"),
        ],
        ..LinkRuntimeOptions::default()
    }
}

fn build_link<C: ByteChannel>(
    channel: C,
    config: LinkConfig,
    options: LinkRuntimeOptions,
) -> (LinkClient, LinkRunner<C>) {
    let (events, _) = broadcast::channel(options.event_capacity);
    let counters = Arc::new(Counters::default());
    let queues = Arc::new(QueueSet::new(&options.queue_layout));
    let lifetime = Arc::new(ClientLifetime {
        queues: Arc::clone(&queues),
    });
    let client = LinkClient {
        queues: Arc::clone(&queues),
        _lifetime: lifetime,
        events: events.clone(),
        counters: counters.clone(),
        default_queue: options.default_queue,
        default_retry: options.default_retry,
        command_policy: options.command_policy,
        command_routing: options.command_routing,
    };
    let runner = LinkRunner {
        channel,
        queues,
        events,
        counters,
        decoder: FrameDecoder::new(config.decoder),
        read_buffer: vec![0; config.read_buffer_size],
        burst_hub: None,
        channel_usable: true,
        maximum_coalesced: options.maximum_coalesced,
        late_response_guard: options.late_response_guard,
    };
    (client, runner)
}

impl<C: ByteChannel> LinkRunner<C> {
    /// Attaches an unsolicited-message router.
    pub fn with_burst_hub(mut self, hub: BurstHub) -> Self {
        self.burst_hub = Some(hub);
        self
    }

    /// Serves the queue until every client is closed.
    pub async fn run(mut self) {
        let mut cursor = QueueCursor::new();
        while let Some((queue, entry)) = self.next_job(&mut cursor).await {
            let mut job = self.queues.finish_entry(queue, entry);
            if job.reply.as_ref().is_none_or(oneshot::Sender::is_closed) {
                self.mark_cancelled(job.id);
                continue;
            }
            let Some(reply) = job.reply.take() else {
                self.mark_cancelled(job.id);
                continue;
            };
            if Instant::now() >= job.deadline {
                self.mark_failed(job.id, &ExchangeError::Deadline);
                let _ = reply.send(Err(ExchangeError::Deadline));
                continue;
            }
            let followers = self.take_coalescible_followers(queue, &job);
            self.counters.started.fetch_add(1, Ordering::Relaxed);
            let _ = self.events.send(LinkEvent::Started {
                id: job.id,
                command: job.request.command,
            });
            match self.exchange(&job).await {
                Ok(value) => {
                    if reply.is_closed() {
                        self.mark_cancelled(job.id);
                    } else {
                        self.mark_completed(job.id);
                        let _ = reply.send(Ok(value.clone()));
                    }
                    self.complete_followers(queue, followers, &value);
                }
                Err(error) => {
                    let timed_out = matches!(error, ExchangeError::ResponseTimeout);
                    self.requeue_followers(queue, followers);
                    if reply.is_closed() {
                        self.mark_cancelled(job.id);
                    } else {
                        self.mark_failed(job.id, &error);
                        let _ = reply.send(Err(error));
                    }
                    if timed_out && self.channel_usable {
                        self.drain_late_responses().await;
                    }
                }
            }
            if !self.channel_usable {
                break;
            }
        }
        self.fail_pending_jobs();
    }

    async fn next_job(&mut self, cursor: &mut QueueCursor) -> Option<(QueueId, QueueEntry<Job>)> {
        loop {
            let notified = self.queues.notified();
            if let Some(job) = self.queues.pop_scheduled(cursor) {
                return Some(job);
            }
            if self.queues.input_closed() {
                return None;
            }
            let input = tokio::select! {
                () = notified => None,
                value = self.channel.receive(&mut self.read_buffer) => Some(value),
            };
            if let Some(result) = input
                && !self.process_idle_input(&result)
            {
                return None;
            }
        }
    }

    fn take_coalescible_followers(&mut self, queue: QueueId, leader: &Job) -> Vec<QueueEntry<Job>> {
        if resolved_safety(&leader.request) != OperationSafety::ReadOnly
            || !leader.transmit_prefix.is_empty()
            || self.maximum_coalesced == 0
        {
            return Vec::new();
        }
        let mut followers = Vec::new();
        while followers.len() < self.maximum_coalesced {
            let Some(candidate) = self.queues.pop_from(queue) else {
                break;
            };
            if requests_can_coalesce(leader, candidate.item()) {
                if candidate
                    .item()
                    .reply
                    .as_ref()
                    .is_none_or(oneshot::Sender::is_closed)
                {
                    let candidate = self.queues.finish_entry(queue, candidate);
                    self.mark_cancelled(candidate.id);
                } else {
                    self.counters.coalesced.fetch_add(1, Ordering::Relaxed);
                    let _ = self.events.send(LinkEvent::Coalesced {
                        id: candidate.item().id,
                        leader_id: leader.id,
                    });
                    followers.push(candidate);
                }
                continue;
            }
            self.queues.push_front(queue, candidate);
            break;
        }
        followers
    }

    fn complete_followers(
        &self,
        queue: QueueId,
        followers: Vec<QueueEntry<Job>>,
        value: &DeviceReply,
    ) {
        for follower in followers {
            let mut follower = self.queues.finish_entry(queue, follower);
            let Some(reply) = follower.reply.take() else {
                self.mark_cancelled(follower.id);
                continue;
            };
            if reply.is_closed() {
                self.mark_cancelled(follower.id);
            } else if Instant::now() >= follower.deadline {
                self.mark_failed(follower.id, &ExchangeError::Deadline);
                let _ = reply.send(Err(ExchangeError::Deadline));
            } else {
                self.mark_completed(follower.id);
                let _ = reply.send(Ok(value.clone()));
            }
        }
    }

    fn requeue_followers(&mut self, queue: QueueId, followers: Vec<QueueEntry<Job>>) {
        for follower in followers.into_iter().rev() {
            self.queues.push_front(queue, follower);
        }
    }

    fn fail_pending_jobs(&mut self) {
        self.queues.close_runner();
        for snapshot in self.queues.snapshots() {
            while let Some(entry) = self.queues.pop_from(snapshot.id) {
                let mut job = self.queues.finish_entry(snapshot.id, entry);
                fail_stopped_job(self, &mut job);
            }
        }
    }

    fn process_idle_input(&mut self, result: &Result<usize, ChannelError>) -> bool {
        let read = match result {
            Ok(read) if *read > 0 && *read <= self.read_buffer.len() => *read,
            Err(error) if is_receive_timeout(error) => return true,
            Ok(0) | Err(ChannelError::Closed) => {
                let _ = self.events.send(LinkEvent::ChannelStopped {
                    reason: "channel closed",
                });
                return false;
            }
            Ok(_) => {
                let _ = self.events.send(LinkEvent::ChannelStopped {
                    reason: "invalid receive length",
                });
                return false;
            }
            Err(_) => {
                let _ = self.events.send(LinkEvent::ChannelStopped {
                    reason: "channel error",
                });
                return false;
            }
        };
        for event in self.decoder.push(&self.read_buffer[..read]) {
            match event {
                DecodeEvent::Rejected(error) => {
                    let _ = self.events.send(LinkEvent::IdleRejectedFrame { error });
                }
                DecodeEvent::Frame(frame) => {
                    let Ok(reply) = DeviceReply::from_frame(frame) else {
                        continue;
                    };
                    if reply.burst {
                        if let Some(hub) = &self.burst_hub {
                            hub.publish(reply.clone());
                        }
                        let _ = self.events.send(LinkEvent::Unsolicited(reply));
                    } else {
                        let _ = self.events.send(LinkEvent::LateResponse(reply));
                    }
                }
            }
        }
        true
    }

    async fn drain_late_responses(&mut self) {
        if self.late_response_guard.is_zero() {
            return;
        }
        let Some(deadline) = Instant::now().checked_add(self.late_response_guard) else {
            return;
        };
        loop {
            match timeout_at(deadline, self.channel.receive(&mut self.read_buffer)).await {
                Err(_) => return,
                Ok(result) if self.process_idle_input(&result) => {}
                Ok(_) => {
                    self.channel_usable = false;
                    return;
                }
            }
        }
    }

    async fn exchange(&mut self, job: &Job) -> Result<DeviceReply, ExchangeError> {
        if Instant::now() >= job.deadline {
            return Err(ExchangeError::Deadline);
        }
        let frame = job.request.to_frame()?;
        let frame_bytes = frame.encode()?;
        let encoded = if job.transmit_prefix.is_empty() {
            frame_bytes
        } else {
            let mut bytes = Vec::with_capacity(job.transmit_prefix.len() + frame_bytes.len());
            bytes.extend_from_slice(&job.transmit_prefix);
            bytes.extend_from_slice(&frame_bytes);
            bytes
        };
        let safety = resolved_safety(&job.request);
        let transport_limit = if safety == OperationSafety::Action {
            0
        } else {
            job.policy.transport_retries
        };
        let mut attempt_number = 1_u8;
        let mut transport_retries = 0_u8;
        let mut busy_retries = 0_u8;
        let mut delayed_response_polls = 0_u8;
        loop {
            let exchange_deadline = Instant::now()
                .checked_add(job.policy.response_timeout)
                .unwrap_or(job.deadline)
                .min(job.deadline);
            let attempt = self
                .exchange_once(job.id, &frame, &encoded, exchange_deadline)
                .await;
            match attempt {
                Ok(reply) if reply.response_code == 32 => {
                    if busy_retries >= job.policy.busy_retries {
                        return Err(ExchangeError::Busy(reply.response_code));
                    }
                    busy_retries = busy_retries.saturating_add(1);
                    attempt_number = attempt_number.saturating_add(1);
                    let _ = self.events.send(LinkEvent::Retrying {
                        id: job.id,
                        attempt: attempt_number,
                        cause: RetryCause::Busy,
                    });
                    wait_before_retry(job.policy.retry_delay, job.deadline).await?;
                }
                Ok(reply) if matches!(reply.response_code, 33 | 34) => {
                    if delayed_response_polls >= job.policy.delayed_response_polls {
                        return Err(ExchangeError::DelayedResponse(reply.response_code));
                    }
                    delayed_response_polls = delayed_response_polls.saturating_add(1);
                    attempt_number = attempt_number.saturating_add(1);
                    let _ = self.events.send(LinkEvent::Retrying {
                        id: job.id,
                        attempt: attempt_number,
                        cause: RetryCause::DelayedResponse,
                    });
                    wait_before_retry(job.policy.retry_delay, job.deadline).await?;
                }
                Ok(reply) => return Ok(reply),
                Err(error)
                    if is_retryable_transport_error(&error)
                        && transport_retries < transport_limit =>
                {
                    transport_retries = transport_retries.saturating_add(1);
                    attempt_number = attempt_number.saturating_add(1);
                    let _ = self.events.send(LinkEvent::Retrying {
                        id: job.id,
                        attempt: attempt_number,
                        cause: RetryCause::Transport,
                    });
                }
                Err(error) => return Err(error),
            }
            if Instant::now() >= job.deadline {
                return Err(ExchangeError::Deadline);
            }
        }
    }

    async fn exchange_once(
        &mut self,
        id: u64,
        request_frame: &Frame,
        encoded: &[u8],
        deadline: Instant,
    ) -> Result<DeviceReply, ExchangeError> {
        match timeout_at(deadline, self.channel.send(encoded)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                self.channel_usable = false;
                return Err(error.into());
            }
            Err(_) => {
                self.channel_usable = false;
                return Err(ExchangeError::SendTimeout);
            }
        }
        loop {
            let read = match timeout_at(deadline, self.channel.receive(&mut self.read_buffer)).await
            {
                Ok(Ok(read)) => read,
                Ok(Err(error)) if is_receive_timeout(&error) => {
                    return Err(ExchangeError::ResponseTimeout);
                }
                Ok(Err(error)) => {
                    self.channel_usable = false;
                    return Err(error.into());
                }
                Err(_) => return Err(ExchangeError::ResponseTimeout),
            };
            if read == 0 {
                self.channel_usable = false;
                return Err(ChannelError::Closed.into());
            }
            if read > self.read_buffer.len() {
                self.channel_usable = false;
                return Err(ChannelError::Configuration(
                    "channel returned more bytes than the receive buffer can hold",
                )
                .into());
            }
            for event in self.decoder.push(&self.read_buffer[..read]) {
                let frame = match event {
                    DecodeEvent::Frame(frame) => frame,
                    DecodeEvent::Rejected(error) => {
                        let _ = self.events.send(LinkEvent::RejectedFrame { id, error });
                        continue;
                    }
                };
                if frame.kind == FrameKind::Request
                    && frame.wire_command == request_frame.wire_command
                    && frame.address == request_frame.address
                    && frame.physical_layer == request_frame.physical_layer
                    && frame.expansion == request_frame.expansion
                    && frame.payload == request_frame.payload
                {
                    let _ = self.events.send(LinkEvent::EchoDiscarded { id });
                    continue;
                }
                let Ok(reply) = DeviceReply::from_frame(frame) else {
                    continue;
                };
                if reply.burst {
                    if let Some(hub) = &self.burst_hub {
                        hub.publish(reply.clone());
                    }
                    let _ = self.events.send(LinkEvent::Unsolicited(reply));
                    continue;
                }
                if reply.command == logical_command(request_frame)
                    && reply.address == request_frame.address
                    && reply.physical_layer == request_frame.physical_layer
                    && reply.frame_expansion == request_frame.expansion
                {
                    return Ok(reply);
                }
                let _ = self.events.send(LinkEvent::StaleFrame {
                    id,
                    command: reply.command,
                });
            }
        }
    }

    fn mark_cancelled(&self, id: u64) {
        self.counters.cancelled.fetch_add(1, Ordering::Relaxed);
        let _ = self.events.send(LinkEvent::Cancelled { id });
    }

    fn mark_completed(&self, id: u64) {
        self.counters.completed.fetch_add(1, Ordering::Relaxed);
        let _ = self.events.send(LinkEvent::Completed { id });
    }

    fn mark_failed(&self, id: u64, error: &ExchangeError) {
        self.counters.failed.fetch_add(1, Ordering::Relaxed);
        let _ = self.events.send(LinkEvent::Failed {
            id,
            reason: error_reason(error),
        });
    }
}

fn resolved_safety(request: &Request) -> OperationSafety {
    request.retry_safety.unwrap_or_else(|| {
        command_descriptor(request.command).map_or(OperationSafety::Action, |entry| entry.safety)
    })
}

fn catalog_safety(request: &Request) -> Option<OperationSafety> {
    command_descriptor(request.command).map(|entry| entry.safety)
}

fn requests_can_coalesce(leader: &Job, candidate: &Job) -> bool {
    candidate.request == leader.request
        && candidate.transmit_prefix.is_empty()
        && candidate.policy == leader.policy
}

fn fail_stopped_job<C: ByteChannel>(runner: &LinkRunner<C>, job: &mut Job) {
    let Some(reply) = job.reply.take() else {
        runner.mark_cancelled(job.id);
        return;
    };
    if reply.is_closed() {
        runner.mark_cancelled(job.id);
    } else {
        runner.mark_failed(job.id, &ExchangeError::RunnerStopped);
        let _ = reply.send(Err(ExchangeError::RunnerStopped));
    }
}

fn logical_command(frame: &Frame) -> CommandCode {
    if frame.wire_command == 31 && frame.payload.len() >= 2 {
        CommandCode::new(u16::from_be_bytes([frame.payload[0], frame.payload[1]]))
    } else {
        CommandCode::from(frame.wire_command)
    }
}

async fn wait_before_retry(delay: Duration, deadline: Instant) -> Result<(), ExchangeError> {
    if delay.is_zero() {
        return Ok(());
    }
    let Some(wake) = Instant::now().checked_add(delay) else {
        return Err(ExchangeError::Deadline);
    };
    if wake >= deadline {
        return Err(ExchangeError::Deadline);
    }
    tokio::time::sleep_until(wake).await;
    Ok(())
}

fn is_retryable_transport_error(error: &ExchangeError) -> bool {
    matches!(error, ExchangeError::ResponseTimeout)
}

fn is_receive_timeout(error: &ChannelError) -> bool {
    matches!(error, ChannelError::Timeout(_))
        || matches!(error, ChannelError::Io(error) if error.kind() == std::io::ErrorKind::TimedOut)
}

const fn error_reason(error: &ExchangeError) -> &'static str {
    match error {
        ExchangeError::CommandDenied { .. } => "command policy",
        ExchangeError::UnknownQueue(_) => "unknown queue",
        ExchangeError::Deadline => "end-to-end deadline",
        ExchangeError::ResponseTimeout => "response timeout",
        ExchangeError::SendTimeout => "send timeout",
        ExchangeError::Busy(_) => "transient device rejection",
        ExchangeError::DelayedResponse(_) => "delayed response",
        ExchangeError::Channel(_) => "transport",
        ExchangeError::Wire(_) => "link-layer frame",
        ExchangeError::Operation(_) => "application format",
        ExchangeError::RunnerStopped => "runner stopped",
        ExchangeError::RetryPolicy(_) => "invalid retry policy",
        ExchangeError::TransmitPrefix(_) => "transmit prefix limit",
    }
}
