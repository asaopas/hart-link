use std::{
    num::NonZeroU8,
    sync::{Arc, Mutex},
    time::Duration,
};

use thiserror::Error;
use tokio::time::Instant;

use crate::{
    CommandCode, Operation, OperationSafety, command_descriptor,
    service::{
        DeviceSession, ExchangeError, MAXIMUM_RETRY_DURATION, QueueId, RetryPolicy, SessionError,
    },
};

/// Bounds used to derive a shorter read-only response timeout from successful exchanges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptiveTiming {
    /// Smallest response timeout selected by adaptation.
    pub minimum: Duration,
    /// Largest response timeout selected by adaptation.
    pub maximum: Duration,
    /// Safety margin added to the learned response duration.
    pub margin: Duration,
}

impl Default for AdaptiveTiming {
    fn default() -> Self {
        Self {
            minimum: Duration::from_millis(100),
            maximum: Duration::from_secs(5),
            margin: Duration::from_millis(50),
        }
    }
}

impl AdaptiveTiming {
    /// Validates progress and hard resource bounds.
    pub fn validate(self) -> Result<(), DeviceHealthConfigError> {
        if self.minimum.is_zero() || self.maximum.is_zero() {
            return Err(DeviceHealthConfigError::ZeroAdaptiveTimeout);
        }
        if self.minimum > self.maximum {
            return Err(DeviceHealthConfigError::AdaptiveOrder);
        }
        if self.maximum > MAXIMUM_RETRY_DURATION || self.margin > MAXIMUM_RETRY_DURATION {
            return Err(DeviceHealthConfigError::AdaptiveLimit);
        }
        Ok(())
    }

    /// Sets the smallest adaptive response timeout.
    pub const fn with_minimum(mut self, minimum: Duration) -> Self {
        self.minimum = minimum;
        self
    }

    /// Sets the largest adaptive response timeout.
    pub const fn with_maximum(mut self, maximum: Duration) -> Self {
        self.maximum = maximum;
        self
    }

    /// Sets the margin added to a learned duration.
    pub const fn with_margin(mut self, margin: Duration) -> Self {
        self.margin = margin;
        self
    }
}

/// Health, cooldown, and optional read-only adaptation settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceHealthOptions {
    /// Consecutive transport failures required before cooldown begins.
    pub failure_threshold: NonZeroU8,
    /// Period during which ordinary calls fail locally without occupying the link queue.
    pub cooldown: Duration,
    /// Read-only adaptive timing, or `None` to keep caller timeouts unchanged.
    pub adaptive: Option<AdaptiveTiming>,
}

impl Default for DeviceHealthOptions {
    fn default() -> Self {
        Self {
            failure_threshold: NonZeroU8::new(3).unwrap_or(NonZeroU8::MIN),
            cooldown: Duration::from_secs(10),
            adaptive: Some(AdaptiveTiming::default()),
        }
    }
}

impl DeviceHealthOptions {
    /// Validates every duration before a managed session is created.
    pub fn validate(self) -> Result<(), DeviceHealthConfigError> {
        if self.cooldown > MAXIMUM_RETRY_DURATION {
            return Err(DeviceHealthConfigError::CooldownLimit);
        }
        if let Some(adaptive) = self.adaptive {
            adaptive.validate()?;
        }
        Ok(())
    }

    /// Sets the number of consecutive transport failures that opens cooldown.
    pub const fn with_failure_threshold(mut self, failure_threshold: NonZeroU8) -> Self {
        self.failure_threshold = failure_threshold;
        self
    }

    /// Sets cooldown duration; zero disables the waiting period while preserving counters.
    pub const fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Enables or disables adaptive read-only timing.
    pub const fn with_adaptive(mut self, adaptive: Option<AdaptiveTiming>) -> Self {
        self.adaptive = adaptive;
        self
    }
}

/// Invalid health or adaptive timing configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DeviceHealthConfigError {
    /// Adaptive timing cannot make progress with a zero bound.
    #[error("adaptive response timeouts must be greater than zero")]
    ZeroAdaptiveTimeout,
    /// The minimum adaptive timeout is greater than its maximum.
    #[error("adaptive minimum timeout exceeds its maximum")]
    AdaptiveOrder,
    /// An adaptive duration exceeds the global per-request safety bound.
    #[error("adaptive timing exceeds the supported 24-hour limit")]
    AdaptiveLimit,
    /// Cooldown exceeds the global per-request safety bound.
    #[error("device cooldown exceeds the supported 24-hour limit")]
    CooldownLimit,
}

/// Point-in-time summary of one managed device session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeviceHealthSnapshot {
    /// Logical operations completed successfully.
    pub successes: u64,
    /// Logical operations that returned a final error.
    pub failures: u64,
    /// Consecutive failures that indicate a transport outage.
    pub consecutive_transport_failures: u32,
    /// Number of times the failure threshold opened cooldown.
    pub cooldown_activations: u64,
    /// Calls rejected locally while cooldown was active.
    pub cooldown_rejections: u64,
    /// Remaining cooldown duration.
    pub cooldown_remaining: Duration,
    /// Smoothed successful duration used for read-only adaptation.
    pub learned_response: Option<Duration>,
    /// Read-only operations started with a learned short timeout.
    pub adaptive_attempts: u64,
    /// Adaptive attempts that timed out and used their conservative retry budget.
    pub adaptive_fallbacks: u64,
}

#[derive(Debug, Default)]
struct HealthState {
    successes: u64,
    failures: u64,
    consecutive_transport_failures: u32,
    cooldown_activations: u64,
    cooldown_rejections: u64,
    cooldown_until: Option<Instant>,
    learned_response: Option<Duration>,
    adaptive_attempts: u64,
    adaptive_fallbacks: u64,
}

/// Error returned by a managed device operation.
#[derive(Debug, Error)]
pub enum ManagedSessionError {
    /// Cooldown rejected an ordinary call before it occupied queue capacity.
    #[error("device is cooling down for another {remaining:?}")]
    CoolingDown {
        /// Remaining wait at the time of rejection.
        remaining: Duration,
    },
    /// Only a command registered by the library as read-only may bypass cooldown.
    #[error("command {command} is not a registered read-only probe")]
    UnsafeProbe {
        /// Rejected logical command number.
        command: u16,
    },
    /// Device-session or exchange failure.
    #[error(transparent)]
    Session(#[from] SessionError),
}

/// Device session with bounded outage suppression and conservative read-only adaptation.
#[derive(Debug, Clone)]
pub struct ManagedDeviceSession {
    session: DeviceSession,
    options: DeviceHealthOptions,
    state: Arc<Mutex<HealthState>>,
}

impl ManagedDeviceSession {
    /// Creates a managed session after validating all limits.
    pub fn new(
        session: DeviceSession,
        options: DeviceHealthOptions,
    ) -> Result<Self, DeviceHealthConfigError> {
        options.validate()?;
        Ok(Self::from_valid_options(session, options))
    }

    /// Creates a managed session with conservative defaults.
    pub fn with_defaults(session: DeviceSession) -> Self {
        Self::from_valid_options(session, DeviceHealthOptions::default())
    }

    fn from_valid_options(session: DeviceSession, options: DeviceHealthOptions) -> Self {
        Self {
            session,
            options,
            state: Arc::new(Mutex::new(HealthState::default())),
        }
    }

    /// Returns the underlying identified session.
    pub const fn session(&self) -> &DeviceSession {
        &self.session
    }

    /// Returns validated health settings.
    pub const fn options(&self) -> DeviceHealthOptions {
        self.options
    }

    /// Returns current health counters and remaining cooldown.
    pub fn snapshot(&self) -> DeviceHealthSnapshot {
        let now = Instant::now();
        let mut state = self.lock_state();
        let remaining = state
            .cooldown_until
            .and_then(|until| until.checked_duration_since(now))
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            state.cooldown_until = None;
        }
        DeviceHealthSnapshot {
            successes: state.successes,
            failures: state.failures,
            consecutive_transport_failures: state.consecutive_transport_failures,
            cooldown_activations: state.cooldown_activations,
            cooldown_rejections: state.cooldown_rejections,
            cooldown_remaining: remaining,
            learned_response: state.learned_response,
            adaptive_attempts: state.adaptive_attempts,
            adaptive_fallbacks: state.adaptive_fallbacks,
        }
    }

    /// Executes an operation unless cooldown is active.
    pub async fn execute<O: Operation>(
        &self,
        operation: &O,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<O::Output, ManagedSessionError> {
        self.execute_inner(operation, queue, policy, false).await
    }

    /// Executes with the defaults configured on the shared link.
    pub async fn execute_default<O: Operation>(
        &self,
        operation: &O,
    ) -> Result<O::Output, ManagedSessionError> {
        self.execute(
            operation,
            self.session.link().default_queue_id(),
            self.session.link().default_retry(),
        )
        .await
    }

    /// Bypasses cooldown for a command registered by the library as read-only.
    pub async fn probe<O: Operation>(
        &self,
        operation: &O,
        queue: QueueId,
        policy: RetryPolicy,
    ) -> Result<O::Output, ManagedSessionError> {
        if !registered_read_only(operation.command()) {
            return Err(ManagedSessionError::UnsafeProbe {
                command: operation.command().get(),
            });
        }
        self.execute_inner(operation, queue, policy, true).await
    }

    async fn execute_inner<O: Operation>(
        &self,
        operation: &O,
        queue: QueueId,
        policy: RetryPolicy,
        bypass_cooldown: bool,
    ) -> Result<O::Output, ManagedSessionError> {
        self.check_cooldown(bypass_cooldown)?;
        let logical_started = Instant::now();
        let Some(fast_timeout) = self.adaptive_timeout(operation.command(), policy) else {
            let attempt_started = Instant::now();
            let result = self.session.execute(operation, queue, policy).await;
            return self.finish(result, attempt_started.elapsed(), operation.command());
        };

        {
            let mut state = self.lock_state();
            state.adaptive_attempts = state.adaptive_attempts.saturating_add(1);
        }
        let fast_policy = RetryPolicy {
            transport_retries: 0,
            response_timeout: fast_timeout,
            ..policy
        };
        let fast_started = Instant::now();
        match self.session.execute(operation, queue, fast_policy).await {
            Ok(value) => {
                self.record_success(fast_started.elapsed(), operation.command());
                Ok(value)
            }
            Err(error) if is_response_timeout(&error) => {
                let Some(remaining) = policy.total_timeout.checked_sub(logical_started.elapsed())
                else {
                    self.record_failure(&error);
                    return Err(error.into());
                };
                if remaining.is_zero() {
                    self.record_failure(&error);
                    return Err(error.into());
                }
                {
                    let mut state = self.lock_state();
                    state.adaptive_fallbacks = state.adaptive_fallbacks.saturating_add(1);
                }
                let fallback = RetryPolicy {
                    transport_retries: policy.transport_retries.saturating_sub(1),
                    response_timeout: policy.response_timeout.min(remaining),
                    total_timeout: remaining,
                    ..policy
                };
                let fallback_started = Instant::now();
                let result = self.session.execute(operation, queue, fallback).await;
                self.finish(result, fallback_started.elapsed(), operation.command())
            }
            Err(error) => {
                self.record_failure(&error);
                Err(error.into())
            }
        }
    }

    fn adaptive_timeout(&self, command: CommandCode, policy: RetryPolicy) -> Option<Duration> {
        if policy.transport_retries == 0 || !registered_read_only(command) {
            return None;
        }
        let adaptive = self.options.adaptive?;
        let learned = self.lock_state().learned_response?;
        let candidate = learned
            .saturating_add(adaptive.margin)
            .clamp(adaptive.minimum, adaptive.maximum)
            .min(policy.response_timeout);
        (candidate < policy.response_timeout).then_some(candidate)
    }

    fn check_cooldown(&self, bypass: bool) -> Result<(), ManagedSessionError> {
        if bypass {
            return Ok(());
        }
        let now = Instant::now();
        let mut state = self.lock_state();
        let Some(until) = state.cooldown_until else {
            return Ok(());
        };
        let Some(remaining) = until.checked_duration_since(now) else {
            state.cooldown_until = None;
            return Ok(());
        };
        if remaining.is_zero() {
            state.cooldown_until = None;
            return Ok(());
        }
        state.cooldown_rejections = state.cooldown_rejections.saturating_add(1);
        Err(ManagedSessionError::CoolingDown { remaining })
    }

    fn finish<T>(
        &self,
        result: Result<T, SessionError>,
        elapsed: Duration,
        command: CommandCode,
    ) -> Result<T, ManagedSessionError> {
        match result {
            Ok(value) => {
                self.record_success(elapsed, command);
                Ok(value)
            }
            Err(error) => {
                self.record_failure(&error);
                Err(error.into())
            }
        }
    }

    fn record_success(&self, elapsed: Duration, command: CommandCode) {
        let mut state = self.lock_state();
        state.successes = state.successes.saturating_add(1);
        state.consecutive_transport_failures = 0;
        state.cooldown_until = None;
        if registered_read_only(command) && self.options.adaptive.is_some() {
            state.learned_response = Some(smoothed_duration(state.learned_response, elapsed));
        }
    }

    fn record_failure(&self, error: &SessionError) {
        let mut state = self.lock_state();
        state.failures = state.failures.saturating_add(1);
        if is_transport_outage(error) {
            state.consecutive_transport_failures =
                state.consecutive_transport_failures.saturating_add(1);
            if state.consecutive_transport_failures
                >= u32::from(self.options.failure_threshold.get())
            {
                let now = Instant::now();
                let already_active = state.cooldown_until.is_some_and(|until| until > now);
                if !already_active {
                    state.cooldown_activations = state.cooldown_activations.saturating_add(1);
                }
                state.cooldown_until = now.checked_add(self.options.cooldown);
            }
        } else {
            state.consecutive_transport_failures = 0;
            state.cooldown_until = None;
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, HealthState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn registered_read_only(command: CommandCode) -> bool {
    command_descriptor(command).is_some_and(|entry| entry.safety == OperationSafety::ReadOnly)
}

fn is_response_timeout(error: &SessionError) -> bool {
    matches!(
        error,
        SessionError::Exchange(ExchangeError::ResponseTimeout)
    )
}

fn is_transport_outage(error: &SessionError) -> bool {
    matches!(
        error,
        SessionError::Exchange(
            ExchangeError::ResponseTimeout
                | ExchangeError::SendTimeout
                | ExchangeError::Channel(_)
                | ExchangeError::RunnerStopped
        )
    )
}

fn smoothed_duration(previous: Option<Duration>, sample: Duration) -> Duration {
    let Some(previous) = previous else {
        return sample;
    };
    let nanoseconds = previous
        .as_nanos()
        .saturating_mul(3)
        .saturating_add(sample.as_nanos())
        / 4;
    Duration::from_nanos(u64::try_from(nanoseconds).unwrap_or(u64::MAX))
}
