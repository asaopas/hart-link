use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU8,
    time::Duration,
};

use thiserror::Error;

use crate::{
    Address, Master,
    operation::{DeviceIdentity, Operation, ReadDeviceIdentity, Request},
    profile::LineProfile,
    service::{ExchangeError, LinkClient, Priority, RetryPolicy},
};

/// One identified device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredDevice {
    /// Profile module through which the response was received.
    pub module: u16,
    /// Physical link address.
    pub address: Address,
    /// Command 0 response.
    pub identity: DeviceIdentity,
}

/// Result of scanning a complete profile.
#[derive(Debug, Default)]
pub struct DiscoveryReport {
    /// Number of address-level discovery requests, excluding internal retries.
    pub attempts: usize,
    /// Number of first probes that used a previously learned preamble count.
    pub hinted_attempts: usize,
    /// Number of hinted probes that timed out and used the conservative fallback.
    pub hint_fallbacks: usize,
    /// Number of completed discovery passes.
    pub passes_completed: u8,
    /// Discovered devices.
    pub devices: Vec<DiscoveredDevice>,
    /// Per-address errors other than an ordinary missing response.
    pub errors: Vec<(Address, ExchangeError)>,
}

/// Maximum number of short-address preamble hints retained for both masters.
pub const MAXIMUM_DISCOVERY_HINTS: usize = 128;

/// Bounded, untrusted performance hints learned from earlier live identification.
///
/// A hint changes only the first preamble count. Discovery never treats it as
/// proof that a device exists and retries with the conservative profile value
/// when the hinted probe does not answer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DiscoveryHints {
    preambles: BTreeMap<Address, u8>,
}

impl DiscoveryHints {
    /// Creates an empty hint set.
    pub const fn new() -> Self {
        Self {
            preambles: BTreeMap::new(),
        }
    }

    /// Adds or replaces one validated short-address hint.
    pub fn insert(&mut self, address: Address, preambles: u8) -> Result<(), DiscoveryHintError> {
        if address.polling_node().is_none() {
            return Err(DiscoveryHintError::UniqueAddress);
        }
        if !(2..=40).contains(&preambles) {
            return Err(DiscoveryHintError::Preambles(preambles));
        }
        if !self.preambles.contains_key(&address) && self.preambles.len() >= MAXIMUM_DISCOVERY_HINTS
        {
            return Err(DiscoveryHintError::Limit(MAXIMUM_DISCOVERY_HINTS));
        }
        self.preambles.insert(address, preambles);
        Ok(())
    }

    /// Returns a validated hint for one short address.
    pub fn get(&self, address: Address) -> Option<u8> {
        self.preambles.get(&address).copied()
    }

    /// Returns the number of retained hints.
    pub fn len(&self) -> usize {
        self.preambles.len()
    }

    /// Reports whether no hints are retained.
    pub fn is_empty(&self) -> bool {
        self.preambles.is_empty()
    }
}

/// Invalid or resource-amplifying discovery hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DiscoveryHintError {
    /// Discovery hints are meaningful only for short polling addresses.
    #[error("discovery hint requires a short polling address")]
    UniqueAddress,
    /// A hinted preamble count is outside the practical profile range.
    #[error("discovery preamble hint {0} is outside 2..=40")]
    Preambles(u8),
    /// The hint set already covers every supported short address and master.
    #[error("discovery hint limit {0} reached")]
    Limit(usize),
}

/// Controls repeated discovery without changing the physical line profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryOptions {
    /// Maximum passes; addresses already identified are skipped in later passes.
    pub passes: NonZeroU8,
    /// Queue priority assigned to discovery requests.
    pub priority: Priority,
    /// Retry counts and delay; response and total timeouts are adapted to the line profile.
    pub retry: RetryPolicy,
    /// Delay between incomplete passes; no delay is applied after all addresses respond.
    pub pass_delay: Duration,
    /// Untrusted first-probe preamble hints with conservative fallback.
    pub preamble_hints: DiscoveryHints,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            passes: NonZeroU8::MIN,
            priority: Priority::Service,
            retry: RetryPolicy::default(),
            pass_delay: Duration::ZERO,
            preamble_hints: DiscoveryHints::new(),
        }
    }
}

impl DiscoveryOptions {
    /// Sets the maximum number of passes.
    pub const fn with_passes(mut self, passes: NonZeroU8) -> Self {
        self.passes = passes;
        self
    }

    /// Sets the queue priority of every discovery probe.
    pub const fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Sets retry counts and transient-state pacing.
    pub const fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Sets the pause before retrying addresses missing from an earlier pass.
    pub const fn with_pass_delay(mut self, pass_delay: Duration) -> Self {
        self.pass_delay = pass_delay;
        self
    }

    /// Supplies bounded hints learned from earlier live responses.
    pub fn with_preamble_hints(mut self, preamble_hints: DiscoveryHints) -> Self {
        self.preamble_hints = preamble_hints;
        self
    }
}

/// Invalid discovery or line settings.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// The physical line profile is invalid.
    #[error(transparent)]
    Profile(#[from] crate::profile::ProfileError),
    /// Retry settings cannot make progress.
    #[error(transparent)]
    Retry(#[from] crate::service::RetryPolicyError),
    /// A pass delay is too large for a practical line profile.
    #[error("discovery pass delay {0:?} exceeds the supported limit")]
    PassDelay(Duration),
}

/// Scans modules sequentially while applying confirmed address offsets.
pub async fn discover_line(
    link: &LinkClient,
    profile: &LineProfile,
    master: Master,
) -> Result<DiscoveryReport, crate::profile::ProfileError> {
    profile.validate()?;
    Ok(discover_validated(link, profile, master, DiscoveryOptions::default()).await)
}

/// Scans a line repeatedly with explicit priority and retry settings.
pub async fn discover_line_with_options(
    link: &LinkClient,
    profile: &LineProfile,
    master: Master,
    options: DiscoveryOptions,
) -> Result<DiscoveryReport, DiscoveryError> {
    profile.validate()?;
    options.retry.validate()?;
    if options.pass_delay > crate::profile::MAXIMUM_PROFILE_DURATION {
        return Err(DiscoveryError::PassDelay(options.pass_delay));
    }
    Ok(discover_validated(link, profile, master, options).await)
}

async fn discover_validated(
    link: &LinkClient,
    profile: &LineProfile,
    master: Master,
    options: DiscoveryOptions,
) -> DiscoveryReport {
    if !profile.timing.connect_settle.is_zero() {
        tokio::time::sleep(profile.timing.connect_settle).await;
    }
    let mut report = DiscoveryReport::default();
    let mut wake_prefix = Some(profile.wake_prefix.clone());
    for pass in 0..options.passes.get() {
        let mut visited = BTreeSet::new();
        let mut previous_probe = false;
        for module in &profile.modules {
            for local in module.polling.iter() {
                let Some(node) = LineProfile::shifted_address(*module, local) else {
                    continue;
                };
                let Ok(address) = Address::polling(node, master) else {
                    continue;
                };
                if !visited.insert(address)
                    || report
                        .devices
                        .iter()
                        .any(|device| device.address == address)
                {
                    continue;
                }
                if previous_probe && !profile.timing.scan_interval.is_zero() {
                    tokio::time::sleep(profile.timing.scan_interval).await;
                }
                previous_probe = true;
                let conservative = profile.discovery_preambles;
                let hinted = (pass == 0)
                    .then(|| options.preamble_hints.get(address))
                    .flatten()
                    .filter(|preambles| *preambles < conservative);
                let first = hinted.unwrap_or(conservative);
                for (probe, preambles) in [first, conservative].into_iter().enumerate() {
                    if probe == 1 && hinted.is_none() {
                        break;
                    }
                    if probe == 0 && hinted.is_some() {
                        report.hinted_attempts += 1;
                    }
                    report.attempts += 1;
                    let response_timeout = if hinted.is_some() && probe == 0 {
                        profile.timing.steady_response
                    } else if report.devices.is_empty() {
                        profile.timing.first_response
                    } else {
                        profile.timing.steady_response
                    };
                    let retry = discovery_retry(
                        options.retry,
                        response_timeout,
                        probe != 0 || hinted.is_none(),
                    );
                    let request = Request::new(address, 0u8, Vec::new()).with_preambles(preambles);
                    let prefix = wake_prefix.take().unwrap_or_default();
                    match link
                        .request_with_prefix(request, prefix, options.priority, retry)
                        .await
                    {
                        Ok(reply) => {
                            match ReadDeviceIdentity.decode_reply(&reply) {
                                Ok(identity) => report.devices.push(DiscoveredDevice {
                                    module: module.module,
                                    address,
                                    identity,
                                }),
                                Err(error) => report
                                    .errors
                                    .push((address, ExchangeError::Operation(error))),
                            }
                            break;
                        }
                        Err(ExchangeError::ResponseTimeout | ExchangeError::Deadline)
                            if hinted.is_some() && probe == 0 =>
                        {
                            report.hint_fallbacks += 1;
                        }
                        Err(ExchangeError::ResponseTimeout | ExchangeError::Deadline) => break,
                        Err(error) => {
                            report.errors.push((address, error));
                            break;
                        }
                    }
                }
            }
        }
        report.passes_completed = pass.saturating_add(1);
        if report.devices.len() >= visited.len() {
            break;
        }
        if pass.saturating_add(1) < options.passes.get() && !options.pass_delay.is_zero() {
            tokio::time::sleep(options.pass_delay).await;
        }
    }
    report
}

fn discovery_retry(
    base: RetryPolicy,
    response_timeout: Duration,
    allow_transport_retries: bool,
) -> RetryPolicy {
    let transport_retries = if allow_transport_retries {
        base.transport_retries
    } else {
        0
    };
    let maximum_attempts = 1_u32
        .saturating_add(u32::from(transport_retries))
        .saturating_add(u32::from(base.busy_retries))
        .saturating_add(u32::from(base.delayed_response_polls));
    let transient_waits =
        u32::from(base.busy_retries).saturating_add(u32::from(base.delayed_response_polls));
    RetryPolicy {
        transport_retries,
        response_timeout,
        total_timeout: response_timeout
            .saturating_mul(maximum_attempts)
            .saturating_add(base.retry_delay.saturating_mul(transient_waits)),
        ..base
    }
}
