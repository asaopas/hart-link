//! Validated settings for a physical link and transparent gateway.

use std::{collections::BTreeMap, time::Duration};

use thiserror::Error;

/// Largest retained link-profile name.
pub const MAXIMUM_PROFILE_NAME_BYTES: usize = 256;
/// Largest possible number of non-overlapping translated short-address modules.
pub const MAXIMUM_PROFILE_MODULES: usize = 64;
/// Largest accepted individual link-profile duration.
pub const MAXIMUM_PROFILE_DURATION: Duration = Duration::from_hours(24);
/// Largest gateway wake-up prefix retained by a profile.
pub const MAXIMUM_PROFILE_WAKE_PREFIX: usize = 256;
/// Largest possible number of per-address timing overrides on one HART line.
pub const MAXIMUM_ADDRESS_TIMING_OVERRIDES: usize = 64;

/// Short-address range for one discovery segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PollingWindow {
    /// Inclusive first address.
    pub first: u8,
    /// Inclusive last address.
    pub last: u8,
}

impl PollingWindow {
    /// Creates a validated range within `0..=63`.
    pub fn new(first: u8, last: u8) -> Result<Self, ProfileError> {
        if first > last || last > 63 {
            return Err(ProfileError::PollingWindow { first, last });
        }
        Ok(Self { first, last })
    }

    /// Iterates over addresses in the range.
    pub fn iter(self) -> core::ops::RangeInclusive<u8> {
        self.first..=self.last
    }
}

/// Address window of one physical multiplexer module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ModuleWindow {
    /// Module number used in reports.
    pub module: u16,
    /// Addresses accessible through the module.
    pub polling: PollingWindow,
    /// Offset applied by the gateway to local addresses.
    pub address_shift: i8,
}

impl ModuleWindow {
    /// Creates an unshifted module window.
    pub const fn new(module: u16, polling: PollingWindow) -> Self {
        Self {
            module,
            polling,
            address_shift: 0,
        }
    }

    /// Applies a confirmed gateway address translation.
    pub const fn with_address_shift(mut self, address_shift: i8) -> Self {
        self.address_shift = address_shift;
        self
    }
}

/// Timing policy for initial and subsequent requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimingProfile {
    /// Delay after opening a TCP or serial channel.
    pub connect_settle: Duration,
    /// Timeout for the first device response.
    pub first_response: Duration,
    /// Timeout after successful identification.
    pub steady_response: Duration,
    /// Delay between addresses during discovery.
    pub scan_interval: Duration,
}

impl Default for TimingProfile {
    fn default() -> Self {
        Self {
            connect_settle: Duration::from_millis(500),
            first_response: Duration::from_secs(5),
            steady_response: Duration::from_millis(1600),
            scan_interval: Duration::from_millis(100),
        }
    }
}

impl TimingProfile {
    /// Validates response progress and rejects impractically large durations.
    pub fn validate(self) -> Result<(), ProfileError> {
        if self.first_response.is_zero() || self.steady_response.is_zero() {
            return Err(ProfileError::ZeroTimeout);
        }
        for duration in [
            self.connect_settle,
            self.first_response,
            self.steady_response,
            self.scan_interval,
        ] {
            if duration > MAXIMUM_PROFILE_DURATION {
                return Err(ProfileError::Duration(duration));
            }
        }
        Ok(())
    }

    /// Sets the delay after transport connection.
    pub const fn with_connect_settle(mut self, duration: Duration) -> Self {
        self.connect_settle = duration;
        self
    }

    /// Sets the response timeout used before any device is identified.
    pub const fn with_first_response(mut self, duration: Duration) -> Self {
        self.first_response = duration;
        self
    }

    /// Sets the response timeout used after the line has responded.
    pub const fn with_steady_response(mut self, duration: Duration) -> Self {
        self.steady_response = duration;
        self
    }

    /// Sets the pacing delay between discovery probes.
    pub const fn with_scan_interval(mut self, duration: Duration) -> Self {
        self.scan_interval = duration;
        self
    }
}

/// Optional discovery timing changes for one physical polling address.
///
/// The delay is applied before probing this address on every discovery pass.
/// A response timeout, when present, replaces both the initial and steady
/// line-wide response timeout only for this address.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AddressTiming {
    /// Additional delay before transmitting Command 0 to this address.
    pub delay_before_probe: Duration,
    /// Address-specific response timeout, or the line-wide timeout when absent.
    pub response_timeout: Option<Duration>,
}

impl AddressTiming {
    /// Creates an override that initially leaves the line-wide timing unchanged.
    pub const fn new() -> Self {
        Self {
            delay_before_probe: Duration::ZERO,
            response_timeout: None,
        }
    }

    /// Sets an additional delay before probing the address.
    pub const fn with_delay_before_probe(mut self, delay: Duration) -> Self {
        self.delay_before_probe = delay;
        self
    }

    /// Replaces the response timeout for this address.
    pub const fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = Some(timeout);
        self
    }

    fn validate(self) -> Result<(), AddressTimingError> {
        if self
            .response_timeout
            .is_some_and(|timeout| timeout.is_zero())
        {
            return Err(AddressTimingError::ZeroTimeout);
        }
        for duration in [
            self.delay_before_probe,
            self.response_timeout.unwrap_or_default(),
        ] {
            if duration > MAXIMUM_PROFILE_DURATION {
                return Err(AddressTimingError::Duration(duration));
            }
        }
        Ok(())
    }
}

/// Invalid per-address discovery timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AddressTimingError {
    /// The polling address is outside the short-address range.
    #[error("polling address {0} is outside 0..=63")]
    PollingAddress(u8),
    /// A configured response timeout is zero.
    #[error("address-specific response timeout cannot be zero")]
    ZeroTimeout,
    /// A timing value is too large for a bounded discovery campaign.
    #[error("address-specific duration {0:?} exceeds the supported maximum")]
    Duration(Duration),
}

/// Validated per-address discovery timing overrides.
///
/// Keys are physical polling addresses after applying a module address shift.
/// Missing addresses continue to use [`TimingProfile`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AddressTimings {
    values: BTreeMap<u8, AddressTiming>,
}

impl AddressTimings {
    /// Creates an empty set that does not change discovery timing.
    pub const fn new() -> Self {
        Self {
            values: BTreeMap::new(),
        }
    }

    /// Adds or replaces timing for one physical polling address.
    pub fn insert(
        &mut self,
        polling_address: u8,
        timing: AddressTiming,
    ) -> Result<(), AddressTimingError> {
        if usize::from(polling_address) >= MAXIMUM_ADDRESS_TIMING_OVERRIDES {
            return Err(AddressTimingError::PollingAddress(polling_address));
        }
        timing.validate()?;
        self.values.insert(polling_address, timing);
        Ok(())
    }

    /// Adds one override and returns the updated set.
    pub fn with(
        mut self,
        polling_address: u8,
        timing: AddressTiming,
    ) -> Result<Self, AddressTimingError> {
        self.insert(polling_address, timing)?;
        Ok(self)
    }

    /// Returns timing configured for one physical polling address.
    pub fn get(&self, polling_address: u8) -> Option<AddressTiming> {
        self.values.get(&polling_address).copied()
    }

    /// Iterates over physical polling addresses and their timing overrides.
    pub fn iter(&self) -> impl Iterator<Item = (u8, AddressTiming)> + '_ {
        self.values
            .iter()
            .map(|(&polling_address, &timing)| (polling_address, timing))
    }

    /// Returns the number of overridden polling addresses.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Reports whether line-wide timing is used for every address.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Complete profile of one physical link.
#[derive(Debug, Clone)]
pub struct LineProfile {
    /// Local link name.
    pub name: String,
    /// Timing settings.
    pub timing: TimingProfile,
    /// Order in which address windows are scanned.
    pub modules: Vec<ModuleWindow>,
    /// Preamble count used before Command 0 succeeds.
    pub discovery_preambles: u8,
    /// Additional wake-up bytes sent to a transparent gateway.
    pub wake_prefix: Vec<u8>,
}

impl LineProfile {
    /// Starts a profile with default timings and no modules.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            timing: TimingProfile::default(),
            modules: Vec::new(),
            discovery_preambles: 20,
            wake_prefix: Vec::new(),
        }
    }

    /// Creates a common single-segment profile without address translation.
    pub fn single_segment(name: impl Into<String>, polling: PollingWindow) -> Self {
        Self::new(name).with_module(ModuleWindow::new(1, polling))
    }

    /// Replaces timing settings.
    pub const fn with_timing(mut self, timing: TimingProfile) -> Self {
        self.timing = timing;
        self
    }

    /// Appends one gateway or multiplexer module.
    pub fn with_module(mut self, module: ModuleWindow) -> Self {
        self.modules.push(module);
        self
    }

    /// Replaces every gateway or multiplexer module.
    pub fn with_modules(mut self, modules: impl Into<Vec<ModuleWindow>>) -> Self {
        self.modules = modules.into();
        self
    }

    /// Sets the conservative preamble count used before identification.
    pub const fn with_discovery_preambles(mut self, preambles: u8) -> Self {
        self.discovery_preambles = preambles;
        self
    }

    /// Sets bytes transmitted once before the first discovery request.
    pub fn with_wake_prefix(mut self, prefix: impl Into<Vec<u8>>) -> Self {
        self.wake_prefix = prefix.into();
        self
    }

    /// Validates ranges, overlaps, and practical timing limits.
    pub fn validate(&self) -> Result<(), ProfileError> {
        if self.name.trim().is_empty() {
            return Err(ProfileError::EmptyName);
        }
        if self.name.len() > MAXIMUM_PROFILE_NAME_BYTES {
            return Err(ProfileError::NameLength(self.name.len()));
        }
        if !(2..=40).contains(&self.discovery_preambles) {
            return Err(ProfileError::Preambles(self.discovery_preambles));
        }
        if self.wake_prefix.len() > MAXIMUM_PROFILE_WAKE_PREFIX {
            return Err(ProfileError::WakePrefix(self.wake_prefix.len()));
        }
        self.timing.validate()?;
        if self.modules.is_empty() {
            return Err(ProfileError::NoModules);
        }
        if self.modules.len() > MAXIMUM_PROFILE_MODULES {
            return Err(ProfileError::ModuleLimit(self.modules.len()));
        }
        let mut physical_addresses = BTreeMap::new();
        for (index, current) in self.modules.iter().enumerate() {
            PollingWindow::new(current.polling.first, current.polling.last)?;
            if self.modules[index + 1..]
                .iter()
                .any(|other| other.module == current.module)
            {
                return Err(ProfileError::DuplicateModule(current.module));
            }
            let mut reachable = false;
            for local in current.polling.iter() {
                let Some(address) = Self::shifted_address(*current, local) else {
                    continue;
                };
                reachable = true;
                if let Some(previous) = physical_addresses.insert(address, current.module) {
                    return Err(ProfileError::AddressOverlap {
                        address,
                        first_module: previous,
                        second_module: current.module,
                    });
                }
            }
            if !reachable {
                return Err(ProfileError::UnreachableModule(current.module));
            }
        }
        Ok(())
    }

    /// Returns the physical address after applying a confirmed module offset.
    pub fn shifted_address(module: ModuleWindow, local: u8) -> Option<u8> {
        if !module.polling.iter().contains(&local) {
            return None;
        }
        let shifted = i16::from(local) + i16::from(module.address_shift);
        u8::try_from(shifted).ok().filter(|value| *value <= 63)
    }
}

/// Link-profile error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProfileError {
    /// The name is empty.
    #[error("link name cannot be empty")]
    EmptyName,
    /// The retained profile name is implausibly large.
    #[error("link name contains {0} UTF-8 bytes")]
    NameLength(usize),
    /// A short-address range is invalid.
    #[error("invalid address range {first}..={last}")]
    PollingWindow {
        /// Range start.
        first: u8,
        /// Range end.
        last: u8,
    },
    /// The preamble count is outside the practical limit.
    #[error("preamble count {0} is outside 2..=40")]
    Preambles(u8),
    /// The wake-up prefix is too large.
    #[error("{0}-byte wake-up prefix is too large")]
    WakePrefix(usize),
    /// A required timeout is zero.
    #[error("timeout cannot be zero")]
    ZeroTimeout,
    /// A timing value is too large for a bounded link campaign.
    #[error("profile duration {0:?} exceeds the supported maximum")]
    Duration(Duration),
    /// A profile must expose at least one discovery segment.
    #[error("link profile has no modules")]
    NoModules,
    /// A line cannot expose more non-overlapping translated short addresses.
    #[error("link profile contains too many modules: {0}")]
    ModuleLimit(usize),
    /// A module number is duplicated.
    #[error("module {0} is declared more than once")]
    DuplicateModule(u16),
    /// Address translation moved the complete window outside `0..=63`.
    #[error("module {0} does not map to any valid polling address")]
    UnreachableModule(u16),
    /// Two declared modules map to the same physical polling address.
    #[error(
        "polling address {address} is produced by both module {first_module} and module {second_module}"
    )]
    AddressOverlap {
        /// Conflicting physical address.
        address: u8,
        /// First module in declaration order.
        first_module: u16,
        /// Later conflicting module.
        second_module: u16,
    },
}
