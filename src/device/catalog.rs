use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{device::DataSchema, operation::CommandCode};

/// Default upper bound for profiles retained by one catalog.
pub const DEFAULT_MAXIMUM_CATALOG_PROFILES: usize = 4096;
/// Default upper bound for one directly loaded JSON profile.
pub const DEFAULT_MAXIMUM_CATALOG_JSON_BYTES: usize = 8 * 1024 * 1024;
/// Hard upper bound for configured exact device revisions.
pub const MAXIMUM_CATALOG_PROFILES: usize = 65_536;
/// Hard upper bound for one direct JSON profile input.
pub const MAXIMUM_CATALOG_JSON_BYTES: usize = 64 * 1024 * 1024;
/// Largest UTF-8 byte length accepted for profile identity text.
pub const MAXIMUM_PROFILE_TEXT_BYTES: usize = 1024;
/// Largest number of command entries accepted in one exact device revision.
pub const MAXIMUM_PROFILE_COMMANDS: usize = 4096;

/// Resource limits for direct DeviceInfo-style catalog loading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogLimits {
    /// Maximum exact device revisions retained at once.
    pub profiles: usize,
    /// Maximum bytes accepted by one [`DeviceCatalog::load_json`] call.
    pub json_bytes: usize,
}

impl Default for CatalogLimits {
    fn default() -> Self {
        Self {
            profiles: DEFAULT_MAXIMUM_CATALOG_PROFILES,
            json_bytes: DEFAULT_MAXIMUM_CATALOG_JSON_BYTES,
        }
    }
}

impl CatalogLimits {
    /// Sets the exact device-revision count limit.
    pub const fn with_profiles(mut self, profiles: usize) -> Self {
        self.profiles = profiles;
        self
    }

    /// Sets the direct JSON input byte limit.
    pub const fn with_json_bytes(mut self, json_bytes: usize) -> Self {
        self.json_bytes = json_bytes;
        self
    }

    /// Validates strict catalog resource limits.
    pub const fn validate(self) -> Result<(), CatalogLoadError> {
        if self.profiles == 0 || self.json_bytes == 0 {
            return Err(CatalogLoadError::ZeroLimit);
        }
        if self.profiles > MAXIMUM_CATALOG_PROFILES {
            return Err(CatalogLoadError::ProfileConfigurationLimit(self.profiles));
        }
        if self.json_bytes > MAXIMUM_CATALOG_JSON_BYTES {
            return Err(CatalogLoadError::JsonConfigurationLimit(self.json_bytes));
        }
        Ok(())
    }
}

/// Exact device-revision key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeviceKey {
    /// Expanded device type.
    pub expanded_type: u16,
    /// Device revision.
    pub device_revision: u8,
}

impl DeviceKey {
    /// Creates an exact device key after checking the 14-bit expanded type.
    pub const fn new(
        expanded_type: u16,
        device_revision: u8,
    ) -> Result<Self, crate::device::SchemaError> {
        if expanded_type > 0x3fff {
            return Err(crate::device::SchemaError::ExpandedDeviceType(
                expanded_type,
            ));
        }
        Ok(Self {
            expanded_type,
            device_revision,
        })
    }
}

/// Command-specific status assigned by the exact DeviceInfo revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseCodeStatus {
    /// The command completed exactly as requested.
    Success,
    /// The command completed with an adjustment and response data remains meaningful.
    Warning,
    /// The command did not complete and response data must not be decoded as success data.
    Error,
}

/// Meaning of one response code for one command and device revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseCodeDefinition {
    /// Execution outcome described by DeviceInfo.
    pub status: ResponseCodeStatus,
    /// Human-readable meaning supplied by the source profile.
    pub description: String,
}

impl ResponseCodeDefinition {
    /// Creates one command-specific response-code meaning.
    pub fn new(status: ResponseCodeStatus, description: impl Into<String>) -> Self {
        Self {
            status,
            description: description.into(),
        }
    }
}

/// Schema set for one device type and revision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceProfile {
    /// Profile-selection key.
    pub key: DeviceKey,
    /// Human-readable manufacturer and model name.
    pub display_name: String,
    /// Revision of the source description.
    pub source_revision: String,
    /// Response schemas indexed by logical command number.
    pub responses: BTreeMap<u16, DataSchema>,
    /// Command-specific response-code meanings indexed by command and code.
    #[serde(default)]
    pub response_codes: BTreeMap<u16, BTreeMap<u8, ResponseCodeDefinition>>,
}

impl DeviceProfile {
    /// Creates an empty exact-revision profile.
    pub fn new(
        key: DeviceKey,
        display_name: impl Into<String>,
        source_revision: impl Into<String>,
    ) -> Self {
        Self {
            key,
            display_name: display_name.into(),
            source_revision: source_revision.into(),
            responses: BTreeMap::new(),
            response_codes: BTreeMap::new(),
        }
    }

    /// Adds or replaces one response schema.
    pub fn with_response(mut self, command: impl Into<CommandCode>, schema: DataSchema) -> Self {
        self.responses.insert(command.into().get(), schema);
        self
    }

    /// Adds or replaces one command-specific response-code meaning.
    pub fn with_response_code(
        mut self,
        command: impl Into<CommandCode>,
        code: u8,
        definition: ResponseCodeDefinition,
    ) -> Self {
        self.response_codes
            .entry(command.into().get())
            .or_default()
            .insert(code, definition);
        self
    }

    /// Returns the schema for a specific response.
    pub fn response(&self, command: CommandCode) -> Option<&DataSchema> {
        self.responses.get(&command.get())
    }

    /// Returns the exact DeviceInfo meaning of one command response code.
    pub fn response_code(&self, command: CommandCode, code: u8) -> Option<&ResponseCodeDefinition> {
        self.response_codes.get(&command.get())?.get(&code)
    }

    /// Returns warning codes that may be passed to
    /// [`crate::Operation::decode_reply_accepting`].
    pub fn warning_codes(&self, command: CommandCode) -> Vec<u8> {
        self.response_codes
            .get(&command.get())
            .into_iter()
            .flat_map(|codes| codes.iter())
            .filter_map(|(code, definition)| {
                (definition.status == ResponseCodeStatus::Warning).then_some(*code)
            })
            .collect()
    }

    /// Validates every schema in the profile.
    pub fn validate(&self) -> Result<(), crate::device::SchemaError> {
        if self.key.expanded_type > 0x3fff {
            return Err(crate::device::SchemaError::ExpandedDeviceType(
                self.key.expanded_type,
            ));
        }
        if self.display_name.trim().is_empty() {
            return Err(crate::device::SchemaError::EmptyProfileName);
        }
        if self.source_revision.trim().is_empty() {
            return Err(crate::device::SchemaError::MissingSourceRevision);
        }
        if self.display_name.len() > MAXIMUM_PROFILE_TEXT_BYTES {
            return Err(crate::device::SchemaError::ProfileTextTooLong(
                "display_name",
                self.display_name.len(),
            ));
        }
        if self.source_revision.len() > MAXIMUM_PROFILE_TEXT_BYTES {
            return Err(crate::device::SchemaError::ProfileTextTooLong(
                "source_revision",
                self.source_revision.len(),
            ));
        }
        if self.responses.len() > MAXIMUM_PROFILE_COMMANDS
            || self.response_codes.len() > MAXIMUM_PROFILE_COMMANDS
        {
            return Err(crate::device::SchemaError::TooManyProfileCommands(
                self.responses.len().max(self.response_codes.len()),
            ));
        }
        for schema in self.responses.values() {
            schema.validate()?;
        }
        for codes in self.response_codes.values() {
            for (code, definition) in codes {
                if code & 0x80 != 0 {
                    return Err(crate::device::SchemaError::ResponseCode(*code));
                }
                if definition.description.trim().is_empty() {
                    return Err(crate::device::SchemaError::EmptyResponseCodeDescription(
                        *code,
                    ));
                }
                if definition.description.len() > MAXIMUM_PROFILE_TEXT_BYTES {
                    return Err(crate::device::SchemaError::ProfileTextTooLong(
                        "response code description",
                        definition.description.len(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Profile catalog with exact and compatible revision selection.
#[derive(Debug, Clone)]
pub struct DeviceCatalog {
    profiles: BTreeMap<DeviceKey, DeviceProfile>,
    limits: CatalogLimits,
}

impl Default for DeviceCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl DeviceCatalog {
    /// Creates an empty catalog.
    pub const fn new() -> Self {
        Self {
            profiles: BTreeMap::new(),
            limits: CatalogLimits {
                profiles: DEFAULT_MAXIMUM_CATALOG_PROFILES,
                json_bytes: DEFAULT_MAXIMUM_CATALOG_JSON_BYTES,
            },
        }
    }

    /// Creates an empty catalog after validating explicit memory limits.
    pub fn with_limits(limits: CatalogLimits) -> Result<Self, CatalogLoadError> {
        limits.validate()?;
        Ok(Self {
            profiles: BTreeMap::new(),
            limits,
        })
    }

    /// Validates and inserts a profile, returning the replaced value.
    pub fn insert(
        &mut self,
        profile: DeviceProfile,
    ) -> Result<Option<DeviceProfile>, crate::device::SchemaError> {
        profile.validate()?;
        if !self.profiles.contains_key(&profile.key) && self.profiles.len() >= self.limits.profiles
        {
            return Err(crate::device::SchemaError::CatalogProfileLimit(
                self.limits.profiles,
            ));
        }
        Ok(self.profiles.insert(profile.key, profile))
    }

    /// Finds only the exact device type and revision.
    ///
    /// A lower device revision is not selected implicitly because device-specific
    /// command layouts may change between revisions.
    pub fn resolve(&self, key: DeviceKey) -> Option<&DeviceProfile> {
        self.profiles.get(&key)
    }

    /// Loads one profile from JSON after structural validation.
    pub fn load_json(&mut self, bytes: &[u8]) -> Result<DeviceKey, CatalogLoadError> {
        if bytes.len() > self.limits.json_bytes {
            return Err(CatalogLoadError::JsonLimit {
                actual: bytes.len(),
                maximum: self.limits.json_bytes,
            });
        }
        let profile: DeviceProfile = serde_json::from_slice(bytes)?;
        let key = profile.key;
        self.insert(profile)?;
        Ok(key)
    }

    /// Returns the number of profiles.
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Reports whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }

    /// Returns the effective catalog resource limits.
    pub const fn limits(&self) -> CatalogLimits {
        self.limits
    }
}

/// Catalog-loading error.
#[derive(Debug, thiserror::Error)]
pub enum CatalogLoadError {
    /// JSON does not match the model.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// An embedded schema is invalid.
    #[error(transparent)]
    Schema(#[from] crate::device::SchemaError),
    /// One or more catalog limits are zero.
    #[error("catalog limits must be greater than zero")]
    ZeroLimit,
    /// The configured profile count exceeds the hard safety bound.
    #[error("catalog profile limit {0} exceeds the supported limit")]
    ProfileConfigurationLimit(usize),
    /// The configured direct JSON size exceeds the hard safety bound.
    #[error("catalog JSON limit {0} exceeds the supported limit")]
    JsonConfigurationLimit(usize),
    /// A direct JSON input exceeds its configured bound.
    #[error("JSON input contains {actual} bytes and exceeds the {maximum}-byte limit")]
    JsonLimit {
        /// Input length.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
}
