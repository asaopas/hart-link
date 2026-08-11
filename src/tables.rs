//! Versioned resolution of codes from HART common tables.

use alloc::{collections::BTreeMap, string::String};

use thiserror::Error;

/// Largest number of values accepted in one versioned common table.
pub const MAXIMUM_COMMON_TABLE_ENTRIES: usize = 65_536;
/// Largest UTF-8 byte length accepted for table revision and labels.
pub const MAXIMUM_COMMON_TABLE_TEXT_BYTES: usize = 1024;
/// Default upper bound for tables retained by one repository.
pub const DEFAULT_MAXIMUM_TABLES: usize = 1024;
/// Default upper bound for entries retained across one repository.
pub const DEFAULT_MAXIMUM_TABLE_ENTRIES: usize = 1_048_576;
/// Hard upper bound for configured table count.
pub const MAXIMUM_TABLES: usize = 4096;
/// Hard upper bound for configured aggregate entry count.
pub const MAXIMUM_TABLE_ENTRIES: usize = 4_194_304;

/// Aggregate repository resource limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableRepositoryLimits {
    /// Maximum number of versioned common tables.
    pub tables: usize,
    /// Maximum number of values across every retained table.
    pub entries: usize,
}

impl Default for TableRepositoryLimits {
    fn default() -> Self {
        Self {
            tables: DEFAULT_MAXIMUM_TABLES,
            entries: DEFAULT_MAXIMUM_TABLE_ENTRIES,
        }
    }
}

impl TableRepositoryLimits {
    /// Sets the retained table-count limit.
    pub const fn with_tables(mut self, tables: usize) -> Self {
        self.tables = tables;
        self
    }

    /// Sets the aggregate retained value-count limit.
    pub const fn with_entries(mut self, entries: usize) -> Self {
        self.entries = entries;
        self
    }

    /// Validates strict repository limits.
    pub const fn validate(self) -> Result<(), TableError> {
        if self.tables == 0 || self.entries == 0 {
            return Err(TableError::ZeroRepositoryLimit);
        }
        if self.tables > MAXIMUM_TABLES {
            return Err(TableError::RepositoryTableLimit(self.tables));
        }
        if self.entries > MAXIMUM_TABLE_ENTRIES {
            return Err(TableError::RepositoryEntryLimit(self.entries));
        }
        Ok(())
    }
}

/// One common-table entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableEntry {
    /// Numeric code carried on the wire.
    pub code: u32,
    /// Human-readable name.
    pub label: String,
}

/// One named table from a specific source revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommonTable {
    /// Published table number.
    pub number: u16,
    /// Source revision.
    pub revision: String,
    /// Entries indexed by numeric code.
    pub entries: BTreeMap<u32, TableEntry>,
}

impl CommonTable {
    /// Validates key consistency and rejects empty names.
    pub fn validate(&self) -> Result<(), TableError> {
        if self.revision.trim().is_empty() {
            return Err(TableError::MissingRevision(self.number));
        }
        if self.revision.len() > MAXIMUM_COMMON_TABLE_TEXT_BYTES {
            return Err(TableError::TextTooLong("revision", self.revision.len()));
        }
        if self.entries.len() > MAXIMUM_COMMON_TABLE_ENTRIES {
            return Err(TableError::TooManyEntries(self.entries.len()));
        }
        for (code, entry) in &self.entries {
            if *code != entry.code {
                return Err(TableError::MismatchedCode {
                    key: *code,
                    value: entry.code,
                });
            }
            if entry.label.trim().is_empty() {
                return Err(TableError::EmptyLabel(*code));
            }
            if entry.label.len() > MAXIMUM_COMMON_TABLE_TEXT_BYTES {
                return Err(TableError::TextTooLong("entry label", entry.label.len()));
            }
        }
        Ok(())
    }

    /// Returns a known name or preserves the original unknown code.
    pub fn resolve(&self, code: u32) -> ResolvedCode<'_> {
        match self.entries.get(&code) {
            Some(entry) => ResolvedCode::Known(entry),
            None => ResolvedCode::Unknown(code),
        }
    }
}

/// Result of resolving a common-table value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedCode<'a> {
    /// Known entry.
    Known(&'a TableEntry),
    /// Unknown code preserved without conversion.
    Unknown(u32),
}

/// Table repository with an explicit revision-replacement policy.
#[derive(Debug, Clone)]
pub struct TableRepository {
    tables: BTreeMap<u16, CommonTable>,
    limits: TableRepositoryLimits,
    total_entries: usize,
}

impl Default for TableRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl TableRepository {
    /// Creates an empty repository with conservative aggregate limits.
    pub const fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            limits: TableRepositoryLimits {
                tables: DEFAULT_MAXIMUM_TABLES,
                entries: DEFAULT_MAXIMUM_TABLE_ENTRIES,
            },
            total_entries: 0,
        }
    }

    /// Creates an empty repository after validating aggregate limits.
    pub fn with_limits(limits: TableRepositoryLimits) -> Result<Self, TableError> {
        limits.validate()?;
        Ok(Self {
            tables: BTreeMap::new(),
            limits,
            total_entries: 0,
        })
    }

    /// Adds a validated table and returns the previous revision.
    pub fn replace(&mut self, table: CommonTable) -> Result<Option<CommonTable>, TableError> {
        table.validate()?;
        let previous = self.tables.get(&table.number);
        let previous_entries = previous.map_or(0, |table| table.entries.len());
        if previous.is_none() && self.tables.len() >= self.limits.tables {
            return Err(TableError::RepositoryTableLimit(self.limits.tables));
        }
        let total_entries = self
            .total_entries
            .checked_sub(previous_entries)
            .and_then(|count| count.checked_add(table.entries.len()))
            .ok_or(TableError::RepositoryEntryLimit(self.limits.entries))?;
        if total_entries > self.limits.entries {
            return Err(TableError::RepositoryEntryLimit(self.limits.entries));
        }
        let previous = self.tables.insert(table.number, table);
        self.total_entries = total_entries;
        Ok(previous)
    }

    /// Returns a table by its published number.
    pub fn get(&self, number: u16) -> Option<&CommonTable> {
        self.tables.get(&number)
    }

    /// Returns the number of loaded tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Reports whether the repository is empty.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Returns the number of retained values across all tables.
    pub const fn total_entries(&self) -> usize {
        self.total_entries
    }

    /// Returns the effective aggregate repository limits.
    pub const fn limits(&self) -> TableRepositoryLimits {
        self.limits
    }
}

/// Common-table validation error.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TableError {
    /// Aggregate repository limits must permit at least one table and entry.
    #[error("table repository limits must be greater than zero")]
    ZeroRepositoryLimit,
    /// The repository reached or exceeded its configured table-count limit.
    #[error("table repository table limit is {0}")]
    RepositoryTableLimit(usize),
    /// The repository reached or exceeded its configured aggregate entry limit.
    #[error("table repository entry limit is {0}")]
    RepositoryEntryLimit(usize),
    /// The source revision is missing.
    #[error("table {0} has no source revision")]
    MissingRevision(u16),
    /// A map key does not match the entry code.
    #[error("map key {key} does not match entry code {value}")]
    MismatchedCode {
        /// Map key.
        key: u32,
        /// Code in the entry value.
        value: u32,
    },
    /// An entry has an empty name.
    #[error("code {0} has an empty name")]
    EmptyLabel(u32),
    /// A revision or label is implausibly large.
    #[error("table {0} contains {1} UTF-8 bytes")]
    TextTooLong(&'static str, usize),
    /// A single table contains an implausible number of values.
    #[error("common table contains too many entries: {0}")]
    TooManyEntries(usize),
}
