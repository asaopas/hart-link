//! Bounded extraction of DeviceInfo from an FDI container.

use std::{
    collections::BTreeSet,
    io::{Cursor, Read},
};

use thiserror::Error;
use zip::ZipArchive;

use crate::device::{DeviceCatalog, DeviceKey};

/// Hard upper bound for a compressed package container.
pub const MAXIMUM_PACKAGE_ARCHIVE_BYTES: usize = 256 * 1024 * 1024;
/// Hard upper bound for archive entry count.
pub const MAXIMUM_PACKAGE_FILES: usize = 65_536;
/// Hard upper bound for an archive entry name.
pub const MAXIMUM_PACKAGE_FILE_NAME_BYTES: usize = 16 * 1024;
/// Hard upper bound for aggregate declared uncompressed content.
pub const MAXIMUM_PACKAGE_UNCOMPRESSED_BYTES: u64 = 1024 * 1024 * 1024;
/// Hard upper bound for one uncompressed entry.
pub const MAXIMUM_PACKAGE_FILE_BYTES: u64 = 64 * 1024 * 1024;

/// Extraction limits for an untrusted package.
#[derive(Debug, Clone, Copy)]
pub struct PackageLimits {
    /// Maximum size of the complete ZIP container.
    pub archive_bytes: usize,
    /// Maximum number of files in the archive.
    pub files: usize,
    /// Maximum UTF-8 byte length of one archive entry name.
    pub file_name_bytes: usize,
    /// Maximum total uncompressed size.
    pub uncompressed_bytes: u64,
    /// Maximum size of a single file.
    pub file_bytes: u64,
}

impl Default for PackageLimits {
    fn default() -> Self {
        Self {
            archive_bytes: 64 * 1024 * 1024,
            files: 1024,
            file_name_bytes: 1024,
            uncompressed_bytes: 64 * 1024 * 1024,
            file_bytes: 8 * 1024 * 1024,
        }
    }
}

impl PackageLimits {
    /// Rejects limits that cannot admit any useful package content.
    pub const fn validate(self) -> Result<(), PackageError> {
        if self.archive_bytes == 0
            || self.files == 0
            || self.file_name_bytes == 0
            || self.uncompressed_bytes == 0
            || self.file_bytes == 0
        {
            return Err(PackageError::ZeroLimit);
        }
        if self.archive_bytes > MAXIMUM_PACKAGE_ARCHIVE_BYTES {
            return Err(PackageError::LimitTooLarge("archive_bytes"));
        }
        if self.files > MAXIMUM_PACKAGE_FILES {
            return Err(PackageError::LimitTooLarge("files"));
        }
        if self.file_name_bytes > MAXIMUM_PACKAGE_FILE_NAME_BYTES {
            return Err(PackageError::LimitTooLarge("file_name_bytes"));
        }
        if self.uncompressed_bytes > MAXIMUM_PACKAGE_UNCOMPRESSED_BYTES {
            return Err(PackageError::LimitTooLarge("uncompressed_bytes"));
        }
        if self.file_bytes > MAXIMUM_PACKAGE_FILE_BYTES {
            return Err(PackageError::LimitTooLarge("file_bytes"));
        }
        Ok(())
    }

    /// Sets the complete compressed-container byte limit.
    pub const fn with_archive_bytes(mut self, archive_bytes: usize) -> Self {
        self.archive_bytes = archive_bytes;
        self
    }

    /// Sets the archive entry-count limit.
    pub const fn with_files(mut self, files: usize) -> Self {
        self.files = files;
        self
    }

    /// Sets the maximum encoded entry-name length.
    pub const fn with_file_name_bytes(mut self, file_name_bytes: usize) -> Self {
        self.file_name_bytes = file_name_bytes;
        self
    }

    /// Sets the aggregate uncompressed-size limit.
    pub const fn with_uncompressed_bytes(mut self, uncompressed_bytes: u64) -> Self {
        self.uncompressed_bytes = uncompressed_bytes;
        self
    }

    /// Sets the per-file uncompressed-size limit.
    pub const fn with_file_bytes(mut self, file_bytes: u64) -> Self {
        self.file_bytes = file_bytes;
        self
    }
}

/// Result of importing one package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageReport {
    /// Loaded device profiles.
    pub devices: Vec<DeviceKey>,
    /// Number of archive entries inspected.
    pub inspected_files: usize,
    /// JSON entries that did not match a valid HartLink DeviceInfo-style profile.
    pub skipped_json: Vec<String>,
}

/// Error produced while reading a package with enforced limits.
#[derive(Debug, Error)]
pub enum PackageError {
    /// Package extraction limits must permit forward progress.
    #[error("package limits must be greater than zero")]
    ZeroLimit,
    /// A configured extraction limit exceeds its hard safety bound.
    #[error("package limit {0} exceeds the supported maximum")]
    LimitTooLarge(&'static str),
    /// The complete compressed container exceeds its configured limit.
    #[error("package contains {actual} bytes and exceeds the {maximum}-byte archive limit")]
    ArchiveSize {
        /// Actual compressed-container length.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// The ZIP container is invalid.
    #[error("ZIP error: {0}")]
    Zip(#[from] zip::result::ZipError),
    /// An extracted entry could not be read.
    #[error("read error: {0}")]
    Io(#[from] std::io::Error),
    /// The archive has too many entries.
    #[error("archive contains more entries than allowed")]
    FileCount,
    /// An entry name is too large to retain safely in diagnostics.
    #[error("archive entry name contains {0} UTF-8 bytes")]
    FileNameSize(usize),
    /// A single file exceeds its limit.
    #[error("file {0} exceeds the configured limit")]
    FileSize(String),
    /// The total extracted size exceeds its limit.
    #[error("total uncompressed size exceeds the configured limit")]
    TotalSize,
    /// An entry path attempts to escape the archive root.
    #[error("unsafe archive path: {0}")]
    UnsafePath(String),
    /// A DeviceInfo profile failed validation.
    #[error("DeviceInfo error: {0}")]
    Device(#[from] crate::device::CatalogLoadError),
    /// Two package entries claim the same exact device revision.
    #[error("package contains duplicate device type {expanded_type} revision {device_revision}")]
    DuplicateDevice {
        /// Expanded device type.
        expanded_type: u16,
        /// Device revision.
        device_revision: u8,
    },
}

/// Loads JSON profiles from a package without extracting the archive to disk.
pub fn import_package(
    bytes: &[u8],
    catalog: &mut DeviceCatalog,
    limits: PackageLimits,
) -> Result<PackageReport, PackageError> {
    limits.validate()?;
    if bytes.len() > limits.archive_bytes {
        return Err(PackageError::ArchiveSize {
            actual: bytes.len(),
            maximum: limits.archive_bytes,
        });
    }
    let mut archive = ZipArchive::new(Cursor::new(bytes))?;
    if archive.len() > limits.files {
        return Err(PackageError::FileCount);
    }
    let mut total = 0u64;
    let mut actual_json_bytes = 0u64;
    let mut devices = Vec::new();
    let mut skipped_json = Vec::new();
    let mut package_keys = BTreeSet::new();
    let mut staging = catalog.clone();
    for index in 0..archive.len() {
        let file = archive.by_index(index)?;
        if file.name().len() > limits.file_name_bytes {
            return Err(PackageError::FileNameSize(file.name().len()));
        }
        let name = file.name().to_owned();
        if file.enclosed_name().is_none() {
            return Err(PackageError::UnsafePath(name));
        }
        if file.size() > limits.file_bytes {
            return Err(PackageError::FileSize(name));
        }
        total = total
            .checked_add(file.size())
            .ok_or(PackageError::TotalSize)?;
        if total > limits.uncompressed_bytes {
            return Err(PackageError::TotalSize);
        }
        if file.is_file() && name.to_ascii_lowercase().ends_with(".json") {
            let capacity = usize::try_from(file.size().min(limits.file_bytes)).unwrap_or(0);
            let mut content = Vec::with_capacity(capacity);
            let read_limit = limits.file_bytes.saturating_add(1);
            file.take(read_limit).read_to_end(&mut content)?;
            if u64::try_from(content.len()).unwrap_or(u64::MAX) > limits.file_bytes {
                return Err(PackageError::FileSize(name));
            }
            actual_json_bytes = actual_json_bytes
                .checked_add(u64::try_from(content.len()).unwrap_or(u64::MAX))
                .ok_or(PackageError::TotalSize)?;
            if actual_json_bytes > limits.uncompressed_bytes {
                return Err(PackageError::TotalSize);
            }
            match staging.load_json(&content) {
                Ok(key) if package_keys.insert(key) => devices.push(key),
                Ok(key) => {
                    return Err(PackageError::DuplicateDevice {
                        expanded_type: key.expanded_type,
                        device_revision: key.device_revision,
                    });
                }
                Err(
                    error @ (crate::device::CatalogLoadError::JsonLimit { .. }
                    | crate::device::CatalogLoadError::Schema(
                        crate::device::SchemaError::CatalogProfileLimit(_),
                    )),
                ) => return Err(PackageError::Device(error)),
                Err(_) => skipped_json.push(name),
            }
        }
    }
    *catalog = staging;
    Ok(PackageReport {
        devices,
        inspected_files: archive.len(),
        skipped_json,
    })
}
