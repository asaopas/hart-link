#![cfg(feature = "fdi-package")]

use std::io::{Cursor, Write};

use hart_link::{
    device::{DeviceCatalog, DeviceKey, DeviceProfile},
    package::{PackageError, PackageLimits, import_package},
};
use zip::{ZipWriter, write::SimpleFileOptions};

fn profile(expanded_type: u16) -> DeviceProfile {
    DeviceProfile {
        key: DeviceKey {
            expanded_type,
            device_revision: 1,
        },
        display_name: format!("device-{expanded_type}"),
        source_revision: "test".into(),
        responses: std::collections::BTreeMap::new(),
        response_codes: std::collections::BTreeMap::new(),
    }
}

fn package(entries: &[(&str, &DeviceProfile)]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, profile) in entries {
        writer
            .start_file(*name, SimpleFileOptions::default())
            .unwrap();
        writer
            .write_all(&serde_json::to_vec(profile).unwrap())
            .unwrap();
    }
    writer.finish().unwrap().into_inner()
}

#[test]
fn package_import_is_atomic_and_rejects_duplicate_device_revisions() {
    let existing = profile(1);
    let imported = profile(2);
    let mut catalog = DeviceCatalog::new();
    catalog.insert(existing.clone()).unwrap();
    let bytes = package(&[("first.json", &imported), ("duplicate.json", &imported)]);

    let error = import_package(&bytes, &mut catalog, PackageLimits::default()).unwrap_err();
    assert!(matches!(
        error,
        PackageError::DuplicateDevice {
            expanded_type: 2,
            device_revision: 1,
        }
    ));
    assert_eq!(catalog.len(), 1);
    assert!(catalog.resolve(existing.key).is_some());
    assert!(catalog.resolve(imported.key).is_none());
}

#[test]
fn package_rejects_the_container_before_zip_processing_when_it_is_too_large() {
    let bytes = package(&[("device.json", &profile(7))]);
    let maximum = bytes.len().saturating_sub(1);
    let mut catalog = DeviceCatalog::new();
    let error = import_package(
        &bytes,
        &mut catalog,
        PackageLimits::default().with_archive_bytes(maximum),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        PackageError::ArchiveSize { actual, maximum: limit }
            if actual == bytes.len() && limit == maximum
    ));
    assert!(catalog.is_empty());
}

#[test]
fn package_catalog_limit_is_an_atomic_error_not_a_skipped_profile() {
    let existing = profile(1);
    let imported = profile(2);
    let mut catalog = DeviceCatalog::with_limits(hart_link::device::CatalogLimits {
        profiles: 1,
        json_bytes: 1024 * 1024,
    })
    .unwrap();
    catalog.insert(existing.clone()).unwrap();
    let bytes = package(&[("device.json", &imported)]);
    assert!(matches!(
        import_package(&bytes, &mut catalog, PackageLimits::default()),
        Err(PackageError::Device(_))
    ));
    assert_eq!(catalog.len(), 1);
    assert!(catalog.resolve(existing.key).is_some());
    assert!(catalog.resolve(imported.key).is_none());
}
