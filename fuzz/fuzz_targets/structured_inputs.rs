#![no_main]

use hart_link::{
    device::{CatalogLimits, DeviceCatalog},
    inspect_base64, inspect_bytes, inspect_hex,
    ip::IpPacket,
    package::{PackageLimits, import_package},
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = inspect_bytes(data);
    let _ = IpPacket::decode(data, 8192);
    if let Ok(text) = core::str::from_utf8(data) {
        let _ = inspect_hex(text);
        let _ = inspect_base64(text);
    }
    let Ok(mut catalog) = DeviceCatalog::with_limits(CatalogLimits {
        profiles: 16,
        json_bytes: 64 * 1024,
    })
    else {
        return;
    };
    let _ = catalog.load_json(data);
    let _ = import_package(
        data,
        &mut catalog,
        PackageLimits {
            archive_bytes: 64 * 1024,
            files: 16,
            file_name_bytes: 256,
            uncompressed_bytes: 64 * 1024,
            file_bytes: 16 * 1024,
        },
    );
});
