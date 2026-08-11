#![no_main]

use hart_link::{ChecksumPolicy, DecodeLimits, FrameDecoder};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let capacity = data.first().map_or(64, |value| usize::from(*value) + 1);
    let minimum_preambles = data.get(1).map_or(1, |value| value.saturating_add(1));
    let chunk = data
        .get(2)
        .map_or(1, |value| usize::from(*value).saturating_add(1));
    let mut decoder = FrameDecoder::new(DecodeLimits {
        buffer_capacity: capacity,
        minimum_preambles,
        checksum_policy: ChecksumPolicy::Strict,
    });
    for fragment in data.chunks(chunk) {
        let _ = decoder.push(fragment);
        assert!(decoder.buffered_len() <= capacity);
    }
});
