//! Export of bounded metrics to an external observability system.

use crate::service::{DeviceHealthSnapshot, LinkSnapshot};

/// Sink for counters and current values.
pub trait MetricSink {
    /// Records a monotonic counter.
    fn counter(&mut self, name: &'static str, value: u64);

    /// Records a current integer value.
    fn gauge(&mut self, name: &'static str, value: u64);
}

/// Exports a snapshot with fixed names and no high-cardinality labels.
pub fn export_link_snapshot(snapshot: LinkSnapshot, sink: &mut impl MetricSink) {
    sink.gauge(
        "hart_link_queue_pending",
        u64::try_from(snapshot.queued).unwrap_or(u64::MAX),
    );
    sink.counter("hart_link_started_total", snapshot.started);
    sink.counter("hart_link_completed_total", snapshot.completed);
    sink.counter("hart_link_failed_total", snapshot.failed);
    sink.counter("hart_link_cancelled_total", snapshot.cancelled);
    sink.counter("hart_link_coalesced_total", snapshot.coalesced);
    sink.counter("hart_link_denied_total", snapshot.denied);
}

/// Exports managed-device health with fixed names and no device identifiers.
pub fn export_device_health_snapshot(snapshot: DeviceHealthSnapshot, sink: &mut impl MetricSink) {
    sink.counter("hart_link_device_successes_total", snapshot.successes);
    sink.counter("hart_link_device_failures_total", snapshot.failures);
    sink.counter(
        "hart_link_device_cooldown_activations_total",
        snapshot.cooldown_activations,
    );
    sink.counter(
        "hart_link_device_cooldown_rejections_total",
        snapshot.cooldown_rejections,
    );
    sink.counter(
        "hart_link_device_adaptive_attempts_total",
        snapshot.adaptive_attempts,
    );
    sink.counter(
        "hart_link_device_adaptive_fallbacks_total",
        snapshot.adaptive_fallbacks,
    );
    sink.gauge(
        "hart_link_device_consecutive_transport_failures",
        u64::from(snapshot.consecutive_transport_failures),
    );
    sink.gauge(
        "hart_link_device_cooldown_remaining_milliseconds",
        u64::try_from(snapshot.cooldown_remaining.as_millis()).unwrap_or(u64::MAX),
    );
    sink.gauge(
        "hart_link_device_learned_response_microseconds",
        snapshot.learned_response.map_or(0, |duration| {
            u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
        }),
    );
}
