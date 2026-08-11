#![cfg(feature = "runtime")]

use std::time::SystemTime;

use hart_link::{
    Address, CommandCode, DeviceReply, Master, PhysicalLayer,
    channel::{ByteChannel, ChannelFuture},
    profile::{
        AddressTiming, AddressTimings, LineProfile, ModuleWindow, PollingWindow, TimingProfile,
    },
    service::{BlockReceiver, BlockSender, BurstConfig, BurstHub, BurstKey, TransferBlock},
    trace::{
        ReplayChannel, ReplayLimits, ReplayStep, Trace, TraceDirection, TraceLimits, TraceRecord,
    },
};

#[derive(Default)]
struct Metrics(std::collections::BTreeMap<&'static str, u64>);

impl hart_link::telemetry::MetricSink for Metrics {
    fn counter(&mut self, name: &'static str, value: u64) {
        self.0.insert(name, value);
    }

    fn gauge(&mut self, name: &'static str, value: u64) {
        self.0.insert(name, value);
    }
}

#[derive(Debug, Clone, Default)]
struct CaptureSend {
    sent: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

impl ByteChannel for CaptureSend {
    fn send<'a>(&'a mut self, bytes: &'a [u8]) -> ChannelFuture<'a, ()> {
        Box::pin(async move {
            self.sent.lock().unwrap().extend_from_slice(bytes);
            Ok(())
        })
    }

    fn receive<'a>(&'a mut self, _buffer: &'a mut [u8]) -> ChannelFuture<'a, usize> {
        Box::pin(async { std::future::pending().await })
    }

    fn flush(&mut self) -> ChannelFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn address() -> Address {
    Address::polling(1, Master::Primary).unwrap()
}

#[test]
fn device_health_metrics_use_fixed_bounded_values() {
    let snapshot = hart_link::DeviceHealthSnapshot {
        successes: 7,
        failures: 2,
        consecutive_transport_failures: 1,
        cooldown_activations: 1,
        cooldown_rejections: 3,
        cooldown_remaining: std::time::Duration::from_millis(25),
        learned_response: Some(std::time::Duration::from_micros(750)),
        adaptive_attempts: 5,
        adaptive_fallbacks: 1,
    };
    let mut metrics = Metrics::default();
    hart_link::telemetry::export_device_health_snapshot(snapshot, &mut metrics);
    assert_eq!(metrics.0["hart_link_device_successes_total"], 7);
    assert_eq!(
        metrics.0["hart_link_device_cooldown_remaining_milliseconds"],
        25
    );
    assert_eq!(
        metrics.0["hart_link_device_learned_response_microseconds"],
        750
    );
}

#[test]
fn block_transfer_round_trip_and_order_check() {
    let source: Vec<u8> = (0..=250).cycle().take(1000).collect();
    let mut sender = BlockSender::new(&source, 73).unwrap();
    let mut receiver = BlockReceiver::new(2000);
    while let Some(block) = sender.next_block().unwrap() {
        receiver.accept(&block).unwrap();
    }
    assert_eq!(receiver.finish().unwrap(), source);

    let mut receiver = BlockReceiver::new(10);
    assert!(
        receiver
            .accept(&TransferBlock {
                sequence: 1,
                last: true,
                data: vec![1],
            })
            .is_err()
    );
    assert!(
        receiver
            .accept(&TransferBlock {
                sequence: 0,
                last: true,
                data: vec![0; 252],
            })
            .is_err()
    );
    assert!(BlockReceiver::try_new(usize::MAX).is_err());
}

#[test]
fn block_transfer_uses_the_complete_u16_sequence_space() {
    let source = vec![0x5a; hart_link::service::MAXIMUM_TRANSFER_BYTES];
    let mut sender = hart_link::service::BlockSender::new(&source, 251).unwrap();
    let mut blocks = 0usize;
    let mut last_sequence = None;
    while let Some(block) = sender.next_block().unwrap() {
        blocks += 1;
        last_sequence = Some(block.sequence);
        if block.last {
            assert_eq!(block.sequence, u16::MAX);
        }
    }
    assert_eq!(blocks, usize::from(u16::MAX) + 1);
    assert_eq!(last_sequence, Some(u16::MAX));
    assert!(sender.progress().complete);

    assert!(matches!(
        hart_link::service::BlockSender::new(&source, 250),
        Err(hart_link::service::TransferError::SequenceSpace)
    ));
}

#[test]
fn block_receiver_rejects_empty_non_final_progress() {
    let mut receiver = hart_link::service::BlockReceiver::new(16);
    let error = receiver
        .accept(&hart_link::service::TransferBlock {
            sequence: 0,
            last: false,
            data: Vec::new(),
        })
        .unwrap_err();
    assert_eq!(
        error,
        hart_link::service::TransferError::EmptyIntermediateBlock
    );
    assert_eq!(receiver.progress().next_sequence, 0);
}

#[tokio::test]
async fn burst_hub_deduplicates_and_unsubscribes_on_drop() {
    let hub = BurstHub::default();
    let key = BurstKey {
        address: address(),
        command: CommandCode::new(1),
    };
    let mut subscription = hub.subscribe(key, 2).unwrap();
    let reply = DeviceReply {
        address: address(),
        command: CommandCode::new(1),
        response_code: 0,
        device_status: 0,
        data: vec![1, 2, 3],
        burst: true,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    };
    hub.publish(reply.clone());
    hub.publish(reply);
    assert_eq!(
        subscription.receive().await.unwrap().reply.data,
        vec![1, 2, 3]
    );
    assert_eq!(hub.snapshot().duplicates, 1);
    drop(subscription);
    assert_eq!(hub.snapshot().subscriptions, 0);
}

#[tokio::test]
async fn burst_hub_bounds_fingerprints_and_expires_duplicate_suppression() {
    let hub = BurstHub::new(BurstConfig {
        maximum_subscriptions: 8,
        maximum_buffered_deliveries: 16,
        maximum_tracked_keys: 2,
        duplicate_window: std::time::Duration::from_millis(10),
    })
    .unwrap();
    let first_key = BurstKey {
        address: address(),
        command: CommandCode::new(1),
    };
    let mut subscription = hub.subscribe(first_key, 2).unwrap();
    for command in 1..=3 {
        hub.publish(DeviceReply {
            address: address(),
            command: CommandCode::new(command),
            response_code: 0,
            device_status: 0,
            data: vec![u8::try_from(command).unwrap()],
            burst: true,
            physical_layer: PhysicalLayer::Fsk,
            response_preambles: 3,
            frame_expansion: vec![],
        });
    }
    assert_eq!(subscription.receive().await.unwrap().reply.data, vec![1]);
    assert_eq!(hub.snapshot().tracked_keys, 2);
    assert_eq!(hub.snapshot().fingerprint_evictions, 1);

    tokio::time::sleep(std::time::Duration::from_millis(12)).await;
    hub.publish(DeviceReply {
        address: address(),
        command: CommandCode::new(1),
        response_code: 0,
        device_status: 0,
        data: vec![1],
        burst: true,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    });
    assert_eq!(subscription.receive().await.unwrap().reply.data, vec![1]);
}

#[test]
fn burst_hub_rejects_forged_oversized_messages() {
    let hub = BurstHub::default();
    let reply = DeviceReply {
        address: address(),
        command: CommandCode::new(1),
        response_code: 0,
        device_status: 0,
        data: vec![0; 256],
        burst: true,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    };
    assert!(hub.try_publish(reply).is_err());
    assert_eq!(hub.snapshot().invalid_messages, 1);
    assert_eq!(hub.snapshot().tracked_keys, 0);
}

#[test]
fn burst_hub_enforces_the_total_subscription_limit() {
    let hub = BurstHub::new(BurstConfig::default().with_maximum_subscriptions(1)).unwrap();
    let key = BurstKey::new(address(), CommandCode::new(1));
    let _first = hub.subscribe_default(key).unwrap();
    assert!(matches!(
        hub.subscribe_default(key),
        Err(hart_link::service::BurstSubscriptionError::SubscriptionLimit(1))
    ));

    let hub = BurstHub::new(
        BurstConfig::default()
            .with_maximum_subscriptions(2)
            .with_maximum_buffered_deliveries(64),
    )
    .unwrap();
    let _first = hub.subscribe_default(key).unwrap();
    assert!(matches!(
        hub.subscribe_default(key),
        Err(hart_link::service::BurstSubscriptionError::AggregateCapacity(64))
    ));
    assert_eq!(hub.snapshot().reserved_delivery_slots, 64);
}

#[test]
fn profile_applies_only_declared_module_shift() {
    let module = ModuleWindow {
        module: 2,
        polling: PollingWindow::new(0, 15).unwrap(),
        address_shift: 16,
    };
    let profile = LineProfile {
        name: "moxa-2".into(),
        timing: TimingProfile::default(),
        modules: vec![module],
        discovery_preambles: 20,
        wake_prefix: vec![0xff; 4],
    };
    profile.validate().unwrap();
    assert_eq!(LineProfile::shifted_address(module, 3), Some(19));
    assert_eq!(LineProfile::shifted_address(module, 16), None);
}

#[test]
fn address_timings_are_bounded_and_validated() {
    let timings = AddressTimings::new()
        .with(
            17,
            AddressTiming::new()
                .with_delay_before_probe(std::time::Duration::from_millis(25))
                .with_response_timeout(std::time::Duration::from_secs(3)),
        )
        .unwrap();
    assert_eq!(timings.len(), 1);
    assert_eq!(
        timings.get(17).unwrap().delay_before_probe,
        std::time::Duration::from_millis(25)
    );
    assert!(
        AddressTimings::new()
            .with(64, AddressTiming::new())
            .is_err()
    );
    assert!(
        AddressTimings::new()
            .with(
                1,
                AddressTiming::new().with_response_timeout(std::time::Duration::ZERO),
            )
            .is_err()
    );
}

#[tokio::test]
async fn discovery_applies_delay_only_to_overridden_address() {
    let profile = LineProfile::single_segment("address-timing", PollingWindow::new(0, 0).unwrap())
        .with_timing(
            TimingProfile::default()
                .with_connect_settle(std::time::Duration::ZERO)
                .with_first_response(std::time::Duration::from_millis(5))
                .with_steady_response(std::time::Duration::from_millis(5))
                .with_scan_interval(std::time::Duration::ZERO),
        );
    let timings = AddressTimings::new()
        .with(
            0,
            AddressTiming::new()
                .with_delay_before_probe(std::time::Duration::from_millis(20))
                .with_response_timeout(std::time::Duration::from_millis(5)),
        )
        .unwrap();
    let channel = CaptureSend::default();
    let (client, runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let task = tokio::spawn(runner.run());
    let started = tokio::time::Instant::now();
    let report = hart_link::service::discover_line_with_address_timings(
        &client,
        &profile,
        Master::Primary,
        hart_link::service::DiscoveryOptions::default(),
        &timings,
    )
    .await
    .unwrap();
    assert!(report.devices.is_empty());
    assert!(started.elapsed() >= std::time::Duration::from_millis(25));
    drop(client);
    task.abort();
}

#[test]
fn profile_rejects_overlapping_translated_modules() {
    let profile = LineProfile::new("overlap")
        .with_module(ModuleWindow::new(1, PollingWindow::new(0, 3).unwrap()))
        .with_module(ModuleWindow::new(2, PollingWindow::new(0, 3).unwrap()).with_address_shift(2));
    assert!(matches!(
        profile.validate(),
        Err(hart_link::profile::ProfileError::AddressOverlap {
            address: 2,
            first_module: 1,
            second_module: 2,
        })
    ));
}

#[tokio::test]
async fn discovery_uses_profile_preamble_count() {
    let profile = LineProfile {
        name: "preamble-check".into(),
        timing: TimingProfile {
            connect_settle: std::time::Duration::ZERO,
            first_response: std::time::Duration::from_millis(10),
            steady_response: std::time::Duration::from_millis(10),
            scan_interval: std::time::Duration::ZERO,
        },
        modules: vec![ModuleWindow {
            module: 1,
            polling: PollingWindow::new(0, 0).unwrap(),
            address_shift: 0,
        }],
        discovery_preambles: 20,
        wake_prefix: vec![0x00, 0xaa, 0x55],
    };
    let channel = CaptureSend::default();
    let sent = channel.sent.clone();
    let (client, runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let task = tokio::spawn(runner.run());
    let report = hart_link::service::discover_line(&client, &profile, Master::Primary)
        .await
        .unwrap();
    assert_eq!(report.attempts, 1);
    assert!(report.devices.is_empty());
    let sent = sent.lock().unwrap();
    assert!(sent.starts_with(&[0x00, 0xaa, 0x55]));
    assert_eq!(&sent[3..23], &[0xff; 20]);
    drop(client);
    task.abort();
}

#[test]
fn trace_is_bounded_and_exports_pcapng() {
    let mut trace = Trace::new(2);
    for byte in 1..=3 {
        trace.push(TraceRecord {
            direction: TraceDirection::Outbound,
            timestamp: SystemTime::UNIX_EPOCH,
            bytes: vec![byte],
        });
    }
    assert_eq!(trace.records().len(), 2);
    assert_eq!(trace.dropped(), 1);
    assert_eq!(&trace.to_pcapng()[..4], &0x0a0d_0d0au32.to_le_bytes());
}

#[test]
fn trace_enforces_a_payload_byte_budget() {
    let mut trace = Trace::with_limits(TraceLimits {
        maximum_records: 10,
        maximum_bytes: 4,
    });
    for bytes in [vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9, 10, 11]] {
        trace.push(TraceRecord {
            direction: TraceDirection::Inbound,
            timestamp: SystemTime::UNIX_EPOCH,
            bytes,
        });
    }
    assert_eq!(trace.records().len(), 1);
    assert_eq!(trace.retained_bytes(), 3);
    assert_eq!(trace.dropped(), 2);
    assert_eq!(trace.dropped_bytes(), 8);
}

#[test]
fn strict_trace_construction_rejects_resource_amplifying_limits() {
    assert!(
        Trace::try_with_limits(TraceLimits {
            maximum_records: usize::MAX,
            maximum_bytes: usize::MAX,
        })
        .is_err()
    );
    let trace = Trace::with_limits(TraceLimits {
        maximum_records: usize::MAX,
        maximum_bytes: usize::MAX,
    });
    assert_eq!(
        trace.limits().maximum_records,
        hart_link::trace::MAXIMUM_TRACE_RECORDS
    );
    assert_eq!(
        trace.limits().maximum_bytes,
        hart_link::trace::MAXIMUM_TRACE_BYTES
    );
}

#[test]
fn replay_construction_stops_infinite_or_oversized_scenarios() {
    let limits = ReplayLimits::default()
        .with_maximum_steps(2)
        .with_maximum_bytes(3);
    assert!(matches!(
        ReplayChannel::with_limits(std::iter::repeat(ReplayStep::Fail), limits),
        Err(hart_link::trace::ReplayBuildError::ScenarioSteps(2))
    ));
    assert!(matches!(
        ReplayChannel::with_limits([ReplayStep::Provide(vec![0; 4])], limits),
        Err(hart_link::trace::ReplayBuildError::ScenarioBytes(3))
    ));
}
