#![cfg(feature = "emulator")]

use std::time::Duration;

use hart_link::{
    Address, Frame, FrameKind, Master, PhysicalLayer, Request,
    emulator::{EmulatedDevice, EmulatedIdentity, FaultPlan, MemoryChannel},
    operation::{
        DeviceIdentity, RawOperation, ReadDeviceIdentity, ReadLoopSignal, ReadPrimaryValue,
    },
    profile::{LineProfile, ModuleWindow, PollingWindow, TimingProfile},
    service::{
        AdaptiveTiming, DeviceHealthOptions, DeviceSession, DiscoveryHints, DiscoveryOptions,
        LinkBuilder, LinkEvent, ManagedDeviceSession, ManagedSessionError, Plan, PlanExpectation,
        PlanStep, Priority, RetryPolicy, SnapshotField, SnapshotOptions, discover_line,
        discover_line_with_options,
    },
    trace::{ReplayChannel, ReplayStep},
};

fn test_device(address: Address) -> EmulatedDevice {
    EmulatedDevice {
        address,
        identity: EmulatedIdentity {
            manufacturer_id: 0x1234,
            device_type: 0x042a,
            request_preambles: 7,
            universal_revision: 7,
            device_revision: 3,
            software_revision: 4,
            hardware_revision: 5,
            physical_signaling: 1,
            flags: 1,
            device_id: 0x12_34_56,
            response_preambles: 5,
            maximum_device_variables: 8,
            configuration_change_counter: 12,
            extended_status: 0,
            private_label_distributor: 0,
            device_profile: 1,
        },
        primary_unit: 57,
        primary_value: 123.5,
        loop_current: 12.0,
        percent: 50.0,
        status: 0,
        additional_status: vec![1, 2, 3, 4],
    }
}

fn test_identity() -> DeviceIdentity {
    DeviceIdentity {
        response_identifier: 0xfe,
        manufacturer_id: 0x1234,
        device_type: 0x042a,
        request_preambles: 7,
        universal_revision: 7,
        device_revision: 3,
        software_revision: 4,
        hardware_revision: 5,
        physical_signaling: 1,
        flags: 1,
        device_id: 0x12_34_56,
        response_preambles: Some(5),
        maximum_device_variables: Some(8),
        configuration_change_counter: Some(12),
        extended_status: Some(0),
        private_label_distributor: Some(0),
        device_profile: Some(1),
        extension: Vec::new(),
    }
}

#[derive(Debug)]
struct HangingSend;

impl hart_link::channel::ByteChannel for HangingSend {
    fn send<'a>(&'a mut self, _bytes: &'a [u8]) -> hart_link::channel::ChannelFuture<'a, ()> {
        Box::pin(std::future::pending())
    }

    fn receive<'a>(
        &'a mut self,
        _buffer: &'a mut [u8],
    ) -> hart_link::channel::ChannelFuture<'a, usize> {
        Box::pin(std::future::pending())
    }

    fn flush(&mut self) -> hart_link::channel::ChannelFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug, Clone, Default)]
struct SilentChannel {
    sends: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Debug)]
struct AdaptiveScriptChannel {
    address: Address,
    sends: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pending: std::collections::VecDeque<u8>,
}

impl AdaptiveScriptChannel {
    fn new(address: Address) -> Self {
        Self {
            address,
            sends: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            pending: std::collections::VecDeque::new(),
        }
    }
}

impl hart_link::channel::ByteChannel for AdaptiveScriptChannel {
    fn send<'a>(&'a mut self, _bytes: &'a [u8]) -> hart_link::channel::ChannelFuture<'a, ()> {
        Box::pin(async move {
            let attempt = self
                .sends
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            if attempt != 2 {
                let mut payload = vec![0, 0, 57];
                payload.extend_from_slice(&123.5f32.to_be_bytes());
                let response = Frame {
                    preambles: 5,
                    kind: FrameKind::Response,
                    physical_layer: PhysicalLayer::Fsk,
                    address: self.address,
                    expansion: Vec::new(),
                    wire_command: 1,
                    payload,
                    repair: None,
                }
                .encode()
                .unwrap();
                self.pending.extend(response);
            }
            Ok(())
        })
    }

    fn receive<'a>(
        &'a mut self,
        buffer: &'a mut [u8],
    ) -> hart_link::channel::ChannelFuture<'a, usize> {
        Box::pin(async move {
            if self.pending.is_empty() {
                return std::future::pending().await;
            }
            let count = buffer.len().min(self.pending.len());
            for destination in &mut buffer[..count] {
                *destination = self.pending.pop_front().unwrap_or_default();
            }
            Ok(count)
        })
    }

    fn flush(&mut self) -> hart_link::channel::ChannelFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

impl hart_link::channel::ByteChannel for SilentChannel {
    fn send<'a>(&'a mut self, _bytes: &'a [u8]) -> hart_link::channel::ChannelFuture<'a, ()> {
        self.sends
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }

    fn receive<'a>(
        &'a mut self,
        _buffer: &'a mut [u8],
    ) -> hart_link::channel::ChannelFuture<'a, usize> {
        Box::pin(std::future::pending())
    }

    fn flush(&mut self) -> hart_link::channel::ChannelFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn runner_handles_echo_noise_fragments_and_many_clients() {
    let address = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(64);
    host.set_faults(FaultPlan {
        echo: true,
        ..FaultPlan::default()
    })
    .await;
    device_channel
        .set_faults(FaultPlan {
            noise_prefix: vec![0x00, 0x55, 0x7e],
            fragment_size: Some(2),
            ..FaultPlan::default()
        })
        .await;
    let (client, runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));

    let session = DeviceSession::identify(client.clone(), address, RetryPolicy::default())
        .await
        .unwrap();
    let identity = &session.profile().identity;
    assert_eq!(identity.device_id, 0x12_34_56);
    assert_eq!(identity.request_preambles, 7);
    assert_eq!(
        session.profile().address,
        Address::unique(0x042a, 0x12_34_56, Master::Primary).unwrap()
    );
    let session_value = session
        .execute(&ReadPrimaryValue, Priority::Normal, RetryPolicy::default())
        .await
        .unwrap();
    assert!((session_value.value - 123.5).abs() < f32::EPSILON);

    let mut tasks = Vec::new();
    for index in 0..24 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            if index % 2 == 0 {
                client
                    .execute(
                        address,
                        &ReadPrimaryValue,
                        Priority::Normal,
                        RetryPolicy::default(),
                    )
                    .await
                    .map(|value| value.value)
            } else {
                client
                    .execute(
                        address,
                        &ReadLoopSignal,
                        Priority::Normal,
                        RetryPolicy::default(),
                    )
                    .await
                    .map(|value| value.percent)
            }
        }));
    }
    for task in tasks {
        let value = task.await.unwrap().unwrap();
        assert!((value - 123.5).abs() < f32::EPSILON || (value - 50.0).abs() < f32::EPSILON);
    }
    let snapshot = client.snapshot();
    assert_eq!(snapshot.completed, 26);
    assert_eq!(snapshot.failed, 0);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn managed_session_opens_cooldown_without_consuming_queue_capacity() {
    let polling = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    device_channel
        .set_faults(FaultPlan {
            drop_next: true,
            ..FaultPlan::default()
        })
        .await;
    let (client, runner) = LinkBuilder::new(host)
        .late_response_guard(Duration::ZERO)
        .build()
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(polling).run(device_channel));
    let session = DeviceSession::from_identity(client.clone(), polling, test_identity()).unwrap();
    let managed = ManagedDeviceSession::new(
        session,
        DeviceHealthOptions::default()
            .with_failure_threshold(std::num::NonZeroU8::MIN)
            .with_cooldown(Duration::from_secs(1))
            .with_adaptive(None),
    )
    .unwrap();
    let retry = RetryPolicy::single_attempt(Duration::from_millis(15), Duration::from_millis(100));

    let first = managed
        .execute(&ReadPrimaryValue, Priority::Normal, retry)
        .await
        .unwrap_err();
    assert!(matches!(
        first,
        ManagedSessionError::Session(hart_link::service::SessionError::Exchange(
            hart_link::ExchangeError::ResponseTimeout
        ))
    ));
    let started = client.snapshot().started;
    let second = managed
        .execute(&ReadPrimaryValue, Priority::Normal, retry)
        .await
        .unwrap_err();
    assert!(matches!(second, ManagedSessionError::CoolingDown { .. }));
    assert_eq!(client.snapshot().started, started);

    let value = managed
        .probe(&ReadPrimaryValue, Priority::Service, retry)
        .await
        .unwrap();
    assert!((value.value - 123.5).abs() < f32::EPSILON);
    let health = managed.snapshot();
    assert_eq!(health.successes, 1);
    assert_eq!(health.failures, 1);
    assert_eq!(health.cooldown_activations, 1);
    assert_eq!(health.cooldown_rejections, 1);
    assert_eq!(health.consecutive_transport_failures, 0);
    assert_eq!(health.cooldown_remaining, Duration::ZERO);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn managed_session_adapts_only_registered_read_only_commands() {
    let polling = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    let (client, runner) = LinkBuilder::new(host)
        .late_response_guard(Duration::ZERO)
        .build()
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(polling).run(device_channel));
    let session = DeviceSession::from_identity(client.clone(), polling, test_identity()).unwrap();
    let managed = ManagedDeviceSession::new(
        session,
        DeviceHealthOptions::default().with_adaptive(Some(
            AdaptiveTiming::default()
                .with_minimum(Duration::from_millis(20))
                .with_maximum(Duration::from_millis(50))
                .with_margin(Duration::from_millis(20)),
        )),
    )
    .unwrap();
    let retry = RetryPolicy::default()
        .with_response_timeout(Duration::from_millis(100))
        .with_total_timeout(Duration::from_secs(1));

    managed
        .execute(&ReadPrimaryValue, Priority::Normal, retry)
        .await
        .unwrap();
    managed
        .execute(&ReadPrimaryValue, Priority::Normal, retry)
        .await
        .unwrap();
    let health = managed.snapshot();
    assert!(health.learned_response.is_some());
    assert_eq!(health.adaptive_attempts, 1);
    assert_eq!(health.adaptive_fallbacks, 0);

    let forged_vendor_read = RawOperation::read(60_000u16, Vec::new());
    let error = managed
        .probe(&forged_vendor_read, Priority::Service, retry)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ManagedSessionError::UnsafeProbe { command: 60_000 }
    ));
    assert_eq!(managed.snapshot().adaptive_attempts, 1);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn adaptive_timeout_falls_back_within_the_original_retry_budget() {
    let polling = Address::polling(3, Master::Primary).unwrap();
    let unique = test_identity().unique_address(Master::Primary).unwrap();
    let channel = AdaptiveScriptChannel::new(unique);
    let sends = channel.sends.clone();
    let (client, runner) = LinkBuilder::new(channel)
        .late_response_guard(Duration::ZERO)
        .build()
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    let session = DeviceSession::from_identity(client.clone(), polling, test_identity()).unwrap();
    let managed = ManagedDeviceSession::new(
        session,
        DeviceHealthOptions::default().with_adaptive(Some(
            AdaptiveTiming::default()
                .with_minimum(Duration::from_millis(5))
                .with_maximum(Duration::from_millis(20))
                .with_margin(Duration::from_millis(5)),
        )),
    )
    .unwrap();
    let retry = RetryPolicy::default()
        .with_transport_retries(1)
        .with_response_timeout(Duration::from_millis(50))
        .with_total_timeout(Duration::from_millis(200));

    managed
        .execute(&ReadPrimaryValue, Priority::Normal, retry)
        .await
        .unwrap();
    let value = managed
        .execute(&ReadPrimaryValue, Priority::Normal, retry)
        .await
        .unwrap();
    assert!((value.value - 123.5).abs() < f32::EPSILON);
    assert_eq!(sends.load(std::sync::atomic::Ordering::Relaxed), 3);
    let health = managed.snapshot();
    assert_eq!(health.successes, 2);
    assert_eq!(health.failures, 0);
    assert_eq!(health.adaptive_attempts, 1);
    assert_eq!(health.adaptive_fallbacks, 1);

    drop(managed);
    drop(client);
    runner_task.await.unwrap();
}

#[tokio::test]
async fn device_snapshot_preserves_successes_when_other_commands_fail() {
    let polling = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    let (client, runner) = LinkBuilder::new(host).build().unwrap();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(polling).run(device_channel));
    let session = DeviceSession::from_identity(client.clone(), polling, test_identity()).unwrap();

    let identity_only = session.snapshot(SnapshotOptions::IDENTITY_ONLY).await;
    assert!(!identity_only.has_errors());
    assert!(matches!(
        identity_only.primary_value,
        SnapshotField::NotRequested
    ));
    assert_eq!(client.snapshot().started, 0);

    let snapshot = session.snapshot(SnapshotOptions::FULL).await;
    assert!(snapshot.primary_value.value().is_some());
    assert!(snapshot.loop_signal.value().is_some());
    assert_eq!(snapshot.dynamic_values.error().unwrap().command.get(), 3);
    assert_eq!(snapshot.tag.error().unwrap().command.get(), 13);
    assert_eq!(snapshot.long_tag.error().unwrap().command.get(), 20);
    assert_eq!(
        snapshot.additional_status.value().unwrap().bytes,
        vec![1, 2, 3, 4]
    );
    assert!(snapshot.has_errors());

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn total_deadline_limits_waiting_for_queue_capacity() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, _peer) = MemoryChannel::pair(1);
    let config = hart_link::LinkConfig {
        normal_capacity: 1,
        ..hart_link::LinkConfig::default()
    };
    let (client, _runner) = hart_link::create_link(host, config);
    let first = client
        .start(
            Request::new(address, 0u8, vec![]),
            Priority::Normal,
            RetryPolicy::default(),
        )
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    let error = client
        .request(
            Request::new(address, 1u8, vec![]),
            Priority::Normal,
            RetryPolicy {
                total_timeout: Duration::from_millis(20),
                ..RetryPolicy::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, hart_link::ExchangeError::Deadline));
    assert!(started.elapsed() < Duration::from_millis(200));
    assert_eq!(client.snapshot().queued_normal, 1);
    drop(first);
}

#[tokio::test]
async fn runner_routes_burst_frames_while_the_queue_is_idle() {
    use hart_link::{
        CommandCode, PhysicalLayer,
        channel::ByteChannel,
        service::{BurstHub, BurstKey},
    };

    let address = Address::polling(3, Master::Primary).unwrap();
    let (host, mut device) = MemoryChannel::pair(4);
    let hub = BurstHub::default();
    let mut subscription = hub
        .subscribe(
            BurstKey {
                address,
                command: CommandCode::new(1),
            },
            1,
        )
        .unwrap();
    let (client, runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.with_burst_hub(hub).run());
    let frame = Frame {
        preambles: 5,
        kind: FrameKind::Burst,
        physical_layer: PhysicalLayer::Fsk,
        address,
        expansion: vec![],
        wire_command: 1,
        payload: vec![0, 0, 45, 0, 0, 0, 0],
        repair: None,
    }
    .encode()
    .unwrap();
    device.send(&frame).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), subscription.receive())
            .await
            .unwrap()
            .unwrap()
            .reply
            .command,
        CommandCode::new(1)
    );
    drop(client);
    runner_task.abort();
}

#[test]
fn validated_link_constructor_rejects_ambiguous_zero_limits() {
    let (host, _peer) = MemoryChannel::pair(1);
    let result = hart_link::try_create_link(
        host,
        hart_link::LinkConfig {
            normal_capacity: 0,
            ..hart_link::LinkConfig::default()
        },
    );
    assert!(matches!(
        result,
        Err(hart_link::LinkConfigError::NormalCapacity)
    ));
}

#[test]
fn validated_link_constructor_rejects_resource_amplifying_limits() {
    let (host, _peer) = MemoryChannel::pair(1);
    assert!(matches!(
        hart_link::try_create_link(
            host,
            hart_link::LinkConfig {
                service_capacity: usize::MAX,
                ..hart_link::LinkConfig::default()
            }
        ),
        Err(hart_link::LinkConfigError::ServiceCapacityLimit(_))
    ));

    let (host, _peer) = MemoryChannel::pair(1);
    assert!(matches!(
        LinkBuilder::new(host).event_capacity(usize::MAX).build(),
        Err(hart_link::LinkBuildError::EventCapacityLimit(_))
    ));

    let (host, _peer) = MemoryChannel::pair(1);
    let mut config = hart_link::LinkConfig::default();
    config.decoder.buffer_capacity = 64;
    assert!(matches!(
        hart_link::try_create_link(host, config),
        Err(hart_link::LinkConfigError::DecoderFrameCapacity { .. })
    ));

    let (host, _peer) = MemoryChannel::pair(1);
    assert!(matches!(
        LinkBuilder::new(host)
            .late_response_guard(hart_link::MAXIMUM_LATE_RESPONSE_GUARD + Duration::from_secs(1))
            .build(),
        Err(hart_link::LinkBuildError::LateResponseGuard)
    ));

    let (host, _peer) = MemoryChannel::pair(1);
    assert!(matches!(
        LinkBuilder::new(host)
            .default_retry(RetryPolicy {
                response_timeout: Duration::MAX,
                ..RetryPolicy::default()
            })
            .build(),
        Err(hart_link::LinkBuildError::RetryPolicy(
            hart_link::RetryPolicyError::ResponseTimeoutLimit
        ))
    ));
}

#[test]
fn request_rejects_resource_pinning_timeouts_before_enqueueing() {
    let (channel, _peer) = MemoryChannel::pair(2);
    let (client, _runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let request = Request::new(Address::polling(1, Master::Primary).unwrap(), 0u8, vec![]);
    let policy = RetryPolicy::default()
        .with_total_timeout(hart_link::MAXIMUM_RETRY_DURATION + Duration::from_secs(1));
    let error = client
        .try_start(request, Priority::Normal, policy)
        .unwrap_err();
    assert!(matches!(
        error,
        hart_link::StartError::RetryPolicy(hart_link::RetryPolicyError::TotalTimeoutLimit)
    ));
    assert_eq!(client.snapshot().queued_normal, 0);
}

#[tokio::test]
async fn request_rejects_a_zero_timeout_before_enqueueing() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, _peer) = MemoryChannel::pair(1);
    let (client, _runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let error = client
        .request(
            Request::new(address, 0u8, vec![]),
            Priority::Normal,
            RetryPolicy {
                response_timeout: Duration::ZERO,
                ..RetryPolicy::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        hart_link::ExchangeError::RetryPolicy(hart_link::RetryPolicyError::ResponseTimeout)
    ));
    assert_eq!(client.snapshot().queued_normal, 0);
}

#[tokio::test]
async fn request_rejects_an_oversized_transmit_prefix_before_enqueueing() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, _peer) = MemoryChannel::pair(1);
    let (client, _runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let error = client
        .start_with_prefix(
            Request::new(address, 0u8, vec![]),
            vec![0; hart_link::MAXIMUM_TRANSMIT_PREFIX + 1],
            Priority::Service,
            RetryPolicy::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, hart_link::StartError::TransmitPrefix(_)));
    assert_eq!(client.snapshot().queued_service, 0);
}

#[tokio::test]
async fn request_rejects_an_oversized_frame_before_enqueueing() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, _peer) = MemoryChannel::pair(1);
    let (client, _runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let error = client
        .try_start(
            Request::new(address, 1u8, vec![0; 256]),
            Priority::Normal,
            RetryPolicy::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        hart_link::service::StartError::Operation(_)
    ));
    assert_eq!(client.snapshot().queued_normal, 0);
}

#[tokio::test]
async fn an_indeterminate_send_stops_the_runner_instead_of_reusing_the_stream() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (client, runner) = hart_link::create_link(HangingSend, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.run());
    let error = client
        .request(
            Request::new(address, 0u8, vec![]),
            Priority::Normal,
            RetryPolicy {
                response_timeout: Duration::from_millis(10),
                total_timeout: Duration::from_millis(20),
                ..RetryPolicy::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, hart_link::ExchangeError::SendTimeout));
    runner_task.await.unwrap();
    assert!(matches!(
        client.try_start(
            Request::new(address, 0u8, vec![]),
            Priority::Normal,
            RetryPolicy::default(),
        ),
        Err(hart_link::service::StartError::Closed)
    ));
}

#[tokio::test]
async fn total_deadline_covers_missing_response() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    host.set_faults(FaultPlan {
        drop_next: true,
        ..FaultPlan::default()
    })
    .await;
    let (client, runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));
    let policy = RetryPolicy {
        transport_retries: 0,
        busy_retries: 0,
        response_timeout: Duration::from_millis(20),
        total_timeout: Duration::from_millis(30),
        ..RetryPolicy::default()
    };
    let error = client
        .execute(address, &ReadDeviceIdentity, Priority::Normal, policy)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        hart_link::ExchangeError::ResponseTimeout | hart_link::ExchangeError::Deadline
    ));
    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn late_response_guard_drains_an_uncorrelatable_timed_out_reply() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    device_channel
        .set_faults(FaultPlan {
            latency: Duration::from_millis(30),
            ..FaultPlan::default()
        })
        .await;
    let (client, runner) = LinkBuilder::new(host)
        .late_response_guard(Duration::from_millis(80))
        .build()
        .unwrap();
    let mut events = client.subscribe();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));
    let first = client
        .request(
            Request::new(address, 1u8, vec![]),
            Priority::Normal,
            RetryPolicy::default()
                .with_transport_retries(0)
                .with_response_timeout(Duration::from_millis(10))
                .with_total_timeout(Duration::from_millis(20)),
        )
        .await;
    assert!(matches!(
        first,
        Err(hart_link::ExchangeError::ResponseTimeout)
    ));

    let second = client
        .request(
            Request::new(address, 1u8, vec![]),
            Priority::Normal,
            RetryPolicy::default()
                .with_transport_retries(0)
                .with_response_timeout(Duration::from_millis(100))
                .with_total_timeout(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    assert_eq!(second.command.get(), 1);
    let mut late_seen = false;
    while let Ok(event) = events.try_recv() {
        late_seen |= matches!(event, LinkEvent::LateResponse(_));
    }
    assert!(late_seen);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn busy_and_delayed_response_have_independent_retry_limits() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let request = Request::new(address, 1u8, vec![]);
    let request_bytes = request.to_frame().unwrap().encode().unwrap();
    let response = |code| {
        Frame {
            preambles: 5,
            kind: FrameKind::Response,
            physical_layer: PhysicalLayer::Fsk,
            address,
            expansion: vec![],
            wire_command: 1,
            payload: vec![code, 0],
            repair: None,
        }
        .encode()
        .unwrap()
    };
    let channel = ReplayChannel::new([
        ReplayStep::Expect(request_bytes.clone()),
        ReplayStep::Provide(response(32)),
        ReplayStep::Expect(request_bytes.clone()),
        ReplayStep::Provide(response(33)),
        ReplayStep::Expect(request_bytes.clone()),
        ReplayStep::Provide(response(34)),
        ReplayStep::Expect(request_bytes),
        ReplayStep::Provide(response(0)),
    ])
    .expect("bounded replay scenario");
    let (client, runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.run());
    let reply = client
        .request(
            request,
            Priority::Normal,
            RetryPolicy {
                transport_retries: 0,
                busy_retries: 1,
                delayed_response_polls: 2,
                retry_delay: Duration::ZERO,
                response_timeout: Duration::from_millis(100),
                total_timeout: Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(reply.response_code, 0);
    assert_eq!(client.snapshot().completed, 1);
    drop(client);
    runner_task.await.unwrap();
}

#[tokio::test]
async fn identical_read_only_requests_share_one_successful_exchange() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let request = Request::new(address, 1u8, vec![]);
    let request_bytes = request.to_frame().unwrap().encode().unwrap();
    let response = Frame {
        preambles: 5,
        kind: FrameKind::Response,
        physical_layer: PhysicalLayer::Fsk,
        address,
        expansion: vec![],
        wire_command: 1,
        payload: vec![0, 0],
        repair: None,
    }
    .encode()
    .unwrap();
    let channel = ReplayChannel::new([
        ReplayStep::Expect(request_bytes),
        ReplayStep::Provide(response),
    ])
    .expect("bounded replay scenario");
    let (client, runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let first = client
        .start(request.clone(), Priority::Normal, RetryPolicy::default())
        .await
        .unwrap();
    let second = client
        .start(request, Priority::Normal, RetryPolicy::default())
        .await
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    assert_eq!(first.wait().await.unwrap().response_code, 0);
    assert_eq!(second.wait().await.unwrap().response_code, 0);
    let snapshot = client.snapshot();
    assert_eq!(snapshot.started, 1);
    assert_eq!(snapshot.completed, 2);
    assert_eq!(snapshot.coalesced, 1);
    assert_eq!(snapshot.queued_normal, 0);
    drop(client);
    runner_task.await.unwrap();
}

#[tokio::test]
async fn action_requests_are_never_coalesced() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let request = Request::new(address, 41u8, vec![]);
    let request_bytes = request.to_frame().unwrap().encode().unwrap();
    let response = Frame {
        preambles: 5,
        kind: FrameKind::Response,
        physical_layer: PhysicalLayer::Fsk,
        address,
        expansion: vec![],
        wire_command: 41,
        payload: vec![0, 0],
        repair: None,
    }
    .encode()
    .unwrap();
    let channel = ReplayChannel::new([
        ReplayStep::Expect(request_bytes.clone()),
        ReplayStep::Provide(response.clone()),
        ReplayStep::Expect(request_bytes),
        ReplayStep::Provide(response),
    ])
    .expect("bounded replay scenario");
    let (client, runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let first = client
        .start(request.clone(), Priority::Normal, RetryPolicy::default())
        .await
        .unwrap();
    let second = client
        .start(request, Priority::Normal, RetryPolicy::default())
        .await
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    first.wait().await.unwrap();
    second.wait().await.unwrap();
    assert_eq!(client.snapshot().started, 2);
    assert_eq!(client.snapshot().coalesced, 0);
    drop(client);
    runner_task.await.unwrap();
}

#[tokio::test]
async fn calibration_timeout_never_repeats_the_physical_request() {
    let channel = SilentChannel::default();
    let sends = channel.sends.clone();
    let (client, runner) = LinkBuilder::new(channel)
        .late_response_guard(Duration::ZERO)
        .build()
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    let error = client
        .request(
            Request::new(
                Address::polling(1, Master::Primary).unwrap(),
                36u8,
                Vec::new(),
            ),
            Priority::Normal,
            RetryPolicy::default()
                .with_transport_retries(5)
                .with_response_timeout(Duration::from_millis(10))
                .with_total_timeout(Duration::from_millis(100)),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, hart_link::ExchangeError::ResponseTimeout));
    assert_eq!(sends.load(std::sync::atomic::Ordering::Relaxed), 1);
    drop(client);
    runner_task.await.unwrap();
}

#[tokio::test]
async fn service_weight_prevents_normal_queue_starvation() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let ordered = [10u8, 11, 20, 12, 13, 21, 14];
    let mut steps = Vec::new();
    for command in ordered {
        let request = Request::new(address, command, vec![])
            .to_frame()
            .unwrap()
            .encode()
            .unwrap();
        let response = Frame {
            preambles: 5,
            kind: FrameKind::Response,
            physical_layer: PhysicalLayer::Fsk,
            address,
            expansion: vec![],
            wire_command: command,
            payload: vec![0, 0],
            repair: None,
        }
        .encode()
        .unwrap();
        steps.push(ReplayStep::Expect(request));
        steps.push(ReplayStep::Provide(response));
    }
    let channel = ReplayChannel::new(steps).expect("bounded replay scenario");
    let scheduling = hart_link::QueueScheduling::custom(2).unwrap();
    let config = hart_link::LinkConfig::default().with_queue_scheduling(scheduling);
    let (client, runner) = hart_link::create_link(channel, config);
    let mut waits = Vec::new();
    for command in 10u8..=14 {
        waits.push(
            client
                .start(
                    Request::new(address, command, vec![]),
                    Priority::Service,
                    RetryPolicy::default(),
                )
                .await
                .unwrap(),
        );
    }
    for command in 20u8..=21 {
        waits.push(
            client
                .start(
                    Request::new(address, command, vec![]),
                    Priority::Normal,
                    RetryPolicy::default(),
                )
                .await
                .unwrap(),
        );
    }
    let runner_task = tokio::spawn(runner.run());
    for wait in waits {
        wait.wait().await.unwrap();
    }
    assert_eq!(client.snapshot().completed, 7);
    drop(client);
    runner_task.await.unwrap();
}

#[test]
fn queue_scheduling_presets_are_valid_and_explicit() {
    assert_eq!(hart_link::QueueScheduling::EQUAL.service_weight(), 1);
    assert_eq!(
        hart_link::QueueScheduling::MAXIMUM_SERVICE.service_weight(),
        u8::MAX
    );
    assert_eq!(
        hart_link::QueueScheduling::custom(4)
            .unwrap()
            .service_weight(),
        4
    );
    assert!(hart_link::QueueScheduling::custom(0).is_err());

    let config =
        hart_link::LinkConfig::default().with_queue_scheduling(hart_link::QueueScheduling::EQUAL);
    assert_eq!(config.service_weight, 1);
}

#[test]
fn managed_device_limits_reject_zero_reversed_and_oversized_timing() {
    assert!(matches!(
        AdaptiveTiming::default()
            .with_minimum(Duration::ZERO)
            .validate(),
        Err(hart_link::DeviceHealthConfigError::ZeroAdaptiveTimeout)
    ));
    assert!(matches!(
        AdaptiveTiming::default()
            .with_minimum(Duration::from_secs(2))
            .with_maximum(Duration::from_secs(1))
            .validate(),
        Err(hart_link::DeviceHealthConfigError::AdaptiveOrder)
    ));
    assert!(matches!(
        AdaptiveTiming::default()
            .with_maximum(hart_link::MAXIMUM_RETRY_DURATION + Duration::from_nanos(1))
            .validate(),
        Err(hart_link::DeviceHealthConfigError::AdaptiveLimit)
    ));
    assert!(matches!(
        DeviceHealthOptions::default()
            .with_cooldown(hart_link::MAXIMUM_RETRY_DURATION + Duration::from_nanos(1))
            .validate(),
        Err(hart_link::DeviceHealthConfigError::CooldownLimit)
    ));
}

#[test]
fn command_policy_rejects_before_queue_capacity_is_used() {
    let (channel, _peer) = MemoryChannel::pair(2);
    let (client, _runner) = LinkBuilder::new(channel)
        .command_policy(hart_link::CommandPolicy::read_only())
        .build()
        .unwrap();
    let mut events = client.subscribe();
    let request = Request::new(Address::polling(1, Master::Primary).unwrap(), 42u8, vec![])
        .with_retry_safety(hart_link::catalog::OperationSafety::Action);
    let error = client.try_start_default(request).unwrap_err();
    assert!(matches!(
        error,
        hart_link::StartError::CommandDenied {
            command: 42,
            safety: hart_link::catalog::OperationSafety::Action,
            priority: Priority::Normal,
        }
    ));
    assert!(matches!(
        events.try_recv().unwrap(),
        hart_link::LinkEvent::Denied {
            command: 42,
            safety: hart_link::OperationSafety::Action,
            priority: Priority::Normal,
        }
    ));
    let snapshot = client.snapshot();
    assert_eq!(snapshot.denied, 1);
    assert_eq!(snapshot.queued_service + snapshot.queued_normal, 0);
}

#[test]
fn strict_safety_policy_cannot_be_bypassed_by_request_metadata() {
    let vendor_command = hart_link::CommandCode::new(60000);
    let (channel, _peer) = MemoryChannel::pair(2);
    let (strict, _runner) = LinkBuilder::new(channel)
        .command_policy(hart_link::CommandPolicy::read_only())
        .build()
        .unwrap();
    let address = Address::polling(1, Master::Primary).unwrap();
    let forged = Request::new(address, vendor_command, vec![])
        .with_retry_safety(hart_link::OperationSafety::ReadOnly);
    assert!(matches!(
        strict.try_start_default(forged),
        Err(hart_link::StartError::CommandDenied { command: 60000, .. })
    ));

    let (channel, _peer) = MemoryChannel::pair(2);
    let reviewed_vendor = hart_link::CommandPolicy::read_only()
        .or(hart_link::CommandPolicy::allow_commands([vendor_command]));
    let (explicit, _runner) = LinkBuilder::new(channel)
        .command_policy(reviewed_vendor)
        .build()
        .unwrap();
    let accepted = explicit
        .try_start_default(
            Request::new(address, vendor_command, vec![])
                .with_retry_safety(hart_link::OperationSafety::ReadOnly),
        )
        .unwrap();
    assert_eq!(explicit.snapshot().queued_normal, 1);
    drop(accepted);
}

#[test]
fn routing_is_applied_before_queue_specific_admission() {
    let (channel, _peer) = MemoryChannel::pair(2);
    let policy = hart_link::CommandPolicy::custom(|context| {
        context.request.command == hart_link::CommandCode::new(0)
            && context.priority == Priority::Service
    });
    let routing = hart_link::CommandRouting::service_commands([hart_link::CommandCode::new(0)]);
    let (client, _runner) = LinkBuilder::new(channel)
        .command_routing(routing)
        .command_policy(policy)
        .build()
        .unwrap();
    let request = Request::new(Address::polling(1, Master::Primary).unwrap(), 0u8, vec![]);
    let _pending = client
        .try_start(request, Priority::Normal, RetryPolicy::default())
        .unwrap();
    assert_eq!(client.snapshot().queued_service, 1);
    assert_eq!(client.snapshot().queued_normal, 0);
}

#[test]
fn policy_sugar_composes_command_and_queue_filters() {
    let command_0 = hart_link::CommandCode::new(0);
    let command_42 = hart_link::CommandCode::new(42);
    let service_policy = hart_link::CommandPolicy::queue_allowlist(Priority::Service, [command_0]);
    let policy = service_policy.and(hart_link::CommandPolicy::deny_commands([command_42]));
    let routing = hart_link::CommandRouting::service_commands([command_0, command_42]);
    let (channel, _peer) = MemoryChannel::pair(4);
    let (client, _runner) = LinkBuilder::new(channel)
        .command_routing(routing)
        .command_policy(policy)
        .build()
        .unwrap();
    let address = Address::polling(1, Master::Primary).unwrap();

    let accepted = client
        .try_start_default(Request::new(address, 0u8, vec![]))
        .unwrap();
    let denied = client
        .try_start_default(
            Request::new(address, 42u8, vec![])
                .with_retry_safety(hart_link::OperationSafety::ReadOnly),
        )
        .unwrap_err();

    assert!(matches!(
        denied,
        hart_link::StartError::CommandDenied { command: 42, .. }
    ));
    assert_eq!(client.snapshot().queued_service, 1);
    assert_eq!(client.snapshot().denied, 1);
    drop(accepted);
}

#[tokio::test]
async fn single_queue_preserves_global_fifo_and_ignores_priority() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let mut steps = Vec::new();
    for command in [20u8, 10] {
        let request = Request::new(address, command, vec![])
            .to_frame()
            .unwrap()
            .encode()
            .unwrap();
        let response = Frame {
            preambles: 5,
            kind: FrameKind::Response,
            physical_layer: PhysicalLayer::Fsk,
            address,
            expansion: vec![],
            wire_command: command,
            payload: vec![0, 0],
            repair: None,
        }
        .encode()
        .unwrap();
        steps.push(ReplayStep::Expect(request));
        steps.push(ReplayStep::Provide(response));
    }
    let channel = ReplayChannel::new(steps).unwrap();
    let (client, runner) = LinkBuilder::new(channel).single_queue(8).build().unwrap();
    let first = client
        .start(
            Request::new(address, 20u8, vec![]),
            Priority::Normal,
            RetryPolicy::default(),
        )
        .await
        .unwrap();
    let second = client
        .start(
            Request::new(address, 10u8, vec![]),
            Priority::Service,
            RetryPolicy::default(),
        )
        .await
        .unwrap();
    assert_eq!(client.queue_mode(), hart_link::QueueMode::SingleFifo);
    assert_eq!(client.snapshot().queued_service, 0);
    assert_eq!(client.snapshot().queued_normal, 2);
    let runner_task = tokio::spawn(runner.run());
    first.wait().await.unwrap();
    second.wait().await.unwrap();
    drop(client);
    runner_task.await.unwrap();
}

#[tokio::test]
async fn discovery_gives_the_retry_a_complete_response_window() {
    let address = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    device_channel
        .set_faults(FaultPlan {
            drop_next: true,
            ..FaultPlan::default()
        })
        .await;
    let (client, runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));
    let profile = LineProfile {
        name: "retry-discovery".into(),
        timing: TimingProfile {
            connect_settle: Duration::ZERO,
            first_response: Duration::from_millis(20),
            steady_response: Duration::from_millis(20),
            scan_interval: Duration::ZERO,
        },
        modules: vec![ModuleWindow {
            module: 1,
            polling: PollingWindow::new(3, 3).unwrap(),
            address_shift: 0,
        }],
        discovery_preambles: 20,
        wake_prefix: Vec::new(),
    };

    let report = discover_line(&client, &profile, Master::Primary)
        .await
        .unwrap();
    assert_eq!(report.devices.len(), 1);
    assert_eq!(report.devices[0].identity.device_id, 0x12_34_56);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn discovery_revisits_only_missing_addresses_on_later_passes() {
    let address = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    device_channel
        .set_faults(FaultPlan {
            drop_next: true,
            ..FaultPlan::default()
        })
        .await;
    let retry = RetryPolicy::default()
        .with_transport_retries(0)
        .with_response_timeout(Duration::from_millis(20))
        .with_total_timeout(Duration::from_millis(20));
    let (client, runner) = LinkBuilder::new(host)
        .default_retry(retry)
        .event_capacity(32)
        .maximum_coalesced(8)
        .build()
        .unwrap();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));
    let profile = LineProfile {
        name: "multi-pass-discovery".into(),
        timing: TimingProfile {
            connect_settle: Duration::ZERO,
            first_response: Duration::from_millis(20),
            steady_response: Duration::from_millis(20),
            scan_interval: Duration::ZERO,
        },
        modules: vec![ModuleWindow {
            module: 1,
            polling: PollingWindow::new(3, 3).unwrap(),
            address_shift: 0,
        }],
        discovery_preambles: 20,
        wake_prefix: Vec::new(),
    };
    let report = discover_line_with_options(
        &client,
        &profile,
        Master::Primary,
        DiscoveryOptions {
            passes: std::num::NonZeroU8::new(2).unwrap(),
            priority: Priority::Service,
            retry,
            pass_delay: Duration::ZERO,
            preamble_hints: DiscoveryHints::new(),
        },
    )
    .await
    .unwrap();
    assert_eq!(report.devices.len(), 1);
    assert_eq!(report.attempts, 2);
    assert_eq!(report.passes_completed, 2);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn discovery_stops_after_the_first_complete_pass() {
    let address = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    let retry = RetryPolicy::default()
        .with_transport_retries(0)
        .with_response_timeout(Duration::from_millis(50))
        .with_total_timeout(Duration::from_millis(50));
    let (client, runner) = LinkBuilder::new(host).default_retry(retry).build().unwrap();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));
    let profile = LineProfile {
        name: "complete-discovery".into(),
        timing: TimingProfile {
            connect_settle: Duration::ZERO,
            first_response: Duration::from_millis(50),
            steady_response: Duration::from_millis(50),
            scan_interval: Duration::from_secs(1),
        },
        modules: vec![ModuleWindow {
            module: 1,
            polling: PollingWindow::new(3, 3).unwrap(),
            address_shift: 0,
        }],
        discovery_preambles: 20,
        wake_prefix: Vec::new(),
    };
    let report = discover_line_with_options(
        &client,
        &profile,
        Master::Primary,
        DiscoveryOptions::default()
            .with_passes(std::num::NonZeroU8::new(3).unwrap())
            .with_retry(retry)
            .with_pass_delay(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    assert_eq!(report.attempts, 1);
    assert_eq!(report.passes_completed, 1);
    assert_eq!(report.devices.len(), 1);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[tokio::test]
async fn stale_discovery_hint_falls_back_to_conservative_preambles() {
    let address = Address::polling(3, Master::Primary).unwrap();
    let (host, device_channel) = MemoryChannel::pair(8);
    host.set_faults(FaultPlan {
        drop_next: true,
        ..FaultPlan::default()
    })
    .await;
    let retry = RetryPolicy::default()
        .with_transport_retries(0)
        .with_response_timeout(Duration::from_millis(30))
        .with_total_timeout(Duration::from_millis(30));
    let (client, runner) = LinkBuilder::new(host).default_retry(retry).build().unwrap();
    let runner_task = tokio::spawn(runner.run());
    let device_task = tokio::spawn(test_device(address).run(device_channel));
    let profile = LineProfile {
        name: "hint-fallback".into(),
        timing: TimingProfile {
            connect_settle: Duration::ZERO,
            first_response: Duration::from_millis(50),
            steady_response: Duration::from_millis(30),
            scan_interval: Duration::ZERO,
        },
        modules: vec![ModuleWindow {
            module: 1,
            polling: PollingWindow::new(3, 3).unwrap(),
            address_shift: 0,
        }],
        discovery_preambles: 20,
        wake_prefix: Vec::new(),
    };
    let mut hints = DiscoveryHints::new();
    hints.insert(address, 5).unwrap();
    let report = discover_line_with_options(
        &client,
        &profile,
        Master::Primary,
        DiscoveryOptions::default()
            .with_retry(retry)
            .with_preamble_hints(hints),
    )
    .await
    .unwrap();
    assert_eq!(report.attempts, 2);
    assert_eq!(report.hinted_attempts, 1);
    assert_eq!(report.hint_fallbacks, 1);
    assert_eq!(report.devices.len(), 1);

    drop(client);
    runner_task.abort();
    device_task.abort();
}

#[test]
fn discovery_hints_reject_unusable_values() {
    let mut hints = DiscoveryHints::new();
    assert!(
        hints
            .insert(Address::polling(1, Master::Primary).unwrap(), 1)
            .is_err()
    );
    assert!(
        hints
            .insert(Address::unique(1, 1, Master::Primary).unwrap(), 5,)
            .is_err()
    );
}

#[tokio::test]
async fn plan_data_prefix_never_turns_an_error_response_into_success() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let request = Request::new(address, 1u8, vec![]);
    let response = Frame {
        preambles: 5,
        kind: FrameKind::Response,
        physical_layer: PhysicalLayer::Fsk,
        address,
        expansion: vec![],
        wire_command: 1,
        payload: vec![8, 0, 0xaa],
        repair: None,
    }
    .encode()
    .unwrap();
    let channel = ReplayChannel::new([
        ReplayStep::Expect(request.to_frame().unwrap().encode().unwrap()),
        ReplayStep::Provide(response),
    ])
    .expect("bounded replay scenario");
    let (client, runner) = hart_link::create_link(channel, hart_link::LinkConfig::default());
    let runner_task = tokio::spawn(runner.run());
    let report = Plan {
        steps: vec![PlanStep {
            name: "error-prefix".into(),
            request,
            priority: Priority::Normal,
            retry: RetryPolicy::default(),
            expectation: PlanExpectation::DataPrefix(vec![0xaa]),
        }],
        continue_after_failure: false,
    }
    .execute(&client)
    .await;
    assert!(!report.steps[0].expectation_met);
    drop(client);
    runner_task.abort();
}

#[tokio::test]
async fn dropping_the_waiter_drains_an_inflight_exchange_before_cancelling_interest() {
    let address = Address::polling(1, Master::Primary).unwrap();
    let (host, peer) = MemoryChannel::pair(8);
    peer.set_faults(FaultPlan {
        latency: Duration::from_millis(25),
        ..FaultPlan::default()
    })
    .await;
    let device_task = tokio::spawn(test_device(address).run(peer));
    let (client, runner) = hart_link::create_link(host, hart_link::LinkConfig::default());
    let mut events = client.subscribe();
    let runner_task = tokio::spawn(runner.run());
    let pending = client
        .start(
            Request::new(address, 0u8, vec![]),
            Priority::Normal,
            RetryPolicy {
                response_timeout: Duration::from_secs(5),
                total_timeout: Duration::from_secs(10),
                ..RetryPolicy::default()
            },
        )
        .await
        .unwrap();
    let request_id = pending.id();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(events.recv().await, Ok(LinkEvent::Started { id, .. }) if id == request_id)
            {
                break;
            }
        }
    })
    .await
    .unwrap();

    drop(pending);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(events.recv().await, Ok(LinkEvent::Cancelled { id }) if id == request_id) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(client.snapshot().cancelled, 1);
    assert_eq!(client.snapshot().failed, 0);

    drop(client);
    runner_task.await.unwrap();
    device_task.await.unwrap().unwrap();
}

#[cfg(feature = "tcp")]
#[test]
fn tcp_configuration_rejects_zero_or_implausible_durations() {
    use hart_link::channel::TcpOptions;

    assert!(
        TcpOptions::default()
            .with_keepalive(Some(Duration::ZERO))
            .validate()
            .is_err()
    );
    assert!(
        TcpOptions::default()
            .with_keepalive(Some(Duration::MAX))
            .validate()
            .is_err()
    );
}

#[tokio::test]
async fn emulator_strict_configuration_rejects_resource_amplification() {
    assert!(MemoryChannel::try_pair(0).is_err());
    assert!(MemoryChannel::try_pair(usize::MAX).is_err());

    let (channel, _peer) = MemoryChannel::pair(8);
    assert!(
        channel
            .try_set_faults(FaultPlan::default().with_fragment_size(0))
            .await
            .is_err()
    );
    assert!(
        channel
            .try_set_faults(FaultPlan::default().with_noise_prefix(vec![
                0;
                hart_link::emulator::MAXIMUM_EMULATOR_NOISE_PREFIX
                    + 1
            ]),)
            .await
            .is_err()
    );
}
