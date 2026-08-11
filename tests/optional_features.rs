#![cfg(all(
    feature = "device-info",
    feature = "hart-ip",
    feature = "wireless-hart"
))]

use hart_link::{
    device::{
        DataSchema, DeviceCatalog, DeviceKey, DeviceProfile, DynamicValue, EnumChoice, FieldKind,
        FieldSpec,
    },
    ip::{
        IpPacket, IpSession, IpTimeouts, MessageId, MessageType, PROTOCOL_VERSION_1, SessionOptions,
    },
    mesh::{
        Cell, CellDirection, DeviceId, KeyMaterial, LinkHealth, MeshGraph, MeshManager, NetworkId,
        NodeState, ReplayWindow, Schedule,
    },
};

#[test]
fn dynamic_schema_preserves_unknown_enum_and_tail() {
    let schema = DataSchema {
        fields: vec![
            FieldSpec {
                name: "mode".into(),
                field_type: FieldKind::Enum8 {
                    choices: vec![EnumChoice {
                        value: 1,
                        name: "enabled".into(),
                    }],
                },
            },
            FieldSpec {
                name: "value".into(),
                field_type: FieldKind::F32,
            },
        ],
        preserve_tail: true,
    };
    let mut bytes = vec![99];
    bytes.extend_from_slice(&42.5f32.to_be_bytes());
    bytes.extend_from_slice(&[0xaa, 0xbb]);
    let record = schema.decode(&bytes).unwrap();
    assert_eq!(
        record.values["mode"],
        DynamicValue::Enumeration {
            code: 99,
            name: None,
        }
    );
    assert_eq!(record.tail, vec![0xaa, 0xbb]);
}

#[test]
fn device_catalog_requires_an_exact_valid_revision() {
    let profile = DeviceProfile {
        key: DeviceKey {
            expanded_type: 0x1234,
            device_revision: 2,
        },
        display_name: "example".into(),
        source_revision: "1.0".into(),
        responses: std::collections::BTreeMap::new(),
        response_codes: [(
            1,
            [(
                8,
                hart_link::device::ResponseCodeDefinition {
                    status: hart_link::device::ResponseCodeStatus::Warning,
                    description: "value was not updated".into(),
                },
            )]
            .into(),
        )]
        .into(),
    };
    let mut catalog = DeviceCatalog::new();
    catalog.insert(profile).unwrap();
    let profile = catalog
        .resolve(DeviceKey {
            expanded_type: 0x1234,
            device_revision: 2,
        })
        .unwrap();
    assert_eq!(profile.warning_codes(hart_link::CommandCode::new(1)), [8]);
    assert!(
        catalog
            .resolve(DeviceKey {
                expanded_type: 0x1234,
                device_revision: 2,
            })
            .is_some()
    );
    assert!(
        catalog
            .resolve(DeviceKey {
                expanded_type: 0x1234,
                device_revision: 3,
            })
            .is_none()
    );
}

#[test]
fn device_schema_and_catalog_reject_resource_amplification() {
    let schema = DataSchema {
        fields: vec![FieldSpec {
            name: "empty".into(),
            field_type: FieldKind::Bytes { length: 0 },
        }],
        preserve_tail: false,
    };
    assert!(matches!(
        schema.validate(),
        Err(hart_link::device::SchemaError::ZeroLengthField(_))
    ));

    let mut catalog = DeviceCatalog::with_limits(hart_link::device::CatalogLimits {
        profiles: 1,
        json_bytes: 64,
    })
    .unwrap();
    assert!(matches!(
        catalog.load_json(&[b' '; 65]),
        Err(hart_link::device::CatalogLoadError::JsonLimit { .. })
    ));
}

#[test]
fn hart_ip_packet_round_trip_and_limits() {
    let packet = IpPacket {
        version: PROTOCOL_VERSION_1,
        message_type: MessageType::Request,
        message_id: 7,
        status: 0,
        sequence: 123,
        body: vec![1, 2, 3, 4],
    };
    let encoded = packet.encode().unwrap();
    assert_eq!(IpPacket::decode(&encoded, 64).unwrap(), packet);
    assert!(IpPacket::decode(&encoded, 2).is_err());
    let mut reserved = encoded;
    reserved[1] |= 0x10;
    assert!(matches!(
        IpPacket::decode(&reserved, 64),
        Err(hart_link::ip::IpError::ReservedMessageBits(0x10))
    ));
}

#[test]
fn hart_ip_session_clamps_resource_limits() {
    let (stream, _peer) = tokio::io::duplex(64);
    let session = IpSession::new(stream)
        .with_maximum_body(usize::MAX)
        .with_maximum_published(usize::MAX)
        .with_maximum_published_bytes(usize::MAX)
        .with_maximum_unmatched(usize::MAX);
    assert_eq!(session.maximum_body(), hart_link::ip::MAXIMUM_IP_BODY_BYTES);
    assert_eq!(
        session.maximum_published(),
        hart_link::ip::MAXIMUM_IP_PUBLISHED_PACKETS
    );
    assert_eq!(
        session.maximum_published_bytes(),
        hart_link::ip::MAXIMUM_IP_PUBLISHED_BYTES
    );
    assert_eq!(
        session.maximum_unmatched(),
        hart_link::ip::MAXIMUM_IP_UNMATCHED_PACKETS
    );
    assert!(
        IpSession::new(tokio::io::duplex(64).0)
            .with_timeouts(IpTimeouts {
                io: std::time::Duration::MAX,
                exchange: std::time::Duration::from_secs(1),
            })
            .is_err()
    );
}

#[tokio::test]
async fn hart_ip_invalid_declared_length_poisoned_stream_framing() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::io::duplex(64);
    server_stream
        .write_all(&[PROTOCOL_VERSION_1, 1, 3, 0, 0, 1, 0, 4])
        .await
        .unwrap();
    let mut session = IpSession::new(client_stream);
    assert!(matches!(
        session.receive().await,
        Err(hart_link::ip::IpError::PacketLength { declared: 4, .. })
    ));
    assert!(!session.is_usable());
    assert!(matches!(
        session.receive().await,
        Err(hart_link::ip::IpError::StreamUnusable)
    ));
}

#[tokio::test]
async fn hart_ip_oversized_body_is_drained_before_the_next_packet() {
    use tokio::io::AsyncWriteExt;

    let (client_stream, mut server_stream) = tokio::io::duplex(256);
    let writer = tokio::spawn(async move {
        for body in [vec![1, 2, 3], vec![4, 5]] {
            server_stream
                .write_all(
                    &IpPacket {
                        version: PROTOCOL_VERSION_1,
                        message_type: MessageType::Response,
                        message_id: 3,
                        status: 0,
                        sequence: 1,
                        body,
                    }
                    .encode()
                    .unwrap(),
                )
                .await
                .unwrap();
        }
    });
    let mut session = IpSession::new(client_stream).with_maximum_body(2);
    assert!(matches!(
        session.receive().await,
        Err(hart_link::ip::IpError::BodyLimit(3))
    ));
    assert_eq!(session.receive().await.unwrap().body, [4, 5]);
    writer.await.unwrap();
}

#[tokio::test]
async fn hart_ip_direct_reads_have_a_real_timeout() {
    let (client_stream, _server_stream) = tokio::io::duplex(64);
    let mut session = IpSession::new(client_stream)
        .with_timeouts(IpTimeouts {
            io: std::time::Duration::from_millis(5),
            exchange: std::time::Duration::from_millis(10),
        })
        .unwrap();
    assert!(matches!(
        session.receive().await,
        Err(hart_link::ip::IpError::Timeout("read"))
    ));
    assert!(!session.is_usable());
    assert!(matches!(
        session.receive().await,
        Err(hart_link::ip::IpError::StreamUnusable)
    ));
}

#[tokio::test]
async fn hart_ip_session_opens_correlates_publish_and_closes() {
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

    async fn read_packet(stream: &mut (impl AsyncRead + Unpin)) -> IpPacket {
        let mut header = [0; IpPacket::HEADER_SIZE];
        stream.read_exact(&mut header).await.unwrap();
        let total = usize::from(u16::from_be_bytes([header[6], header[7]]));
        let mut bytes = header.to_vec();
        bytes.resize(total, 0);
        stream
            .read_exact(&mut bytes[IpPacket::HEADER_SIZE..])
            .await
            .unwrap();
        IpPacket::decode(&bytes, 1024).unwrap()
    }

    async fn write_packet(stream: &mut (impl AsyncWrite + Unpin), packet: IpPacket) {
        stream.write_all(&packet.encode().unwrap()).await.unwrap();
    }

    let (client_stream, mut server_stream) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move {
        let open = read_packet(&mut server_stream).await;
        assert_eq!(open.message_type, MessageType::Request);
        assert_eq!(open.message_id, u8::from(MessageId::SessionInitiate));
        assert_eq!(open.body, [1, 0, 0, 0xea, 0x60]);
        write_packet(
            &mut server_stream,
            IpPacket {
                version: PROTOCOL_VERSION_1,
                message_type: MessageType::Response,
                message_id: open.message_id,
                status: 0,
                sequence: open.sequence,
                body: vec![0xaa],
            },
        )
        .await;

        let request = read_packet(&mut server_stream).await;
        assert_eq!(request.message_id, u8::from(MessageId::TokenPassingPdu));
        assert_eq!(request.body, [1, 2, 3]);
        write_packet(
            &mut server_stream,
            IpPacket {
                version: PROTOCOL_VERSION_1,
                message_type: MessageType::Response,
                message_id: request.message_id,
                status: 0,
                sequence: request.sequence.wrapping_sub(1),
                body: vec![0xba, 0xd0],
            },
        )
        .await;
        write_packet(
            &mut server_stream,
            IpPacket {
                version: PROTOCOL_VERSION_1,
                message_type: MessageType::Publish,
                message_id: request.message_id,
                status: 0,
                sequence: request.sequence,
                body: vec![0xde, 0xad],
            },
        )
        .await;
        write_packet(
            &mut server_stream,
            IpPacket {
                version: PROTOCOL_VERSION_1,
                message_type: MessageType::Response,
                message_id: request.message_id,
                status: 0,
                sequence: request.sequence,
                body: vec![0xbe, 0xef],
            },
        )
        .await;

        let close = read_packet(&mut server_stream).await;
        assert_eq!(close.message_id, u8::from(MessageId::SessionClose));
        write_packet(
            &mut server_stream,
            IpPacket {
                version: PROTOCOL_VERSION_1,
                message_type: MessageType::Response,
                message_id: close.message_id,
                status: 0,
                sequence: close.sequence,
                body: vec![],
            },
        )
        .await;
    });

    let mut session = IpSession::new(client_stream);
    session.open(SessionOptions::default()).await.unwrap();
    assert!(session.is_open());
    let response = session.token_passing(&[1, 2, 3]).await.unwrap();
    assert_eq!(response.body, [0xbe, 0xef]);
    assert_eq!(session.snapshot().published_queued, 1);
    assert_eq!(session.snapshot().unmatched_skipped, 1);
    assert_eq!(session.take_published().unwrap().body, [0xde, 0xad]);
    session.close().await.unwrap();
    assert!(!session.is_open());
    server.await.unwrap();
}

#[test]
fn mesh_route_and_schedule_reject_invalid_topology() {
    let a = DeviceId([1; 8]);
    let b = DeviceId([2; 8]);
    let c = DeviceId([3; 8]);
    let mut graph = MeshGraph::default();
    for (id, nickname) in [(a, 1), (b, 2), (c, 3)] {
        graph.upsert_node(NodeState {
            id,
            nickname,
            active: true,
        });
    }
    graph.update_link(
        a,
        b,
        LinkHealth {
            rssi_dbm: -70,
            reliability: 0.99,
            age_seconds: 0,
        },
    );
    graph.update_link(
        b,
        c,
        LinkHealth {
            rssi_dbm: -72,
            reliability: 0.98,
            age_seconds: 0,
        },
    );
    assert_eq!(graph.route(a, c, 0.9).unwrap().hops, vec![a, b, c]);
    graph.upsert_node(NodeState {
        id: b,
        nickname: 2,
        active: false,
    });
    assert!(graph.route(a, c, 0.9).is_none());
    graph.upsert_node(NodeState {
        id: b,
        nickname: 2,
        active: true,
    });
    assert!(!graph.update_link(
        a,
        c,
        LinkHealth {
            rssi_dbm: -70,
            reliability: f32::NAN,
            age_seconds: 0,
        },
    ));

    let mut schedule = Schedule::new(10).unwrap();
    schedule
        .insert(Cell {
            timeslot: 1,
            channel_offset: 3,
            source: a,
            destination: b,
            direction: CellDirection::Transmit,
        })
        .unwrap();
    assert!(
        schedule
            .insert(Cell {
                timeslot: 1,
                channel_offset: 4,
                source: b,
                destination: c,
                direction: CellDirection::Transmit,
            })
            .is_err()
    );
    assert!(
        schedule
            .insert(Cell {
                timeslot: 1,
                channel_offset: 3,
                source: c,
                destination: DeviceId([4; 8]),
                direction: CellDirection::Transmit,
            })
            .is_err()
    );
}

#[test]
fn mesh_join_authentication_and_replay_state_are_explicit() {
    let a = DeviceId([1; 8]);
    let mut replay = ReplayWindow::default();
    replay.accept(100).unwrap();
    assert!(replay.accept(100).is_err());
    replay.accept(101).unwrap();

    let mut manager = MeshManager::new(NetworkId(10), 20).unwrap();
    let mut source_key = [9; 16];
    let key = KeyMaterial::new_and_zeroize(&mut source_key);
    assert_eq!(source_key, [0; 16]);
    assert_eq!(format!("{key:?}"), "KeyMaterial([redacted])");
    manager.allow(a, key);
    let rejected = manager
        .join_authenticated(
            hart_link::mesh::JoinRequest {
                device: a,
                network: NetworkId(10),
                security_counter: 1,
            },
            |_, _| false,
        )
        .unwrap();
    assert_eq!(
        rejected,
        hart_link::mesh::JoinDecision::AuthenticationFailed
    );
    let accepted = manager
        .join_authenticated(
            hart_link::mesh::JoinRequest {
                device: a,
                network: NetworkId(10),
                security_counter: 1,
            },
            |_, key| key == &[9; 16],
        )
        .unwrap();
    assert!(matches!(
        accepted,
        hart_link::mesh::JoinDecision::Accepted { .. }
    ));
    manager
        .schedule_mut()
        .insert(Cell {
            timeslot: 1,
            channel_offset: 0,
            source: a,
            destination: DeviceId([2; 8]),
            direction: CellDirection::Transmit,
        })
        .unwrap();
    let rejoined = manager
        .join_authenticated(
            hart_link::mesh::JoinRequest {
                device: a,
                network: NetworkId(10),
                security_counter: 2,
            },
            |_, key| key == &[9; 16],
        )
        .unwrap();
    assert_eq!(accepted, rejoined);

    manager.allow(a, KeyMaterial::new([8; 16]));
    assert_eq!(manager.snapshot().nodes, 0);
    assert_eq!(manager.snapshot().cells, 0);
    let old_key = manager
        .join_authenticated(
            hart_link::mesh::JoinRequest {
                device: a,
                network: NetworkId(10),
                security_counter: 1,
            },
            |_, key| key == &[9; 16],
        )
        .unwrap();
    assert_eq!(old_key, hart_link::mesh::JoinDecision::AuthenticationFailed);
    let fresh_join = manager
        .join_authenticated(
            hart_link::mesh::JoinRequest {
                device: a,
                network: NetworkId(10),
                security_counter: 1,
            },
            |_, key| key == &[8; 16],
        )
        .unwrap();
    assert!(matches!(
        fresh_join,
        hart_link::mesh::JoinDecision::Accepted { .. }
    ));
    manager.revoke(a);
    assert_eq!(manager.snapshot().nodes, 0);
    assert_eq!(manager.snapshot().cells, 0);
}
