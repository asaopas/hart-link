use std::collections::BTreeMap;

use hart_link::{
    Address, CheckedOperation, ChecksumPolicy, CommandCode, DecodeEvent, DecodeLimits, DeviceReply,
    Frame, FrameDecoder, FrameKind, FrameRepair, Master, Operation, PhysicalLayer, RawOperation,
    Request, command_descriptor, inspect_base64, inspect_bytes, inspect_hex,
    operation::{
        ActionCommand, ReadDeviceIdentity, ReadDeviceVariables, ReadDynamicValues, ReadLoopSignal,
        ReadPrimaryValue, ReadTagDescriptorDate, WriteMessage, pack_text, unpack_text,
    },
    tables::{CommonTable, ResolvedCode, TableEntry, TableRepository, TableRepositoryLimits},
};

fn short_address() -> Address {
    Address::polling(7, Master::Primary).unwrap()
}

#[test]
fn short_and_long_frames_round_trip() {
    let addresses = [
        short_address(),
        Address::unique(0x1234, 0x00ab_cdef, Master::Secondary).unwrap(),
    ];
    for address in addresses {
        for expansion in [vec![], vec![1], vec![1, 2], vec![1, 2, 3]] {
            let frame = Frame {
                preambles: 5,
                kind: FrameKind::Request,
                physical_layer: PhysicalLayer::Fsk,
                address,
                expansion,
                wire_command: 9,
                payload: vec![0, 1, 2, 3],
                repair: None,
            };
            let encoded = frame.encode().unwrap();
            let events = FrameDecoder::default().push(&encoded);
            assert_eq!(events, vec![DecodeEvent::Frame(frame)]);
        }
    }
}

#[test]
fn every_two_fragment_split_is_supported() {
    let frame = Frame {
        preambles: 8,
        kind: FrameKind::Response,
        physical_layer: PhysicalLayer::Psk,
        address: Address::unique(0x01ff, 0x0012_3456, Master::Primary).unwrap(),
        expansion: vec![0xaa, 0x55],
        wire_command: 1,
        payload: vec![0, 0, 57, 0x41, 0x48, 0, 0],
        repair: None,
    };
    let encoded = frame.encode().unwrap();
    for split in 0..=encoded.len() {
        let mut decoder = FrameDecoder::default();
        let mut events = decoder.push(&encoded[..split]);
        events.extend(decoder.push(&encoded[split..]));
        assert_eq!(
            events,
            vec![DecodeEvent::Frame(frame.clone())],
            "split at position {split}",
        );
    }
}

#[test]
fn decoder_applies_the_memory_limit_before_copying_a_large_fragment() {
    let mut input = vec![0x55; 1_000_000];
    input.extend_from_slice(&[0xff, 0xff]);
    let mut decoder = FrameDecoder::new(DecodeLimits {
        buffer_capacity: 8,
        minimum_preambles: 2,
        checksum_policy: ChecksumPolicy::Strict,
    });
    assert!(decoder.push(&input).is_empty());
    assert_eq!(decoder.buffered_len(), 2);
    assert_eq!(decoder.statistics().overflow_bytes, 999_994);
}

#[test]
fn inspection_rejects_input_larger_than_any_hart_frame() {
    let issue = inspect_bytes(&vec![0xff; hart_link::MAX_ENCODED_FRAME_SIZE + 1]).unwrap_err();
    assert_eq!(issue.code, "input_limit");
    let whitespace = " ".repeat(hart_link::MAXIMUM_INSPECTION_TEXT_BYTES + 1);
    assert_eq!(inspect_hex(&whitespace).unwrap_err().code, "input_limit");
    assert_eq!(inspect_base64(&whitespace).unwrap_err().code, "input_limit");
}

#[test]
fn decoder_recovers_after_noise_and_bad_checksum() {
    let frame = Frame {
        preambles: 5,
        kind: FrameKind::Request,
        physical_layer: PhysicalLayer::Fsk,
        address: short_address(),
        expansion: vec![],
        wire_command: 0,
        payload: vec![],
        repair: None,
    };
    let valid = frame.encode().unwrap();
    let mut damaged = valid.clone();
    *damaged.last_mut().unwrap() ^= 0x80;
    let mut stream = vec![0x00, 0x55, 0x7e];
    stream.extend_from_slice(&damaged);
    stream.extend_from_slice(&[0x13, 0x37]);
    stream.extend_from_slice(&valid);
    let events = FrameDecoder::default().push(&stream);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, DecodeEvent::Rejected(_)))
    );
    assert!(events.contains(&DecodeEvent::Frame(frame)));
}

#[test]
fn decoder_memory_is_bounded() {
    let limits = DecodeLimits {
        buffer_capacity: 32,
        minimum_preambles: 2,
        checksum_policy: hart_link::ChecksumPolicy::Strict,
    };
    let mut decoder = FrameDecoder::new(limits);
    decoder.push(&vec![0xaa; 10_000]);
    assert!(decoder.buffered_len() <= 32);
    assert!(decoder.statistics().overflow_bytes > 0);
}

#[test]
fn decoder_handles_delimiter_dense_noise_iteratively() {
    let mut decoder = FrameDecoder::new(DecodeLimits {
        buffer_capacity: 16_384,
        minimum_preambles: 2,
        checksum_policy: ChecksumPolicy::Strict,
    });
    let events = decoder.push(&vec![0x02; 16_384]);
    assert!(events.is_empty());
    assert_eq!(decoder.buffered_len(), 0);
    assert_eq!(decoder.statistics().discarded_bytes, 16_384);
}

#[test]
fn known_gateway_repairs_are_opt_in_and_observable() {
    let frame = Frame {
        preambles: 5,
        kind: FrameKind::Response,
        physical_layer: PhysicalLayer::Fsk,
        address: Address::polling(0, Master::Primary).unwrap(),
        expansion: vec![],
        wire_command: 1,
        payload: vec![0, 0, 57, 0, 0, 0, 0],
        repair: None,
    };
    let mut rewritten = frame.encode().unwrap();
    rewritten[6] = (rewritten[6] & 0xc0) | 0x11;
    let mut strict = FrameDecoder::default();
    assert!(
        !strict
            .push(&rewritten)
            .iter()
            .any(|event| matches!(event, DecodeEvent::Frame(_)))
    );

    let mut compatible = FrameDecoder::new(DecodeLimits {
        checksum_policy: ChecksumPolicy::KnownGateway {
            delimiter_bit: false,
            checksum_source_node: Some(0),
        },
        ..DecodeLimits::default()
    });
    let events = compatible.push(&rewritten);
    let DecodeEvent::Frame(repaired) = &events[0] else {
        panic!("expected a repaired frame");
    };
    assert_eq!(
        repaired.address,
        Address::polling(17, Master::Primary).unwrap()
    );
    assert_eq!(
        repaired.repair,
        Some(FrameRepair::ShortAddressRewrite {
            checksum_source_node: 0,
        })
    );

    let mut delimiter_changed = frame.encode().unwrap();
    delimiter_changed[5] ^= 0x10;
    let mut compatible = FrameDecoder::new(DecodeLimits {
        checksum_policy: ChecksumPolicy::KnownGateway {
            delimiter_bit: true,
            checksum_source_node: None,
        },
        ..DecodeLimits::default()
    });
    let events = compatible.push(&delimiter_changed);
    let DecodeEvent::Frame(repaired) = &events[0] else {
        panic!("expected a repaired delimiter");
    };
    assert_eq!(repaired.repair, Some(FrameRepair::DelimiterBit));

    // Some transparent gateways transmit a valid delimiter but retain the
    // checksum calculated before clearing its reserved 0x10 bit.
    let mut checksum_from_gateway_delimiter = frame.encode().unwrap();
    let checksum_index = checksum_from_gateway_delimiter.len() - 1;
    checksum_from_gateway_delimiter[checksum_index] ^= 0x10;
    let mut strict = FrameDecoder::default();
    assert!(
        !strict
            .push(&checksum_from_gateway_delimiter)
            .iter()
            .any(|event| matches!(event, DecodeEvent::Frame(_)))
    );
    let mut compatible = FrameDecoder::new(DecodeLimits {
        checksum_policy: ChecksumPolicy::KnownGateway {
            delimiter_bit: true,
            checksum_source_node: None,
        },
        ..DecodeLimits::default()
    });
    let events = compatible.push(&checksum_from_gateway_delimiter);
    let DecodeEvent::Frame(repaired) = &events[0] else {
        panic!("expected a frame with a gateway checksum repair");
    };
    assert_eq!(repaired.repair, Some(FrameRepair::DelimiterBit));
}

#[test]
fn expanded_command_is_normalized_in_both_directions() {
    let request = Request::new(short_address(), 554u16, vec![1, 2, 3]);
    let request_frame = request.to_frame().unwrap();
    assert_eq!(request_frame.wire_command, 31);
    assert_eq!(&request_frame.payload[..2], &554u16.to_be_bytes());

    let reply = DeviceReply::from_frame(Frame {
        preambles: 3,
        kind: FrameKind::Response,
        physical_layer: PhysicalLayer::Fsk,
        address: short_address(),
        expansion: vec![],
        wire_command: 31,
        payload: vec![0, 0, 0x02, 0x2a, 9, 8, 7],
        repair: None,
    })
    .unwrap();
    assert_eq!(reply.command, CommandCode::new(554));
    assert_eq!(reply.data, vec![9, 8, 7]);
}

#[test]
fn typed_operations_decode_values() {
    let primary_reply = DeviceReply {
        address: short_address(),
        command: CommandCode::new(1),
        response_code: 0,
        device_status: 0,
        data: [vec![57], 12.5f32.to_be_bytes().to_vec()].concat(),
        burst: false,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    };
    let primary = ReadPrimaryValue.decode_reply(&primary_reply).unwrap();
    assert_eq!(primary.unit, 57);
    assert!((primary.value - 12.5).abs() < f32::EPSILON);

    let signal_reply = DeviceReply {
        command: CommandCode::new(2),
        data: [4.0f32.to_be_bytes(), 25.0f32.to_be_bytes()].concat(),
        ..primary_reply
    };
    let signal = ReadLoopSignal.decode_reply(&signal_reply).unwrap();
    assert!((signal.milliamps - 4.0).abs() < f32::EPSILON);
    assert!((signal.percent - 25.0).abs() < f32::EPSILON);
}

#[test]
fn identity_keeps_extension_bytes() {
    let reply = DeviceReply {
        address: short_address(),
        command: CommandCode::new(0),
        response_code: 0,
        device_status: 0,
        data: vec![
            0xfe, 0x04, 0x2a, 7, 7, 3, 4, 0x29, 6, 0x12, 0x34, 0x56, 5, 8, 0, 12, 0, 0x12, 0x34, 0,
            0, 1, 0xaa, 0xbb,
        ],
        burst: false,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    };
    let identity = ReadDeviceIdentity.decode_reply(&reply).unwrap();
    assert_eq!(identity.device_id, 0x12_34_56);
    assert_eq!(identity.manufacturer_id, 0x1234);
    assert_eq!(identity.device_type, 0x042a);
    assert_eq!(identity.extension, vec![0xaa, 0xbb]);
}

#[test]
fn checked_operation_rejects_wrong_length_but_raw_accepts_it() {
    assert!(CheckedOperation::new(17u8, vec![0; 23]).is_err());
    assert!(CheckedOperation::new(17u8, vec![0; 24]).is_ok());
    let raw = RawOperation::new(17u8, vec![0; 23]);
    assert_eq!(raw.command(), CommandCode::new(17));
}

#[test]
fn convenience_builders_keep_retry_intent_explicit() {
    let read = RawOperation::read(500u16, [1, 2]);
    assert_eq!(read.retry_safety(), hart_link::OperationSafety::ReadOnly);
    let action = RawOperation::action(500u16, [1, 2]);
    assert_eq!(action.retry_safety(), hart_link::OperationSafety::Action);

    let polling = Address::polling(12, Master::Secondary).unwrap();
    assert_eq!(polling.polling_node(), Some(12));
    assert_eq!(polling.unique_parts(), None);
    let unique = Address::unique(0x1234, 0x00ab_cdef, Master::Primary).unwrap();
    assert_eq!(unique.unique_parts(), Some((0x1234, 0x00ab_cdef)));
}

#[test]
fn process_calibration_commands_are_never_automatically_retried() {
    for command in [36, 37] {
        let operation = ActionCommand::new(command, Vec::new()).unwrap();
        assert_eq!(operation.retry_safety(), hart_link::OperationSafety::Action);
        assert_eq!(
            command_descriptor(CommandCode::from(command))
                .unwrap()
                .safety,
            hart_link::OperationSafety::Action
        );
    }
}

#[test]
fn unknown_operation_requires_an_explicit_retry_safety_declaration() {
    use hart_link::OperationSafety;

    let operation = RawOperation::new(500u16, vec![]);
    assert_eq!(
        operation.request(short_address()).unwrap().retry_safety,
        Some(OperationSafety::Action)
    );
    let operation = operation.with_retry_safety(OperationSafety::ReadOnly);
    assert_eq!(
        operation.request(short_address()).unwrap().retry_safety,
        Some(OperationSafety::ReadOnly)
    );
}

#[test]
fn hart5_polling_address_write_uses_the_legacy_one_byte_form() {
    use hart_link::operation::WriteLegacyPollingAddress;

    let request = WriteLegacyPollingAddress { node: 15 }
        .request(short_address())
        .unwrap();
    assert_eq!(request.data, [15]);
    assert!(
        WriteLegacyPollingAddress { node: 16 }
            .request(short_address())
            .is_err()
    );
}

#[test]
fn inspector_reports_structure_and_known_class() {
    let encoded = Request::new(short_address(), 1u8, vec![])
        .to_frame()
        .unwrap()
        .encode()
        .unwrap();
    let report = inspect_bytes(&encoded).unwrap();
    assert_eq!(report.command, CommandCode::new(1));
    assert!(report.class.is_some());
    assert!(report.issues.is_empty());
    assert!(command_descriptor(report.command).is_some());
}

#[test]
fn inspector_rejects_bytes_outside_the_single_frame() {
    let encoded = Request::new(short_address(), 1u8, vec![])
        .to_frame()
        .unwrap()
        .encode()
        .unwrap();
    let mut leading_noise = vec![0x00, 0x55];
    leading_noise.extend_from_slice(&encoded);
    assert_eq!(
        inspect_bytes(&leading_noise).unwrap_err().code,
        "unexpected_bytes"
    );

    let mut trailing_noise = encoded;
    trailing_noise.extend_from_slice(&[0x55, 0x00]);
    assert_eq!(
        inspect_bytes(&trailing_noise).unwrap_err().code,
        "unexpected_bytes"
    );
}

#[test]
fn inspector_preserves_the_checksum_error_as_the_primary_diagnostic() {
    let mut encoded = Request::new(short_address(), 1u8, vec![])
        .to_frame()
        .unwrap()
        .encode()
        .unwrap();
    *encoded.last_mut().unwrap() ^= 1;
    let error = inspect_bytes(&encoded).unwrap_err();
    assert_eq!(error.code, "wire");
    assert!(error.message.contains("checksum mismatch"));
}

#[test]
fn expanded_request_limit_accounts_for_the_service_number() {
    assert!(CheckedOperation::new(256u16, vec![0; 253]).is_ok());
    assert!(CheckedOperation::new(256u16, vec![0; 254]).is_err());
    assert!(Request::try_new(short_address(), 256u16, vec![0; 253]).is_ok());
    assert!(Request::try_new(short_address(), 256u16, vec![0; 254]).is_err());
    assert!(
        Request::new(short_address(), 256u16, vec![0; 254])
            .to_frame()
            .is_err()
    );
    assert!(
        RawOperation::read(256u16, vec![0; 254])
            .request(short_address())
            .is_err()
    );
}

#[test]
fn common_table_preserves_unknown_code() {
    let mut entries = BTreeMap::new();
    entries.insert(
        57,
        TableEntry {
            code: 57,
            label: "degree Celsius".into(),
        },
    );
    let table = CommonTable {
        number: 2,
        revision: "local test".into(),
        entries,
    };
    let mut repository = TableRepository::default();
    repository.replace(table).unwrap();
    assert!(matches!(
        repository.get(2).unwrap().resolve(57),
        ResolvedCode::Known(_)
    ));
    assert_eq!(
        repository.get(2).unwrap().resolve(999),
        ResolvedCode::Unknown(999)
    );
}

#[test]
fn common_table_repository_enforces_aggregate_limits() {
    let mut repository = TableRepository::with_limits(TableRepositoryLimits {
        tables: 1,
        entries: 1,
    })
    .unwrap();
    repository
        .replace(CommonTable {
            number: 1,
            revision: "test".into(),
            entries: [(
                1,
                TableEntry {
                    code: 1,
                    label: "one".into(),
                },
            )]
            .into(),
        })
        .unwrap();
    assert!(
        repository
            .replace(CommonTable {
                number: 2,
                revision: "test".into(),
                entries: BTreeMap::new(),
            })
            .is_err()
    );
    assert_eq!(repository.len(), 1);
    assert_eq!(repository.total_entries(), 1);
}

#[test]
fn typed_decoder_accepts_only_explicit_warnings_and_keeps_status() {
    use hart_link::operation::{Operation, ReadPrimaryValue};

    let reply = DeviceReply {
        address: short_address(),
        command: CommandCode::new(1),
        response_code: 8,
        device_status: 0x20,
        data: {
            let mut bytes = vec![45];
            bytes.extend_from_slice(&12.5f32.to_be_bytes());
            bytes
        },
        burst: false,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 5,
        frame_expansion: vec![],
    };
    assert!(ReadPrimaryValue.decode_reply(&reply).is_err());
    let outcome = ReadPrimaryValue
        .decode_reply_accepting(&reply, &[8])
        .unwrap();
    assert!(outcome.is_warning());
    assert_eq!(outcome.response_code, 8);
    assert_eq!(outcome.device_status, 0x20);
    assert_eq!(outcome.value.unit, 45);
    assert!((outcome.value.value - 12.5).abs() < f32::EPSILON);

    let mut communication_error = reply;
    communication_error.response_code = 0x82;
    assert!(
        ReadPrimaryValue
            .decode_reply_accepting(&communication_error, &[0x82])
            .is_err()
    );
}

#[test]
fn every_logical_command_number_builds_a_frame() {
    for code in 0u16..=u16::MAX {
        let frame = Request::new(short_address(), code, vec![])
            .to_frame()
            .unwrap();
        if code > u16::from(u8::MAX) {
            assert_eq!(frame.wire_command, 31);
            assert_eq!(&frame.payload[..2], &code.to_be_bytes());
        } else {
            assert_eq!(u16::from(frame.wire_command), code);
        }
        assert!(frame.encode().is_ok());
    }
}

#[test]
fn packed_text_round_trip_and_rejects_lowercase() {
    let packed = pack_text("HART LINK", 24).unwrap();
    assert_eq!(packed.len(), 24);
    assert_eq!(unpack_text(&packed), "HART LINK");
    assert_eq!(
        pack_text("STID8FT", 6).unwrap(),
        [0x4d, 0x42, 0x44, 0xe0, 0x65, 0x20]
    );
    assert_eq!(
        unpack_text(&[0x4d, 0x42, 0x44, 0xe0, 0x65, 0x20]),
        "STID8FT"
    );
    assert_eq!(unpack_text(&[0x2c, 0x94, 0x2d, 0x26, 0xd8, 0x20]), "KIP-I-");
    assert!(WriteMessage::new("lowercase").is_err());
    assert!(pack_text("", 258).is_err());
}

#[test]
fn direct_decoder_construction_is_bounded_and_has_a_strict_variant() {
    let limits = DecodeLimits::default().with_buffer_capacity(usize::MAX);
    assert!(FrameDecoder::try_new(limits).is_err());
    let decoder = FrameDecoder::new(limits);
    assert_eq!(
        decoder.limits().buffer_capacity,
        hart_link::MAXIMUM_DECODE_BUFFER_CAPACITY
    );
}

#[test]
fn decoder_bounds_the_number_of_events_from_one_fragment() {
    let encoded = Frame {
        preambles: 2,
        kind: FrameKind::Request,
        physical_layer: PhysicalLayer::Fsk,
        address: short_address(),
        expansion: vec![],
        wire_command: 0,
        payload: vec![],
        repair: None,
    }
    .encode()
    .unwrap();
    let input = encoded.repeat(hart_link::MAXIMUM_DECODE_EVENTS_PER_PUSH + 1);
    let mut decoder = FrameDecoder::new(DecodeLimits::default().with_buffer_capacity(input.len()));
    let events = decoder.push(&input);
    assert_eq!(events.len(), hart_link::MAXIMUM_DECODE_EVENTS_PER_PUSH);
    assert_eq!(decoder.statistics().event_limit_hits, 1);
    assert_eq!(decoder.buffered_len(), 0);
}

#[test]
fn command_13_preserves_text_when_the_device_date_is_unspecified() {
    let reply = DeviceReply {
        address: short_address(),
        command: CommandCode::new(13),
        response_code: 0,
        device_status: 0x10,
        data: vec![
            0x19, 0x4b, 0x78, 0xda, 0x08, 0x20, 0x43, 0x28, 0x20, 0x82, 0x08, 0x20, 0x82, 0x08,
            0x20, 0x82, 0x08, 0x20, 0, 0, 0,
        ],
        burst: false,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 5,
        frame_expansion: vec![],
    };
    let value = ReadTagDescriptorDate.decode_reply(&reply).unwrap();
    assert_eq!(value.tag, "FT-86");
    assert_eq!(value.descriptor, "P2");
    assert_eq!(value.date.day, 0);
    assert_eq!(value.date.month, 0);
    assert_eq!(value.date.year_since_1900, 0);

    let mut partially_invalid = reply;
    partially_invalid.data[18] = 1;
    assert!(
        ReadTagDescriptorDate
            .decode_reply(&partially_invalid)
            .is_err()
    );
}

#[test]
fn dynamic_and_device_variable_formats_are_strict() {
    let mut dynamic_data = 12.0f32.to_be_bytes().to_vec();
    dynamic_data.push(57);
    dynamic_data.extend_from_slice(&42.0f32.to_be_bytes());
    let dynamic_reply = DeviceReply {
        address: short_address(),
        command: CommandCode::new(3),
        response_code: 0,
        device_status: 0,
        data: dynamic_data,
        burst: false,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    };
    let dynamic = ReadDynamicValues.decode_reply(&dynamic_reply).unwrap();
    assert_eq!(dynamic.values.len(), 1);
    assert!((dynamic.values[0].value - 42.0).abs() < f32::EPSILON);

    let mut variable_data = vec![0, 2, 10, 57];
    variable_data.extend_from_slice(&17.0f32.to_be_bytes());
    variable_data.push(1);
    let variable_reply = DeviceReply {
        command: CommandCode::new(9),
        data: variable_data,
        ..dynamic_reply
    };
    let operation = ReadDeviceVariables::new(vec![2]).unwrap();
    let decoded = operation.decode_reply(&variable_reply).unwrap();
    assert_eq!(decoded.variables.len(), 1);
    assert_eq!(decoded.variables[0].code, 2);
}

#[test]
fn hart5_identity_uses_legacy_manufacturer_and_type() {
    let reply = DeviceReply {
        address: short_address(),
        command: CommandCode::new(0),
        response_code: 0,
        device_status: 0,
        data: vec![0xfe, 0xd8, 0x82, 5, 5, 2, 3, 0x29, 0, 1, 2, 3],
        burst: false,
        physical_layer: PhysicalLayer::Fsk,
        response_preambles: 3,
        frame_expansion: vec![],
    };
    let identity = ReadDeviceIdentity.decode_reply(&reply).unwrap();
    assert_eq!(identity.manufacturer_id, 0xd8);
    assert_eq!(identity.device_type, 0x82);
    assert_eq!(identity.device_id, 0x01_02_03);
    assert_eq!(identity.response_preambles, None);
    assert_eq!(identity.address_device_type(), 0x1882);
    assert_eq!(
        identity.unique_address(Master::Primary).unwrap(),
        Address::unique(0x1882, 0x01_02_03, Master::Primary).unwrap()
    );
}
