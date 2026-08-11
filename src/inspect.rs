//! Fast structural frame inspection for development and link diagnostics.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};

use crate::{
    catalog::{CommandClass, command_descriptor},
    operation::{CommandCode, DeviceReply},
    wire::{
        Address, ChecksumPolicy, DecodeEvent, DecodeLimits, FrameDecoder, FrameKind, FrameRepair,
        MAX_ENCODED_FRAME_SIZE, PhysicalLayer,
    },
};

/// Largest textual frame representation accepted before decoding whitespace or encoding.
pub const MAXIMUM_INSPECTION_TEXT_BYTES: usize = MAX_ENCODED_FRAME_SIZE * 8;

/// Inspector diagnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectionIssue {
    /// Machine-readable diagnostic code.
    pub code: &'static str,
    /// Human-readable explanation.
    pub message: String,
}

/// Summary of an inspected frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameReport {
    /// Frame direction.
    pub kind: FrameKind,
    /// Frame address without service bits.
    pub address: Address,
    /// Physical mode carried by the delimiter.
    pub physical_layer: PhysicalLayer,
    /// Logical command number.
    pub command: CommandCode,
    /// Known command class, or `None`.
    pub class: Option<CommandClass>,
    /// Preamble count.
    pub preambles: u8,
    /// One-byte command value actually carried on the wire.
    pub wire_command: u8,
    /// Number of link-header extension bytes.
    pub frame_expansion_length: usize,
    /// Applied repair for a known gateway defect.
    pub repair: Option<FrameRepair>,
    /// Application payload length.
    pub data_length: usize,
    /// Response code for a response frame.
    pub response_code: Option<u8>,
    /// Device status for a response frame.
    pub device_status: Option<u8>,
    /// Non-structural diagnostics.
    pub issues: Vec<InspectionIssue>,
}

/// Consistency of a complete request and response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeReport {
    /// Request summary.
    pub request: FrameReport,
    /// Response summary.
    pub response: FrameReport,
    /// Whether the canonical addresses match.
    pub address_matches: bool,
    /// Whether the logical command numbers match, including Command 31.
    pub command_matches: bool,
    /// Cross-frame consistency errors.
    pub issues: Vec<InspectionIssue>,
}

/// Inspects exactly one frame in a complete byte sequence.
pub fn inspect_bytes(bytes: &[u8]) -> Result<FrameReport, InspectionIssue> {
    if bytes.len() > MAX_ENCODED_FRAME_SIZE {
        return Err(input_limit_issue());
    }
    let mut decoder = FrameDecoder::new(DecodeLimits {
        buffer_capacity: bytes.len().max(64),
        minimum_preambles: 1,
        checksum_policy: ChecksumPolicy::Strict,
    });
    let mut events = decoder.push(bytes);
    if events.len() != 1 || decoder.buffered_len() != 0 {
        return Err(InspectionIssue {
            code: "frame_count",
            message: "expected exactly one complete frame".into(),
        });
    }
    match events.remove(0) {
        DecodeEvent::Rejected(error) => Err(InspectionIssue {
            code: "wire",
            message: error.to_string(),
        }),
        DecodeEvent::Frame(frame) => {
            if decoder.statistics().discarded_bytes != 0 {
                return Err(InspectionIssue {
                    code: "unexpected_bytes",
                    message: "unexpected bytes were found before or after the frame".into(),
                });
            }
            let (command, response_code, device_status, data_length) = if frame.kind
                == FrameKind::Request
            {
                let expanded = frame.wire_command == 31 && frame.payload.len() >= 2;
                let command = if expanded {
                    CommandCode::new(u16::from_be_bytes([frame.payload[0], frame.payload[1]]))
                } else {
                    CommandCode::from(frame.wire_command)
                };
                (
                    command,
                    None,
                    None,
                    frame.payload.len() - usize::from(expanded) * 2,
                )
            } else {
                let reply =
                    DeviceReply::from_frame(frame.clone()).map_err(|error| InspectionIssue {
                        code: "reply",
                        message: error.to_string(),
                    })?;
                (
                    reply.command,
                    Some(reply.response_code),
                    Some(reply.device_status),
                    reply.data.len(),
                )
            };
            let descriptor = command_descriptor(command);
            let mut issues = Vec::new();
            if descriptor.is_none() {
                issues.push(InspectionIssue {
                    code: "unknown_semantics",
                    message: format!(
                        "the frame is structurally valid, but command {} is not in the registry",
                        command.get()
                    ),
                });
            }
            Ok(FrameReport {
                kind: frame.kind,
                address: frame.address,
                physical_layer: frame.physical_layer,
                command,
                class: descriptor.map(|value| value.class),
                preambles: frame.preambles,
                wire_command: frame.wire_command,
                frame_expansion_length: frame.expansion.len(),
                repair: frame.repair,
                data_length,
                response_code,
                device_status,
                issues,
            })
        }
    }
}

/// Validates the structure and consistency of a complete exchange.
pub fn inspect_exchange(
    request_bytes: &[u8],
    response_bytes: &[u8],
) -> Result<ExchangeReport, InspectionIssue> {
    let request = inspect_bytes(request_bytes)?;
    let response = inspect_bytes(response_bytes)?;
    let address_matches = request.address == response.address;
    let command_matches = request.command == response.command;
    let mut issues = Vec::new();
    if request.kind != FrameKind::Request {
        issues.push(InspectionIssue {
            code: "request_direction",
            message: "the first frame is not a request".into(),
        });
    }
    if response.kind != FrameKind::Response {
        issues.push(InspectionIssue {
            code: "response_direction",
            message: "the second frame is not a solicited response".into(),
        });
    }
    if !address_matches {
        issues.push(InspectionIssue {
            code: "address_mismatch",
            message: "the response address does not match the request address".into(),
        });
    }
    if !command_matches {
        issues.push(InspectionIssue {
            code: "command_mismatch",
            message: "the response logical command does not match the request".into(),
        });
    }
    Ok(ExchangeReport {
        request,
        response,
        address_matches,
        command_matches,
        issues,
    })
}

/// Decodes a standard Base64 string and inspects the contained frame.
pub fn inspect_base64(value: &str) -> Result<FrameReport, InspectionIssue> {
    if value.len() > MAXIMUM_INSPECTION_TEXT_BYTES {
        return Err(input_limit_issue());
    }
    let value = value.trim();
    let maximum_encoded = MAX_ENCODED_FRAME_SIZE.div_ceil(3) * 4;
    if value.len() > maximum_encoded {
        return Err(input_limit_issue());
    }
    let bytes = STANDARD.decode(value).map_err(|_| InspectionIssue {
        code: "base64",
        message: "the string is not valid Base64".into(),
    })?;
    inspect_bytes(&bytes)
}

/// Decodes a hexadecimal string and inspects the contained frame.
pub fn inspect_hex(value: &str) -> Result<FrameReport, InspectionIssue> {
    if value.len() > MAXIMUM_INSPECTION_TEXT_BYTES {
        return Err(input_limit_issue());
    }
    let maximum_digits = MAX_ENCODED_FRAME_SIZE * 2;
    let mut compact = Vec::with_capacity(value.len().min(maximum_digits));
    for byte in value.bytes().filter(|byte| !byte.is_ascii_whitespace()) {
        if compact.len() == maximum_digits {
            return Err(input_limit_issue());
        }
        compact.push(byte);
    }
    if !compact.len().is_multiple_of(2) {
        return Err(InspectionIssue {
            code: "hex_length",
            message: "the hexadecimal string must contain an even number of digits".into(),
        });
    }
    let mut bytes = Vec::with_capacity(compact.len() / 2);
    for (pair_index, pair) in compact.chunks_exact(2).enumerate() {
        let offset = pair_index * 2;
        let high = hex_nibble(pair[0]).ok_or_else(|| InspectionIssue {
            code: "hex_digit",
            message: format!("invalid hexadecimal digit at offset {offset}"),
        })?;
        let low = hex_nibble(pair[1]).ok_or_else(|| InspectionIssue {
            code: "hex_digit",
            message: format!("invalid hexadecimal digit at offset {}", offset + 1),
        })?;
        bytes.push((high << 4) | low);
    }
    inspect_bytes(&bytes)
}

fn input_limit_issue() -> InspectionIssue {
    InspectionIssue {
        code: "input_limit",
        message: format!("input exceeds the {MAX_ENCODED_FRAME_SIZE}-byte frame limit"),
    }
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
