use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::operation::{OperationError, PayloadReader};

/// Largest number of named fields in one application payload schema.
pub const MAXIMUM_SCHEMA_FIELDS: usize = 255;
/// Largest UTF-8 byte length accepted for a field or enumeration label.
pub const MAXIMUM_SCHEMA_NAME_BYTES: usize = 256;

/// Enumeration variant that preserves its numeric code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumChoice {
    /// Numeric value carried on the wire.
    pub value: u32,
    /// Display name from the source description.
    pub name: String,
}

impl EnumChoice {
    /// Creates one preserved numeric enumeration choice.
    pub fn new(value: u32, name: impl Into<String>) -> Self {
        Self {
            value,
            name: name.into(),
        }
    }
}

/// Type of one sequential field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FieldKind {
    /// Unsigned byte.
    U8,
    /// Signed byte.
    I8,
    /// Unsigned 16-bit integer.
    U16,
    /// Signed 16-bit integer.
    I16,
    /// Unsigned 24-bit integer.
    U24,
    /// Unsigned 32-bit integer.
    U32,
    /// Signed 32-bit integer.
    I32,
    /// Single-precision IEEE-754 number.
    F32,
    /// Boolean value where zero means false.
    Boolean,
    /// Raw bytes of fixed length.
    Bytes {
        /// Field length.
        length: usize,
    },
    /// Fixed-length Latin-1 string.
    Latin1 {
        /// Field length.
        length: usize,
    },
    /// Packed six-bit HART text.
    PackedAscii {
        /// Number of input bytes.
        length: usize,
    },
    /// Enumeration encoded as an unsigned byte.
    Enum8 {
        /// Known values.
        choices: Vec<EnumChoice>,
    },
}

impl FieldKind {
    fn encoded_len(&self) -> usize {
        match self {
            Self::U8 | Self::I8 | Self::Boolean | Self::Enum8 { .. } => 1,
            Self::U16 | Self::I16 => 2,
            Self::U24 => 3,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::Bytes { length } | Self::Latin1 { length } | Self::PackedAscii { length } => {
                *length
            }
        }
    }
}

/// Description of one response field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldSpec {
    /// Name unique within the schema.
    pub name: String,
    /// Representation type.
    pub field_type: FieldKind,
}

impl FieldSpec {
    /// Creates one named sequential field.
    pub fn new(name: impl Into<String>, field_type: FieldKind) -> Self {
        Self {
            name: name.into(),
            field_type,
        }
    }
}

/// Sequential payload schema.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSchema {
    /// Fields in wire order.
    pub fields: Vec<FieldSpec>,
    /// Preserves an unknown tail instead of returning an error.
    #[serde(default)]
    pub preserve_tail: bool,
}

impl DataSchema {
    /// Creates an empty strict sequential schema.
    pub const fn new() -> Self {
        Self {
            fields: Vec::new(),
            preserve_tail: false,
        }
    }

    /// Appends one field in wire order.
    pub fn with_field(mut self, field: FieldSpec) -> Self {
        self.fields.push(field);
        self
    }

    /// Chooses whether unknown response bytes are preserved.
    pub const fn preserving_tail(mut self, preserve_tail: bool) -> Self {
        self.preserve_tail = preserve_tail;
        self
    }

    /// Validates unique names and the HART payload-size limit.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.fields.len() > MAXIMUM_SCHEMA_FIELDS {
            return Err(SchemaError::TooManyFields(self.fields.len()));
        }
        let mut names = BTreeSet::new();
        let mut total = 0usize;
        for field in &self.fields {
            if field.name.trim().is_empty() {
                return Err(SchemaError::EmptyName);
            }
            if field.name.len() > MAXIMUM_SCHEMA_NAME_BYTES {
                return Err(SchemaError::NameTooLong(field.name.len()));
            }
            if !names.insert(field.name.as_str()) {
                return Err(SchemaError::DuplicateName(field.name.clone()));
            }
            total = total
                .checked_add(field.field_type.encoded_len())
                .ok_or(SchemaError::PayloadTooLarge(usize::MAX))?;
            if let FieldKind::Enum8 { choices } = &field.field_type {
                if choices.len() > usize::from(u8::MAX) + 1 {
                    return Err(SchemaError::TooManyEnumChoices(choices.len()));
                }
                let mut values = BTreeSet::new();
                for choice in choices {
                    if choice.name.trim().is_empty() {
                        return Err(SchemaError::EmptyEnumLabel(choice.value));
                    }
                    if choice.name.len() > MAXIMUM_SCHEMA_NAME_BYTES {
                        return Err(SchemaError::EnumLabelTooLong {
                            value: choice.value,
                            bytes: choice.name.len(),
                        });
                    }
                    if choice.value > u32::from(u8::MAX) {
                        return Err(SchemaError::EnumValue(choice.value));
                    }
                    if !values.insert(choice.value) {
                        return Err(SchemaError::DuplicateEnum(choice.value));
                    }
                }
            }
            if matches!(
                field.field_type,
                FieldKind::Bytes { length: 0 }
                    | FieldKind::Latin1 { length: 0 }
                    | FieldKind::PackedAscii { length: 0 }
            ) {
                return Err(SchemaError::ZeroLengthField(field.name.clone()));
            }
        }
        if total > usize::from(u8::MAX) {
            return Err(SchemaError::PayloadTooLarge(total));
        }
        Ok(())
    }

    /// Decodes a record while preserving unknown values without losing their codes.
    pub fn decode(&self, bytes: &[u8]) -> Result<DynamicRecord, SchemaError> {
        self.validate()?;
        let mut reader = PayloadReader::new(bytes);
        let mut values = BTreeMap::new();
        for field in &self.fields {
            let value = decode_field(&mut reader, field)?;
            values.insert(field.name.clone(), value);
        }
        let tail = if self.preserve_tail {
            reader.rest().to_vec()
        } else {
            reader.finish().map_err(SchemaError::Codec)?;
            Vec::new()
        };
        Ok(DynamicRecord { values, tail })
    }
}

/// Dynamically typed field value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum DynamicValue {
    /// Unsigned integer.
    Unsigned(u32),
    /// Signed integer.
    Signed(i32),
    /// Floating-point number.
    Float(f32),
    /// Boolean value.
    Boolean(bool),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Text.
    Text(String),
    /// Known or unknown enumeration value.
    Enumeration {
        /// Original numeric code.
        code: u32,
        /// Name when the code is described by the schema.
        name: Option<String>,
    },
}

/// Result of dynamic decoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamicRecord {
    /// Values indexed by field name.
    pub values: BTreeMap<String, DynamicValue>,
    /// Unrecognized tail.
    pub tail: Vec<u8>,
}

/// Error defining or applying a schema.
#[derive(Debug, Error)]
pub enum SchemaError {
    /// A field name is empty.
    #[error("field name cannot be empty")]
    EmptyName,
    /// A schema has more named fields than can fit in a HART payload model.
    #[error("schema contains too many fields: {0}")]
    TooManyFields(usize),
    /// A field name is implausibly large.
    #[error("field name contains {0} UTF-8 bytes")]
    NameTooLong(usize),
    /// A device profile has no display name.
    #[error("device profile display name cannot be empty")]
    EmptyProfileName,
    /// A device profile has no source revision.
    #[error("device profile source revision cannot be empty")]
    MissingSourceRevision,
    /// A profile identity or response-code description is implausibly large.
    #[error("{0} contains {1} UTF-8 bytes")]
    ProfileTextTooLong(&'static str, usize),
    /// An exact device revision contains an implausible number of commands.
    #[error("device profile contains too many command entries: {0}")]
    TooManyProfileCommands(usize),
    /// A catalog reached its configured exact-revision limit.
    #[error("device catalog reached its {0}-profile limit")]
    CatalogProfileLimit(usize),
    /// A response-code value has the communication-error bit set.
    #[error("response code {0} is outside the seven-bit application range")]
    ResponseCode(u8),
    /// A response-code description is empty.
    #[error("response code {0} has an empty description")]
    EmptyResponseCodeDescription(u8),
    /// An expanded device type does not fit a HART unique address.
    #[error("expanded device type {0} does not fit in 14 bits")]
    ExpandedDeviceType(u16),
    /// A field name is duplicated.
    #[error("field {0} is declared more than once")]
    DuplicateName(String),
    /// The total length does not fit in a payload.
    #[error("schema requires {0} bytes")]
    PayloadTooLarge(usize),
    /// An enumeration code does not fit the selected type.
    #[error("enumeration code {0} does not fit in one byte")]
    EnumValue(u32),
    /// An enumeration code is duplicated.
    #[error("enumeration code {0} is declared more than once")]
    DuplicateEnum(u32),
    /// An enumeration choice has no display label.
    #[error("enumeration code {0} has an empty label")]
    EmptyEnumLabel(u32),
    /// A one-byte enumeration declares more than 256 alternatives.
    #[error("one-byte enumeration contains too many choices: {0}")]
    TooManyEnumChoices(usize),
    /// An enumeration label is implausibly large.
    #[error("enumeration code {value} label contains {bytes} UTF-8 bytes")]
    EnumLabelTooLong {
        /// Numeric enumeration value.
        value: u32,
        /// Label length.
        bytes: usize,
    },
    /// A named byte or text field must consume at least one byte.
    #[error("field {0} has zero encoded length")]
    ZeroLengthField(String),
    /// The payload does not match the schema.
    #[error(transparent)]
    Codec(#[from] OperationError),
}

fn decode_field(
    reader: &mut PayloadReader<'_>,
    field: &FieldSpec,
) -> Result<DynamicValue, SchemaError> {
    let name: &'static str = "dynamic field";
    let value = match &field.field_type {
        FieldKind::U8 => DynamicValue::Unsigned(u32::from(reader.u8(name)?)),
        FieldKind::I8 => DynamicValue::Signed(i32::from(i8::from_be_bytes([reader.u8(name)?]))),
        FieldKind::U16 => DynamicValue::Unsigned(u32::from(reader.u16(name)?)),
        FieldKind::I16 => DynamicValue::Signed(i32::from(i16::from_be_bytes(
            reader.u16(name)?.to_be_bytes(),
        ))),
        FieldKind::U24 => DynamicValue::Unsigned(reader.u24(name)?),
        FieldKind::U32 => DynamicValue::Unsigned(reader.u32(name)?),
        FieldKind::I32 => DynamicValue::Signed(i32::from_be_bytes(reader.u32(name)?.to_be_bytes())),
        FieldKind::F32 => DynamicValue::Float(reader.f32(name)?),
        FieldKind::Boolean => DynamicValue::Boolean(reader.u8(name)? != 0),
        FieldKind::Bytes { length } => DynamicValue::Bytes(reader.bytes(*length, name)?.to_vec()),
        FieldKind::Latin1 { length } => {
            let text: String = reader
                .bytes(*length, name)?
                .iter()
                .map(|byte| char::from(*byte))
                .collect();
            DynamicValue::Text(text.trim_end_matches(['\0', ' ']).to_owned())
        }
        FieldKind::PackedAscii { length } => {
            DynamicValue::Text(decode_packed_ascii(reader.bytes(*length, name)?))
        }
        FieldKind::Enum8 { choices } => {
            let code = u32::from(reader.u8(name)?);
            let known = choices
                .iter()
                .find(|choice| choice.value == code)
                .map(|choice| choice.name.clone());
            DynamicValue::Enumeration { code, name: known }
        }
    };
    Ok(value)
}

fn decode_packed_ascii(bytes: &[u8]) -> String {
    let mut output = String::new();
    let mut accumulator = 0u32;
    let mut bits = 0u8;
    for byte in bytes {
        accumulator = (accumulator << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 6 {
            bits -= 6;
            let code = ((accumulator >> bits) & 0x3f) as u8;
            output.push(char::from(code + 0x20));
        }
    }
    output.trim_end().to_owned()
}
