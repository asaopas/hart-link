use alloc::{string::String, vec::Vec};

use crate::operation::{CommandCode, DeviceReply, Operation, OperationError, PayloadWriter};

/// Three-byte date in device format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeviceDate {
    /// Day of the month.
    pub day: u8,
    /// Month.
    pub month: u8,
    /// Year relative to 1900.
    pub year_since_1900: u8,
}

impl DeviceDate {
    /// Creates a validated device date.
    pub fn new(day: u8, month: u8, year_since_1900: u8) -> Result<Self, OperationError> {
        let date = Self {
            day,
            month,
            year_since_1900,
        };
        validate_date(date)?;
        Ok(date)
    }

    /// Returns the all-zero unspecified date representation.
    pub const fn unspecified() -> Self {
        Self {
            day: 0,
            month: 0,
            year_since_1900: 0,
        }
    }
}

/// Tag, descriptor, and date used by Command 13 or 18.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TagDescriptorDate {
    /// Short tag of up to eight characters.
    pub tag: String,
    /// Descriptor of up to sixteen characters.
    pub descriptor: String,
    /// Device date.
    pub date: DeviceDate,
}

impl TagDescriptorDate {
    /// Creates text fields that will be validated when a write operation is constructed.
    pub fn new(tag: impl Into<String>, descriptor: impl Into<String>, date: DeviceDate) -> Self {
        Self {
            tag: tag.into(),
            descriptor: descriptor.into(),
            date,
        }
    }
}

/// Reads the 32-character message with Command 12.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadMessage;

impl Operation for ReadMessage {
    type Output = String;

    fn command(&self) -> CommandCode {
        CommandCode::new(12)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if reply.data.len() != 24 {
            return Err(OperationError::InvalidValue("message length"));
        }
        Ok(unpack_text(&reply.data))
    }
}

/// Writes the 32-character message with Command 17.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteMessage {
    packed: Vec<u8>,
}

impl WriteMessage {
    /// Validates the character set and creates the operation.
    pub fn new(message: &str) -> Result<Self, OperationError> {
        Ok(Self {
            packed: pack_text(message, 24)?,
        })
    }
}

impl Operation for WriteMessage {
    type Output = String;

    fn command(&self) -> CommandCode {
        CommandCode::new(17)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.packed);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        if reply.data.len() != 24 {
            return Err(OperationError::InvalidValue("message length"));
        }
        Ok(unpack_text(&reply.data))
    }
}

/// Reads the short tag, descriptor, and date with Command 13.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadTagDescriptorDate;

impl Operation for ReadTagDescriptorDate {
    type Output = TagDescriptorDate;

    fn command(&self) -> CommandCode {
        CommandCode::new(13)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_tag_descriptor_date(&reply.data)
    }
}

/// Writes the short tag, descriptor, and date with Command 18.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTagDescriptorDate {
    data: Vec<u8>,
}

impl WriteTagDescriptorDate {
    /// Validates the fields and creates the operation.
    pub fn new(value: &TagDescriptorDate) -> Result<Self, OperationError> {
        validate_date(value.date)?;
        let mut data = pack_text(&value.tag, 6)?;
        data.extend_from_slice(&pack_text(&value.descriptor, 12)?);
        data.extend_from_slice(&[value.date.day, value.date.month, value.date.year_since_1900]);
        Ok(Self { data })
    }
}

impl Operation for WriteTagDescriptorDate {
    type Output = TagDescriptorDate;

    fn command(&self) -> CommandCode {
        CommandCode::new(18)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.data);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_tag_descriptor_date(&reply.data)
    }
}

/// Reads the long tag with Command 20.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadLongTag;

impl Operation for ReadLongTag {
    type Output = String;

    fn command(&self) -> CommandCode {
        CommandCode::new(20)
    }

    fn encode_request(&self, _writer: &mut PayloadWriter) -> Result<(), OperationError> {
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_latin1(&reply.data, 32)
    }
}

/// Writes the long tag with Command 22.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteLongTag {
    data: Vec<u8>,
}

impl WriteLongTag {
    /// Encodes up to 32 Latin-1 characters and pads them with spaces.
    pub fn new(tag: &str) -> Result<Self, OperationError> {
        Ok(Self {
            data: encode_latin1(tag, 32)?,
        })
    }
}

impl Operation for WriteLongTag {
    type Output = String;

    fn command(&self) -> CommandCode {
        CommandCode::new(22)
    }

    fn encode_request(&self, writer: &mut PayloadWriter) -> Result<(), OperationError> {
        writer.bytes(&self.data);
        Ok(())
    }

    fn decode_reply(&self, reply: &DeviceReply) -> Result<Self::Output, OperationError> {
        reply.require_success(self.command())?;
        decode_latin1(&reply.data, 32)
    }
}

/// Packs characters in `0x20..=0x5F`, four characters into three bytes.
pub fn pack_text(text: &str, output_bytes: usize) -> Result<Vec<u8>, OperationError> {
    if output_bytes > usize::from(u8::MAX) || !output_bytes.is_multiple_of(3) {
        return Err(OperationError::InvalidValue("packed-text length"));
    }
    let capacity = output_bytes / 3 * 4;
    let mut codes = Vec::with_capacity(capacity);
    for character in text.chars() {
        let code = u32::from(character);
        if !(0x20..=0x5f).contains(&code) || codes.len() == capacity {
            return Err(OperationError::InvalidValue("packed-text character"));
        }
        codes.push(u8::try_from(code).unwrap_or_default() & 0x3f);
    }
    codes.resize(capacity, b' ' & 0x3f);
    let mut output = Vec::with_capacity(output_bytes);
    for group in codes.chunks_exact(4) {
        output.push((group[0] << 2) | (group[1] >> 4));
        output.push((group[1] << 4) | (group[2] >> 2));
        output.push((group[2] << 6) | group[3]);
    }
    Ok(output)
}

/// Unpacks six-bit text and removes trailing spaces.
pub fn unpack_text(bytes: &[u8]) -> String {
    let mut output = String::new();
    for group in bytes.chunks_exact(3) {
        let codes = [
            group[0] >> 2,
            ((group[0] & 0x03) << 4) | (group[1] >> 4),
            ((group[1] & 0x0f) << 2) | (group[2] >> 6),
            group[2] & 0x3f,
        ];
        for code in codes {
            let ascii = if code & 0x20 == 0 { code | 0x40 } else { code };
            output.push(char::from(ascii));
        }
    }
    output.trim_end().into()
}

fn decode_tag_descriptor_date(data: &[u8]) -> Result<TagDescriptorDate, OperationError> {
    if data.len() != 21 {
        return Err(OperationError::InvalidValue(
            "tag, descriptor, and date length",
        ));
    }
    let device_date = DeviceDate {
        day: data[18],
        month: data[19],
        year_since_1900: data[20],
    };
    validate_date(device_date)?;
    Ok(TagDescriptorDate {
        tag: unpack_text(&data[..6]),
        descriptor: unpack_text(&data[6..18]),
        date: device_date,
    })
}

fn validate_date(date: DeviceDate) -> Result<(), OperationError> {
    if date.day == 0 && date.month == 0 && date.year_since_1900 == 0 {
        return Ok(());
    }
    if !(1..=31).contains(&date.day) || !(1..=12).contains(&date.month) {
        return Err(OperationError::InvalidValue("device date"));
    }
    Ok(())
}

pub(crate) fn encode_latin1(value: &str, length: usize) -> Result<Vec<u8>, OperationError> {
    let mut output = Vec::with_capacity(length);
    for character in value.chars() {
        if output.len() == length || u32::from(character) > 0xff {
            return Err(OperationError::InvalidValue("Latin-1 string"));
        }
        output.push(u8::try_from(u32::from(character)).unwrap_or_default());
    }
    output.resize(length, b' ');
    Ok(output)
}

fn decode_latin1(data: &[u8], length: usize) -> Result<String, OperationError> {
    if data.len() != length {
        return Err(OperationError::InvalidValue("Latin-1 string length"));
    }
    let value: String = data.iter().copied().map(char::from).collect();
    Ok(value.trim_end_matches(['\0', ' ']).into())
}
