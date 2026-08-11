use std::process::ExitCode;

use clap::{Parser, Subcommand};
use hart_link::{Address, Master, Request, inspect_base64, inspect_exchange, inspect_hex};

#[derive(Debug, Parser)]
#[command(name = "hart-link", about = "Inspect and build HART frames")]
struct Arguments {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect one hexadecimal frame.
    InspectHex {
        /// Bytes with optional whitespace.
        value: String,
    },
    /// Inspect one Base64-encoded frame.
    InspectBase64 {
        /// Standard Base64 string.
        value: String,
    },
    /// Compare a hexadecimal request and response.
    InspectExchange {
        /// Request frame.
        request: String,
        /// Response frame.
        response: String,
    },
    /// Build a request using a short address.
    Build {
        /// Short address in `0..=63`.
        address: u8,
        /// Logical command number in `0..=65535`.
        command: u16,
        /// Hexadecimal request data without `0x`.
        #[arg(default_value = "")]
        data: String,
    },
}

fn main() -> ExitCode {
    match execute(Arguments::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("Error: {message}");
            ExitCode::FAILURE
        }
    }
}

fn execute(arguments: Arguments) -> Result<(), String> {
    match arguments.command {
        Command::InspectHex { value } => {
            println!("{:#?}", inspect_hex(&value).map_err(|error| error.message)?);
        }
        Command::InspectBase64 { value } => {
            println!(
                "{:#?}",
                inspect_base64(&value).map_err(|error| error.message)?
            );
        }
        Command::InspectExchange { request, response } => {
            let request = decode_hex(&request)?;
            let response = decode_hex(&response)?;
            println!(
                "{:#?}",
                inspect_exchange(&request, &response).map_err(|error| error.message)?
            );
        }
        Command::Build {
            address,
            command,
            data,
        } => {
            let address =
                Address::polling(address, Master::Primary).map_err(|error| error.to_string())?;
            let bytes = Request::new(address, command, decode_hex(&data)?)
                .to_frame()
                .and_then(|frame| frame.encode().map_err(Into::into))
                .map_err(|error| error.to_string())?;
            println!("{}", encode_hex(&bytes));
        }
    }
    Ok(())
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    let compact: Vec<u8> = value
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    if !compact.len().is_multiple_of(2) {
        return Err("odd number of hexadecimal digits".into());
    }
    compact
        .chunks_exact(2)
        .enumerate()
        .map(|(index, pair)| {
            let high = nibble(pair[0]).ok_or_else(|| format!("invalid digit at {index}"))?;
            let low = nibble(pair[1]).ok_or_else(|| format!("invalid digit at {index}"))?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02X}");
    }
    output
}
