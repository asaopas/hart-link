# Fuzz targets

Install `cargo-fuzz`, then run either bounded parser target:

```text
cargo fuzz run wire_stream
cargo fuzz run structured_inputs
```

The first target mutates transport fragmentation and decoder limits. The second
feeds the same untrusted bytes to the strict frame inspector, HART-IP packet
decoder, bounded DeviceInfo-style JSON catalog, and bounded FDI/ZIP importer.
