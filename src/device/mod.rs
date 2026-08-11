//! Versioned data schemas for specific devices.

mod catalog;
mod schema;

pub use catalog::{
    CatalogLimits, CatalogLoadError, DEFAULT_MAXIMUM_CATALOG_JSON_BYTES,
    DEFAULT_MAXIMUM_CATALOG_PROFILES, DeviceCatalog, DeviceKey, DeviceProfile,
    MAXIMUM_PROFILE_COMMANDS, MAXIMUM_PROFILE_TEXT_BYTES, ResponseCodeDefinition,
    ResponseCodeStatus,
};
pub use schema::{
    DataSchema, DynamicRecord, DynamicValue, EnumChoice, FieldKind, FieldSpec,
    MAXIMUM_SCHEMA_FIELDS, MAXIMUM_SCHEMA_NAME_BYTES, SchemaError,
};
