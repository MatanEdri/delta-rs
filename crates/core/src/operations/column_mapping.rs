//! Column mapping metadata utilities for schema evolution operations.
//!
//! This module provides helpers to assign, validate, and manage column mapping
//! metadata (physicalName, id, maxColumnId) when creating tables with column
//! mapping, activating column mapping on existing tables, or adding columns
//! to column-mapped tables.

use std::collections::{HashMap, HashSet};

use delta_kernel::schema::{
    ColumnMetadataKey, DataType as DeltaDataType, MetadataValue, StructField, StructType,
};
use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use uuid::Uuid;

/// Controls how physical names are generated for fields that lack them.
#[derive(Clone, Copy)]
pub enum PhysicalNameMode {
    /// Generate a UUID-based physical name (for new columns on a column-mapped table).
    Generated,
    /// Use the logical name as the physical name (for activating column mapping on existing tables,
    /// preserving Parquet file readability).
    Identity,
}

/// Assign column mapping metadata to fields that don't already have it.
/// Uses UUID-based generated physical names. Suitable for new columns added
/// to a table that already has column mapping enabled.
pub fn assign_column_mapping_metadata(fields: &mut [StructField], max_id: &mut i64) {
    assign_column_mapping_metadata_with_mode(fields, max_id, PhysicalNameMode::Generated);
}

/// Assign column mapping metadata using identity mapping (physical name = logical name).
/// Suitable for activating column mapping on an existing table where Parquet files
/// already use the logical column names.
pub fn assign_identity_column_mapping_metadata(fields: &mut [StructField], max_id: &mut i64) {
    assign_column_mapping_metadata_with_mode(fields, max_id, PhysicalNameMode::Identity);
}

fn assign_column_mapping_metadata_with_mode(
    fields: &mut [StructField],
    max_id: &mut i64,
    mode: PhysicalNameMode,
) {
    for field in fields.iter_mut() {
        assign_field_mapping_metadata(field, max_id, mode);
    }
}

fn assign_field_mapping_metadata(field: &mut StructField, max_id: &mut i64, mode: PhysicalNameMode) {
    let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    let id_key = ColumnMetadataKey::ColumnMappingId.as_ref();

    if !field.metadata.contains_key(physical_name_key) {
        let physical_name = match mode {
            PhysicalNameMode::Generated => format!("col-{}", Uuid::new_v4()),
            PhysicalNameMode::Identity => field.name.clone(),
        };
        field.metadata.insert(
            physical_name_key.to_string(),
            MetadataValue::String(physical_name),
        );
    }

    if !field.metadata.contains_key(id_key) {
        *max_id += 1;
        field
            .metadata
            .insert(id_key.to_string(), MetadataValue::Number(*max_id));
    } else if let Some(MetadataValue::Number(existing_id)) = field.metadata.get(id_key) {
        if *existing_id > *max_id {
            *max_id = *existing_id;
        }
    }

    // Recurse into the data type to annotate any nested struct fields at any depth
    if let Some(new_type) = annotate_data_type(field.data_type(), max_id, mode) {
        field.data_type = new_type;
    }
}

/// Recursively walks a data type and annotates any reachable struct fields with column mapping metadata.
/// Returns `Some(new_type)` if any changes were made, `None` otherwise.
fn annotate_data_type(dt: &DeltaDataType, max_id: &mut i64, mode: PhysicalNameMode) -> Option<DeltaDataType> {
    match dt {
        DeltaDataType::Struct(inner) => {
            let mut nested_fields: Vec<StructField> = inner.fields().cloned().collect();
            assign_column_mapping_metadata_with_mode(&mut nested_fields, max_id, mode);
            Some(DeltaDataType::Struct(Box::new(
                StructType::try_new(nested_fields).ok()?,
            )))
        }
        DeltaDataType::Array(inner) => {
            let element_type = inner.element_type();
            annotate_data_type(element_type, max_id, mode).map(|new_element| {
                DeltaDataType::Array(Box::new(delta_kernel::schema::ArrayType::new(
                    new_element,
                    inner.contains_null(),
                )))
            })
        }
        DeltaDataType::Map(inner) => {
            let new_key = annotate_data_type(inner.key_type(), max_id, mode);
            let new_value = annotate_data_type(inner.value_type(), max_id, mode);
            if new_key.is_some() || new_value.is_some() {
                Some(DeltaDataType::Map(Box::new(delta_kernel::schema::MapType::new(
                    new_key.unwrap_or_else(|| inner.key_type().clone()),
                    new_value.unwrap_or_else(|| inner.value_type().clone()),
                    inner.value_contains_null(),
                ))))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Read the column mapping mode from a configuration map.
pub fn column_mapping_mode_from_config(config: &HashMap<String, String>) -> ColumnMappingMode {
    match config.get("delta.columnMapping.mode").map(|s| s.as_str()) {
        Some("name") => ColumnMappingMode::Name,
        Some("id") => ColumnMappingMode::Id,
        _ => ColumnMappingMode::None,
    }
}

/// Validate the column mapping mode property value.
/// Rejects invalid mode values and blocks disabling column mapping on existing column-mapped tables.
pub fn validate_column_mapping_mode_property(
    mode_value: &str,
    current_mode: ColumnMappingMode,
) -> Result<(), crate::DeltaTableError> {
    match mode_value {
        "name" | "id" => Ok(()),
        "none" => {
            if current_mode != ColumnMappingMode::None {
                Err(crate::DeltaTableError::Generic(
                    "Disabling column mapping on a column-mapped table is not supported. \
                     Column mapping cannot be deactivated once enabled."
                        .to_string(),
                ))
            } else {
                Ok(())
            }
        }
        other => Err(crate::DeltaTableError::Generic(format!(
            "Invalid column mapping mode '{other}'. Supported values are 'name' and 'id'."
        ))),
    }
}

/// Get the current maxColumnId from metadata configuration.
pub fn get_max_column_id_from_config(config: &HashMap<String, String>) -> i64 {
    config
        .get("delta.columnMapping.maxColumnId")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Get the current maxColumnId from a schema by inspecting field metadata.
pub fn get_max_column_id(schema: &StructType) -> i64 {
    let id_key = ColumnMetadataKey::ColumnMappingId.as_ref();
    let mut max_id: i64 = 0;
    for field in schema.fields() {
        if let Some(MetadataValue::Number(id)) = field.metadata.get(id_key) {
            if *id > max_id {
                max_id = *id;
            }
        }
    }
    max_id
}

/// Ensure the protocol supports column mapping.
/// For legacy tables (reader < 3): bumps to reader 2 / writer 5.
/// For table-features tables (reader >= 3 or writer >= 7): adds ColumnMapping to both
/// reader and writer features.
pub fn ensure_column_mapping_protocol(
    protocol: crate::kernel::Protocol,
) -> crate::kernel::Protocol {
    use crate::kernel::ProtocolExt as _;

    if protocol.min_reader_version() >= 3 || protocol.min_writer_version() >= 7 {
        // Table-features path: add ColumnMapping to both reader and writer features
        let protocol = protocol.append_reader_features(&[TableFeature::ColumnMapping]);
        protocol.append_writer_features(&[TableFeature::ColumnMapping])
    } else {
        // Legacy path: bump to reader 2 / writer 5 (only if currently lower)
        let reader = protocol.min_reader_version().max(2);
        let writer = protocol.min_writer_version().max(5);
        let mut inner = crate::kernel::ProtocolInner::from_kernel(&protocol);
        inner.min_reader_version = reader;
        inner.min_writer_version = writer;
        inner.as_kernel()
    }
}

/// Validate column mapping metadata consistency across all fields in the schema.
/// Checks for:
/// - Positive maxColumnId
/// - Positive field IDs (> 0)
/// - Duplicate physical names
/// - Duplicate IDs
/// - IDs not exceeding maxColumnId
/// - Correct metadata value types
pub fn validate_column_mapping_metadata(
    fields: &[StructField],
    max_column_id: i64,
) -> Result<(), crate::DeltaTableError> {
    if max_column_id <= 0 {
        return Err(crate::DeltaTableError::Generic(format!(
            "delta.columnMapping.maxColumnId must be positive, got: {max_column_id}"
        )));
    }
    let mut physical_names: HashSet<String> = HashSet::new();
    let mut ids: HashSet<i64> = HashSet::new();

    validate_fields_recursive(fields, &mut physical_names, &mut ids, max_column_id)
}

fn validate_fields_recursive(
    fields: &[StructField],
    physical_names: &mut HashSet<String>,
    ids: &mut HashSet<i64>,
    max_column_id: i64,
) -> Result<(), crate::DeltaTableError> {
    let physical_name_key = ColumnMetadataKey::ColumnMappingPhysicalName.as_ref();
    let id_key = ColumnMetadataKey::ColumnMappingId.as_ref();

    for field in fields {
        // Validate physicalName type and uniqueness
        if let Some(value) = field.metadata.get(physical_name_key) {
            match value {
                MetadataValue::String(name) => {
                    if !physical_names.insert(name.clone()) {
                        return Err(crate::DeltaTableError::Generic(format!(
                            "Duplicate column mapping physicalName '{name}' found in schema."
                        )));
                    }
                }
                other => {
                    return Err(crate::DeltaTableError::Generic(format!(
                        "Column mapping physicalName for field '{}' must be a string, \
                         got: {other:?}",
                        field.name
                    )));
                }
            }
        }

        // Validate id type, uniqueness, positivity, and range
        if let Some(value) = field.metadata.get(id_key) {
            match value {
                MetadataValue::Number(id) => {
                    if *id <= 0 {
                        return Err(crate::DeltaTableError::Generic(format!(
                            "Column mapping id for field '{}' must be positive, got: {id}",
                            field.name
                        )));
                    }
                    if !ids.insert(*id) {
                        return Err(crate::DeltaTableError::Generic(format!(
                            "Duplicate column mapping id {id} found in schema."
                        )));
                    }
                    if *id > max_column_id {
                        return Err(crate::DeltaTableError::Generic(format!(
                            "Column mapping id {id} for field '{}' exceeds \
                             maxColumnId ({max_column_id}).",
                            field.name
                        )));
                    }
                }
                other => {
                    return Err(crate::DeltaTableError::Generic(format!(
                        "Column mapping id for field '{}' must be a number, got: {other:?}",
                        field.name
                    )));
                }
            }
        }

        // Recurse into nested types
        validate_data_type_recursive(field.data_type(), physical_names, ids, max_column_id)?;
    }

    Ok(())
}

fn validate_data_type_recursive(
    dt: &DeltaDataType,
    physical_names: &mut HashSet<String>,
    ids: &mut HashSet<i64>,
    max_column_id: i64,
) -> Result<(), crate::DeltaTableError> {
    match dt {
        DeltaDataType::Struct(inner) => {
            let fields: Vec<_> = inner.fields().cloned().collect();
            validate_fields_recursive(&fields, physical_names, ids, max_column_id)?;
        }
        DeltaDataType::Array(inner) => {
            validate_data_type_recursive(inner.element_type(), physical_names, ids, max_column_id)?;
        }
        DeltaDataType::Map(inner) => {
            validate_data_type_recursive(inner.key_type(), physical_names, ids, max_column_id)?;
            validate_data_type_recursive(inner.value_type(), physical_names, ids, max_column_id)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assign_column_mapping_metadata() {
        let mut fields = vec![
            StructField::new("a", DeltaDataType::INTEGER, false),
            StructField::new("b", DeltaDataType::STRING, true),
        ];
        let mut max_id = 0i64;
        assign_column_mapping_metadata(&mut fields, &mut max_id);

        assert_eq!(max_id, 2);
        for field in &fields {
            assert!(field
                .metadata
                .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()));
            assert!(field
                .metadata
                .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref()));
        }
        // Generated physical names should be UUID-based, not the logical name
        let phys = fields[0]
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap();
        match phys {
            MetadataValue::String(s) => assert!(s.starts_with("col-"), "got: {s}"),
            other => panic!("expected string, got: {other:?}"),
        }
    }

    #[test]
    fn test_assign_preserves_existing_metadata() {
        let mut fields = vec![StructField::new("a", DeltaDataType::INTEGER, false)
            .with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("custom_phys".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(42),
                ),
            ])];
        let mut max_id = 0i64;
        assign_column_mapping_metadata(&mut fields, &mut max_id);

        // Existing metadata should be preserved
        assert_eq!(max_id, 42);
        let phys = fields[0]
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap();
        assert_eq!(
            phys,
            &MetadataValue::String("custom_phys".to_string())
        );
        let id = fields[0]
            .metadata
            .get(ColumnMetadataKey::ColumnMappingId.as_ref())
            .unwrap();
        assert_eq!(id, &MetadataValue::Number(42));
    }

    #[test]
    fn test_assign_nested_struct() {
        let inner = StructType::try_new([StructField::new("x", DeltaDataType::INTEGER, false)])
            .unwrap();
        let mut fields = vec![StructField::new(
            "nested",
            DeltaDataType::Struct(Box::new(inner)),
            true,
        )];
        let mut max_id = 0i64;
        assign_column_mapping_metadata(&mut fields, &mut max_id);

        assert_eq!(max_id, 2); // nested + x
        // The nested struct field "x" should also have metadata
        if let DeltaDataType::Struct(inner) = fields[0].data_type() {
            let x = inner.fields().next().unwrap();
            assert!(x
                .metadata
                .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()));
            assert!(x
                .metadata
                .contains_key(ColumnMetadataKey::ColumnMappingId.as_ref()));
        } else {
            panic!("expected struct type");
        }
    }

    #[test]
    fn test_assign_deeply_nested_array_of_array_of_struct() {
        // array<array<struct<x: int>>>
        let inner_struct =
            StructType::try_new([StructField::new("x", DeltaDataType::INTEGER, false)]).unwrap();
        let inner_array = DeltaDataType::Array(Box::new(delta_kernel::schema::ArrayType::new(
            DeltaDataType::Struct(Box::new(inner_struct)),
            true,
        )));
        let outer_array = DeltaDataType::Array(Box::new(delta_kernel::schema::ArrayType::new(
            inner_array,
            true,
        )));
        let mut fields = vec![StructField::new("col", outer_array, true)];
        let mut max_id = 0i64;
        assign_column_mapping_metadata(&mut fields, &mut max_id);

        // col gets id 1, nested x gets id 2
        assert_eq!(max_id, 2);
        // Verify the deeply nested struct field x has metadata
        if let DeltaDataType::Array(outer) = fields[0].data_type() {
            if let DeltaDataType::Array(inner) = outer.element_type() {
                if let DeltaDataType::Struct(s) = inner.element_type() {
                    let x = s.fields().next().unwrap();
                    assert!(
                        x.metadata
                            .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()),
                        "deeply nested field should have physicalName"
                    );
                } else {
                    panic!("expected struct");
                }
            } else {
                panic!("expected inner array");
            }
        } else {
            panic!("expected outer array");
        }
    }

    #[test]
    fn test_assign_map_with_array_struct_value() {
        // map<string, array<struct<v: int>>>
        let inner_struct =
            StructType::try_new([StructField::new("v", DeltaDataType::INTEGER, false)]).unwrap();
        let array_type = DeltaDataType::Array(Box::new(delta_kernel::schema::ArrayType::new(
            DeltaDataType::Struct(Box::new(inner_struct)),
            true,
        )));
        let map_type = DeltaDataType::Map(Box::new(delta_kernel::schema::MapType::new(
            DeltaDataType::STRING,
            array_type,
            true,
        )));
        let mut fields = vec![StructField::new("m", map_type, true)];
        let mut max_id = 0i64;
        assign_column_mapping_metadata(&mut fields, &mut max_id);

        // m gets id 1, v gets id 2
        assert_eq!(max_id, 2);
        if let DeltaDataType::Map(map) = fields[0].data_type() {
            if let DeltaDataType::Array(arr) = map.value_type() {
                if let DeltaDataType::Struct(s) = arr.element_type() {
                    let v = s.fields().next().unwrap();
                    assert!(
                        v.metadata
                            .contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()),
                        "map value struct field should have physicalName"
                    );
                } else {
                    panic!("expected struct");
                }
            } else {
                panic!("expected array");
            }
        } else {
            panic!("expected map");
        }
    }

    #[test]
    fn test_column_mapping_mode_from_config() {
        let mut config = HashMap::new();
        assert_eq!(
            column_mapping_mode_from_config(&config),
            ColumnMappingMode::None
        );
        config.insert(
            "delta.columnMapping.mode".to_string(),
            "name".to_string(),
        );
        assert_eq!(
            column_mapping_mode_from_config(&config),
            ColumnMappingMode::Name
        );
        config.insert(
            "delta.columnMapping.mode".to_string(),
            "id".to_string(),
        );
        assert_eq!(
            column_mapping_mode_from_config(&config),
            ColumnMappingMode::Id
        );
        config.insert(
            "delta.columnMapping.mode".to_string(),
            "none".to_string(),
        );
        assert_eq!(
            column_mapping_mode_from_config(&config),
            ColumnMappingMode::None
        );
    }

    #[test]
    fn test_assign_identity_column_mapping_metadata() {
        let mut fields = vec![
            StructField::new("alpha", DeltaDataType::INTEGER, false),
            StructField::new("beta", DeltaDataType::STRING, true),
        ];
        let mut max_id = 0i64;
        assign_identity_column_mapping_metadata(&mut fields, &mut max_id);

        assert_eq!(max_id, 2);
        // Identity mapping: physical name = logical name
        let phys_a = fields[0]
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap();
        assert_eq!(
            phys_a,
            &MetadataValue::String("alpha".to_string()),
            "identity mapping should use logical name as physical name"
        );
        let phys_b = fields[1]
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap();
        assert_eq!(
            phys_b,
            &MetadataValue::String("beta".to_string()),
            "identity mapping should use logical name as physical name"
        );
    }

    #[test]
    fn test_identity_mapping_preserves_existing_metadata() {
        let mut fields = vec![StructField::new("a", DeltaDataType::INTEGER, false)
            .with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("custom_phys".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(10),
                ),
            ])];
        let mut max_id = 0i64;
        assign_identity_column_mapping_metadata(&mut fields, &mut max_id);

        assert_eq!(max_id, 10);
        let phys = fields[0]
            .metadata
            .get(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref())
            .unwrap();
        assert_eq!(
            phys,
            &MetadataValue::String("custom_phys".to_string()),
            "existing physicalName should be preserved even with identity mode"
        );
    }

    #[test]
    fn test_ensure_column_mapping_protocol_legacy_bump() {
        let inner = crate::kernel::ProtocolInner {
            min_reader_version: 1,
            min_writer_version: 2,
            reader_features: None,
            writer_features: None,
        };
        let result = ensure_column_mapping_protocol(inner.as_kernel());
        assert_eq!(result.min_reader_version(), 2);
        assert_eq!(result.min_writer_version(), 5);
    }

    #[test]
    fn test_ensure_column_mapping_protocol_table_features() {
        use std::collections::HashSet;
        let inner = crate::kernel::ProtocolInner {
            min_reader_version: 3,
            min_writer_version: 7,
            reader_features: Some(HashSet::from([TableFeature::DeletionVectors])),
            writer_features: Some(HashSet::from([TableFeature::DeletionVectors])),
        };
        let result = ensure_column_mapping_protocol(inner.as_kernel());
        assert_eq!(result.min_reader_version(), 3);
        assert_eq!(result.min_writer_version(), 7);
        let reader_features: Vec<_> = result.reader_features().unwrap().into_iter().collect();
        assert!(
            reader_features.contains(&&TableFeature::ColumnMapping),
            "ColumnMapping should be in reader features"
        );
        let writer_features: Vec<_> = result.writer_features().unwrap().into_iter().collect();
        assert!(
            writer_features.contains(&&TableFeature::ColumnMapping),
            "ColumnMapping should be in writer features"
        );
    }

    #[test]
    fn test_validate_column_mapping_mode_valid_values() {
        assert!(validate_column_mapping_mode_property("name", ColumnMappingMode::None).is_ok());
        assert!(validate_column_mapping_mode_property("id", ColumnMappingMode::None).is_ok());
        assert!(validate_column_mapping_mode_property("none", ColumnMappingMode::None).is_ok());
    }

    #[test]
    fn test_validate_column_mapping_mode_invalid_value() {
        let err = validate_column_mapping_mode_property("invalid", ColumnMappingMode::None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Invalid column mapping mode"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validate_column_mapping_mode_reject_disable() {
        let err = validate_column_mapping_mode_property("none", ColumnMappingMode::Name)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("cannot be deactivated"),
            "got: {err}"
        );
    }

    #[test]
    fn test_validate_metadata_ok() {
        let fields = vec![
            StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("phys_a".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
            ]),
            StructField::new("b", DeltaDataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("phys_b".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
            ]),
        ];
        assert!(validate_column_mapping_metadata(&fields, 2).is_ok());
    }

    #[test]
    fn test_validate_metadata_duplicate_physical_name() {
        let fields = vec![
            StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("same_name".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
            ]),
            StructField::new("b", DeltaDataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("same_name".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(2),
                ),
            ]),
        ];
        let err = validate_column_mapping_metadata(&fields, 2).unwrap_err().to_string();
        assert!(err.contains("Duplicate"), "got: {err}");
    }

    #[test]
    fn test_validate_metadata_duplicate_id() {
        let fields = vec![
            StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("phys_a".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
            ]),
            StructField::new("b", DeltaDataType::STRING, true).with_metadata([
                (
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                    MetadataValue::String("phys_b".to_string()),
                ),
                (
                    ColumnMetadataKey::ColumnMappingId.as_ref(),
                    MetadataValue::Number(1),
                ),
            ]),
        ];
        let err = validate_column_mapping_metadata(&fields, 1).unwrap_err().to_string();
        assert!(err.contains("Duplicate column mapping id"), "got: {err}");
    }

    #[test]
    fn test_validate_metadata_id_exceeds_max() {
        let fields = vec![StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("phys_a".to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(5),
            ),
        ])];
        let err = validate_column_mapping_metadata(&fields, 3).unwrap_err().to_string();
        assert!(err.contains("exceeds"), "got: {err}");
    }

    #[test]
    fn test_validate_metadata_wrong_type_physical_name() {
        let fields = vec![StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::Number(42),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
        ])];
        let err = validate_column_mapping_metadata(&fields, 1).unwrap_err().to_string();
        assert!(err.contains("must be a string"), "got: {err}");
    }

    #[test]
    fn test_validate_metadata_negative_id_rejected() {
        let fields = vec![StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("phys_a".to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(-1),
            ),
        ])];
        let err = validate_column_mapping_metadata(&fields, 1).unwrap_err().to_string();
        assert!(err.contains("must be positive"), "got: {err}");
    }

    #[test]
    fn test_validate_metadata_zero_id_rejected() {
        let fields = vec![StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("phys_a".to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(0),
            ),
        ])];
        let err = validate_column_mapping_metadata(&fields, 1).unwrap_err().to_string();
        assert!(err.contains("must be positive"), "got: {err}");
    }

    #[test]
    fn test_validate_metadata_zero_max_column_id_rejected() {
        let fields = vec![StructField::new("a", DeltaDataType::INTEGER, false).with_metadata([
            (
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref(),
                MetadataValue::String("phys_a".to_string()),
            ),
            (
                ColumnMetadataKey::ColumnMappingId.as_ref(),
                MetadataValue::Number(1),
            ),
        ])];
        let err = validate_column_mapping_metadata(&fields, 0).unwrap_err().to_string();
        assert!(err.contains("maxColumnId must be positive"), "got: {err}");
    }
}
