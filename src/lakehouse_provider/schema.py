"""
Schema definitions for the provider's Iceberg tables.

This module mirrors the schema definitions from scripts/init_tables.py
and provides PyArrow schema equivalents for type-safe operations.

NOTE: Keep this in sync with the Spark DDL in scripts/init_tables.py.
A test in tests/test_schema.py validates that they match.
"""

import pyarrow as pa

# =============================================================================
# Provider Configuration
# =============================================================================

# Namespace for this provider in the lakehouse
NAMESPACE = "template_provider"

# Schema version (increment when making breaking changes)
SCHEMA_VERSION = "1.0.0"

# =============================================================================
# Table Schemas (PyArrow)
# =============================================================================

# Primary data table schema
# Customize this for your specific data model
PRIMARY_TABLE_SCHEMA = pa.schema(
    [
        # Primary key - always required
        pa.field(
            "id", pa.string(), nullable=False, metadata={"comment": "Unique record identifier"}
        ),
        # Partition key - used for efficient querying
        pa.field(
            "category",
            pa.string(),
            nullable=False,
            metadata={"comment": "Category (partition key)"},
        ),
        # Timestamps
        pa.field(
            "created_at", pa.string(), metadata={"comment": "Creation timestamp (ISO format)"}
        ),
        pa.field(
            "updated_at", pa.string(), metadata={"comment": "Last update timestamp (ISO format)"}
        ),
        # Data fields - customize these for your domain
        pa.field("name", pa.string(), metadata={"comment": "Record name"}),
        pa.field("description", pa.string(), metadata={"comment": "Record description"}),
        pa.field("value", pa.float64(), metadata={"comment": "Numeric value"}),
        pa.field("tags", pa.list_(pa.string()), metadata={"comment": "List of tags"}),
        # Nested data example
        pa.field(
            "attributes",
            pa.struct(
                [
                    pa.field("key1", pa.string()),
                    pa.field("key2", pa.int64()),
                ]
            ),
            metadata={"comment": "Additional attributes"},
        ),
        # Ingestion metadata
        pa.field("source_file", pa.string(), metadata={"comment": "Original source filename"}),
        pa.field(
            "ingestion_time", pa.string(), metadata={"comment": "Ingestion timestamp (ISO format)"}
        ),
    ]
)

# Secondary table example (e.g., for time-series data)
EVENTS_TABLE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False, metadata={"comment": "Event ID"}),
        pa.field(
            "record_id",
            pa.string(),
            nullable=False,
            metadata={"comment": "Reference to primary record"},
        ),
        pa.field(
            "category",
            pa.string(),
            nullable=False,
            metadata={"comment": "Category (partition key)"},
        ),
        pa.field("event_time", pa.string(), metadata={"comment": "Event timestamp (ISO format)"}),
        pa.field("event_type", pa.string(), metadata={"comment": "Type of event"}),
        pa.field("event_data", pa.string(), metadata={"comment": "Event payload (JSON)"}),
    ]
)

# =============================================================================
# Table Registry
# =============================================================================

# All tables provided by this provider
TABLES: dict[str, pa.Schema] = {
    "records": PRIMARY_TABLE_SCHEMA,
    "events": EVENTS_TABLE_SCHEMA,
}

# Table descriptions (for documentation and registry)
TABLE_DESCRIPTIONS: dict[str, str] = {
    "records": "Primary data records with attributes and metadata",
    "events": "Time-series events associated with records",
}


# =============================================================================
# Helper Functions
# =============================================================================


def get_table_schema(table_name: str) -> pa.Schema:
    """Get the PyArrow schema for a table."""
    if table_name not in TABLES:
        raise ValueError(f"Unknown table: {table_name}. Available: {list(TABLES.keys())}")
    return TABLES[table_name]


def get_table_names() -> list[str]:
    """Get list of all table names."""
    return list(TABLES.keys())


def get_field_names(table_name: str) -> list[str]:
    """Get list of field names for a table."""
    schema = get_table_schema(table_name)
    return [field.name for field in schema]


def get_partition_keys(table_name: str) -> list[str]:
    """
    Get partition keys for a table.

    By convention, partition keys are the first non-nullable string fields
    after the id field, typically 'category'.
    """
    # This should match the PARTITIONED BY clause in init_tables.py
    return ["category"]
