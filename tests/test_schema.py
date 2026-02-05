"""
Tests for schema module.

Validates that:
1. PyArrow schemas are properly defined
2. Helper functions work correctly
3. Schema in schema.py matches init_tables.py (structure check)
"""

import re
from pathlib import Path

import pyarrow as pa
import pytest

from lakehouse_provider import schema


class TestSchemaDefinitions:
    """Test schema definitions."""

    def test_namespace_defined(self):
        """Namespace should be defined."""
        assert hasattr(schema, "NAMESPACE")
        assert isinstance(schema.NAMESPACE, str)
        assert len(schema.NAMESPACE) > 0

    def test_schema_version_defined(self):
        """Schema version should be defined."""
        assert hasattr(schema, "SCHEMA_VERSION")
        assert isinstance(schema.SCHEMA_VERSION, str)
        # Should be semver-like
        assert re.match(r"^\d+\.\d+\.\d+", schema.SCHEMA_VERSION)

    def test_tables_dict_defined(self):
        """TABLES dict should contain table schemas."""
        assert hasattr(schema, "TABLES")
        assert isinstance(schema.TABLES, dict)
        assert len(schema.TABLES) > 0

    def test_all_tables_have_pyarrow_schemas(self):
        """All tables should have PyArrow schemas."""
        for table_name, table_schema in schema.TABLES.items():
            assert isinstance(table_schema, pa.Schema), (
                f"{table_name} schema is not a PyArrow Schema"
            )

    def test_primary_table_has_required_fields(self):
        """Primary table should have id and category fields."""
        records_schema = schema.TABLES.get("records")
        assert records_schema is not None, "records table not found"

        field_names = [f.name for f in records_schema]
        assert "id" in field_names, "id field required"
        assert "category" in field_names, "category field required (partition key)"

    def test_id_field_not_nullable(self):
        """ID field should not be nullable."""
        for table_name, table_schema in schema.TABLES.items():
            id_field = table_schema.field("id")
            assert not id_field.nullable, f"{table_name}.id should not be nullable"


class TestSchemaHelpers:
    """Test schema helper functions."""

    def test_get_table_schema(self):
        """get_table_schema should return correct schema."""
        for table_name in schema.TABLES:
            result = schema.get_table_schema(table_name)
            assert isinstance(result, pa.Schema)
            assert result == schema.TABLES[table_name]

    def test_get_table_schema_unknown_table(self):
        """get_table_schema should raise for unknown table."""
        with pytest.raises(ValueError, match="Unknown table"):
            schema.get_table_schema("nonexistent_table")

    def test_get_table_names(self):
        """get_table_names should return list of table names."""
        names = schema.get_table_names()
        assert isinstance(names, list)
        assert len(names) == len(schema.TABLES)
        for name in names:
            assert name in schema.TABLES

    def test_get_field_names(self):
        """get_field_names should return field names for a table."""
        for table_name, table_schema in schema.TABLES.items():
            field_names = schema.get_field_names(table_name)
            expected = [f.name for f in table_schema]
            assert field_names == expected


class TestSchemaInitScriptSync:
    """Test that schema.py and init_tables.py are in sync."""

    def test_init_script_exists(self):
        """init_tables.py should exist."""
        init_script = Path(__file__).parent.parent / "scripts" / "init_tables.py"
        assert init_script.exists(), "scripts/init_tables.py not found"

    def test_init_script_has_matching_tables(self):
        """init_tables.py should define the same tables as schema.py."""
        init_script = Path(__file__).parent.parent / "scripts" / "init_tables.py"
        content = init_script.read_text()

        # Check that each table in schema.py is mentioned in init_tables.py
        for table_name in schema.TABLES:
            assert table_name in content, f"Table '{table_name}' not found in init_tables.py"

    def test_init_script_has_required_fields(self):
        """init_tables.py should have id and category fields for each table."""
        init_script = Path(__file__).parent.parent / "scripts" / "init_tables.py"
        content = init_script.read_text()

        # These fields must appear in the DDL
        required_fields = ["id STRING NOT NULL", "category STRING NOT NULL"]

        for field in required_fields:
            assert field in content, f"Required field '{field}' not in init_tables.py"
