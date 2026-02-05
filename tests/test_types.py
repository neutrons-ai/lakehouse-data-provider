"""
Tests for type definitions.
"""

import pytest

from lakehouse_provider.types import QueryResult, Record, TableInfo


class TestRecord:
    """Test Record dataclass."""

    def test_create_record(self):
        """Should create a record with required fields."""
        record = Record(id="test-123")
        assert record.id == "test-123"
        assert record.created_at is None
        assert record.metadata == {}

    def test_create_record_with_all_fields(self):
        """Should create a record with all fields."""
        record = Record(
            id="test-123",
            created_at="2026-02-05T10:00:00Z",
            updated_at="2026-02-05T11:00:00Z",
            metadata={"key": "value"},
        )
        assert record.id == "test-123"
        assert record.created_at == "2026-02-05T10:00:00Z"
        assert record.metadata == {"key": "value"}

    def test_from_dict(self):
        """Should create record from dictionary."""
        data = {
            "id": "test-456",
            "created_at": "2026-02-05T10:00:00Z",
            "metadata": {"foo": "bar"},
        }
        record = Record.from_dict(data)
        assert record.id == "test-456"
        assert record.created_at == "2026-02-05T10:00:00Z"
        assert record.metadata == {"foo": "bar"}

    def test_from_dict_missing_fields(self):
        """Should handle missing fields with defaults."""
        data = {"id": "minimal"}
        record = Record.from_dict(data)
        assert record.id == "minimal"
        assert record.created_at is None
        assert record.metadata == {}

    def test_to_dict(self):
        """Should convert record to dictionary."""
        record = Record(
            id="test-789",
            created_at="2026-02-05T10:00:00Z",
            metadata={"test": True},
        )
        data = record.to_dict()
        assert data["id"] == "test-789"
        assert data["created_at"] == "2026-02-05T10:00:00Z"
        assert data["metadata"] == {"test": True}


class TestQueryResult:
    """Test QueryResult dataclass."""

    def test_create_empty_result(self):
        """Should create empty query result."""
        result = QueryResult(records=[], total_count=0)
        assert result.records == []
        assert result.total_count == 0
        assert result.has_more is False

    def test_is_empty(self):
        """is_empty should return True for empty results."""
        empty = QueryResult(records=[], total_count=0)
        assert empty.is_empty is True

        non_empty = QueryResult(
            records=[Record(id="1")],
            total_count=1,
        )
        assert non_empty.is_empty is False


class TestTableInfo:
    """Test TableInfo dataclass."""

    def test_create_table_info(self):
        """Should create table info."""
        info = TableInfo(name="records", namespace="template_provider")
        assert info.name == "records"
        assert info.namespace == "template_provider"

    def test_full_name(self):
        """full_name should return namespace.table."""
        info = TableInfo(name="events", namespace="test_ns")
        assert info.full_name == "test_ns.events"

    def test_with_optional_fields(self):
        """Should handle optional fields."""
        info = TableInfo(
            name="records",
            namespace="template_provider",
            description="Primary records table",
            row_count=1000,
        )
        assert info.description == "Primary records table"
        assert info.row_count == 1000
