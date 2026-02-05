"""
Tests for the query client.

Note: These tests focus on client initialization and configuration.
Full integration tests require a running lakehouse environment.
"""

import pytest

from lakehouse_provider import ProviderClient, get_client
from lakehouse_provider.config import LakehouseConfig


class TestClientInitialization:
    """Test client initialization."""

    def test_client_with_config(self, mock_config: LakehouseConfig):
        """Client should accept explicit config."""
        client = ProviderClient(config=mock_config)
        assert client.config == mock_config

    def test_client_default_config(self, monkeypatch):
        """Client should load config from environment by default."""
        monkeypatch.setenv("LAKEHOUSE_S3_ENDPOINT", "http://test:9000")
        monkeypatch.setenv("LAKEHOUSE_NAMESPACE", "env_namespace")

        # Clear any cached config
        from lakehouse_provider import config

        config._config = None

        client = ProviderClient()
        assert client.config.s3_endpoint == "http://test:9000"
        assert client.config.namespace == "env_namespace"

    def test_get_client_function(self, mock_config: LakehouseConfig):
        """get_client() convenience function should work."""
        client = get_client(mock_config)
        assert isinstance(client, ProviderClient)
        assert client.config == mock_config


class TestClientConfiguration:
    """Test client configuration methods."""

    def test_s3_endpoint_host(self, mock_config: LakehouseConfig):
        """s3_endpoint_host should strip protocol."""
        assert mock_config.s3_endpoint_host == "localhost:9000"

    def test_warehouse_s3_path(self, mock_config: LakehouseConfig):
        """warehouse_s3_path should convert s3a to s3."""
        assert mock_config.warehouse_s3_path == "s3://test-bucket/warehouse"

    def test_get_table_path_simple(self, mock_config: LakehouseConfig):
        """get_table_path should return correct path."""
        path = mock_config.get_table_path("records")
        assert path == "s3://test-bucket/warehouse/test_provider/records"

    def test_get_table_path_with_uuid(self, mock_config: LakehouseConfig):
        """get_table_path should include UUID when provided."""
        path = mock_config.get_table_path("records", "abc-123")
        assert path == "s3://test-bucket/warehouse/test_provider/records_abc-123"


class TestClientTablePaths:
    """Test client table path handling."""

    def test_set_table_path(self, mock_config: LakehouseConfig):
        """set_table_path should override default path."""
        client = ProviderClient(config=mock_config)
        custom_path = "s3://custom/path/records"
        client.set_table_path("records", custom_path)

        assert client._get_table_path("records") == custom_path

    def test_default_table_path(self, mock_config: LakehouseConfig):
        """Default table path should use config."""
        client = ProviderClient(config=mock_config)
        path = client._get_table_path("records")
        assert path == "s3://test-bucket/warehouse/test_provider/records"


class TestClientSchemaAccess:
    """Test client schema access methods."""

    def test_list_tables(self, mock_config: LakehouseConfig):
        """list_tables should return available tables."""
        client = ProviderClient(config=mock_config)
        tables = client.list_tables()

        assert isinstance(tables, list)
        assert "records" in tables
        assert "events" in tables

    def test_get_schema(self, mock_config: LakehouseConfig):
        """get_schema should return PyArrow schema."""
        import pyarrow as pa

        client = ProviderClient(config=mock_config)
        schema = client.get_schema("records")

        assert isinstance(schema, pa.Schema)
        field_names = [f.name for f in schema]
        assert "id" in field_names


class TestClientContextManager:
    """Test client context manager."""

    def test_context_manager(self, mock_config: LakehouseConfig):
        """Client should work as context manager."""
        with ProviderClient(config=mock_config) as client:
            assert client is not None
            tables = client.list_tables()
            assert len(tables) > 0

    def test_close(self, mock_config: LakehouseConfig):
        """close() should clean up connection."""
        client = ProviderClient(config=mock_config)
        # Access conn to create it
        _ = client.conn
        assert client._conn is not None

        client.close()
        assert client._conn is None
