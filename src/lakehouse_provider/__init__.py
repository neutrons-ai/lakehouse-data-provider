"""
Lakehouse Provider - Template for data providers in the neutron-lakehouse ecosystem.

This package provides:
- Schema definitions for Iceberg tables
- Configuration management for lakehouse connection
- Query client using DuckDB for efficient data access
- (Phase 2) MCP server for AI assistant integration

Quick Start:
    >>> from lakehouse_provider import ProviderClient
    >>>
    >>> # Create a client (loads config from environment)
    >>> client = ProviderClient()
    >>>
    >>> # Query data
    >>> result = client.query("SELECT * FROM records LIMIT 10")
    >>>
    >>> # Get a specific record
    >>> record = client.get_by_id("records", "my-id")
    >>>
    >>> # List available tables
    >>> tables = client.list_tables()
"""

from lakehouse_provider.client import ProviderClient, get_client
from lakehouse_provider.config import LakehouseConfig, get_config, set_config
from lakehouse_provider.schema import (
    NAMESPACE,
    SCHEMA_VERSION,
    TABLES,
    get_field_names,
    get_table_names,
    get_table_schema,
)
from lakehouse_provider.types import QueryResult, Record, TableInfo

__version__ = "0.1.0"

__all__ = [
    # Client
    "ProviderClient",
    "get_client",
    # Config
    "LakehouseConfig",
    "get_config",
    "set_config",
    # Schema
    "NAMESPACE",
    "SCHEMA_VERSION",
    "TABLES",
    "get_table_schema",
    "get_table_names",
    "get_field_names",
    # Types
    "Record",
    "QueryResult",
    "TableInfo",
]
