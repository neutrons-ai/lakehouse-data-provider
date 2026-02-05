"""
Query client for provider data using DuckDB.

This module provides a DuckDB-based client for querying data
directly from S3/Iceberg tables without needing Spark.
"""

from typing import Any, Optional

import duckdb
import pyarrow as pa

from .config import LakehouseConfig, get_config
from .schema import TABLES, get_table_schema


class ProviderClient:
    """
    Query client for provider-specific data.

    Uses DuckDB with httpfs extension for direct S3 access.
    Reads Parquet files from Iceberg table locations.

    Example:
        >>> client = ProviderClient()
        >>> result = client.query("SELECT * FROM records LIMIT 10")
        >>> df = client.query_df("SELECT * FROM records WHERE category = 'A'")
    """

    def __init__(self, config: Optional[LakehouseConfig] = None):
        """
        Initialize the query client.

        Args:
            config: Lakehouse configuration. If None, loads from environment.
        """
        self.config = config or get_config()
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._table_paths: dict[str, str] = {}

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with S3 configured."""
        if self._conn is None:
            self._conn = duckdb.connect()
            self._setup_connection()
        return self._conn

    def _setup_connection(self) -> None:
        """Configure DuckDB connection for S3 access."""
        # Install and load httpfs extension
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

        # Configure S3 credentials
        self._conn.execute(f"""
            SET s3_endpoint='{self.config.s3_endpoint_host}';
            SET s3_access_key_id='{self.config.s3_access_key}';
            SET s3_secret_access_key='{self.config.s3_secret_key}';
            SET s3_region='{self.config.s3_region}';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
        """)

    def set_table_path(self, table_name: str, path: str) -> None:
        """
        Set a custom path for a table.

        Use this to specify table paths with Iceberg UUID suffixes.

        Args:
            table_name: Name of the table
            path: Full S3 path to the table data
        """
        self._table_paths[table_name] = path

    def _get_table_path(self, table_name: str) -> str:
        """Get the S3 path for a table."""
        if table_name in self._table_paths:
            return self._table_paths[table_name]
        return self.config.get_table_path(table_name)

    def _get_parquet_glob(self, table_name: str) -> str:
        """Get glob pattern for reading Parquet files from a table."""
        base_path = self._get_table_path(table_name)
        return f"{base_path}/data/**/*.parquet"

    def query(self, sql: str) -> pa.Table:
        """
        Execute SQL and return PyArrow Table.

        Args:
            sql: SQL query to execute

        Returns:
            PyArrow Table with query results
        """
        return self.conn.execute(sql).fetch_arrow_table()

    def query_df(self, sql: str):
        """
        Execute SQL and return pandas DataFrame.

        Args:
            sql: SQL query to execute

        Returns:
            pandas DataFrame with query results
        """
        return self.conn.execute(sql).fetchdf()

    def read_table(self, table_name: str, limit: Optional[int] = None) -> pa.Table:
        """
        Read all data from a table.

        Args:
            table_name: Name of the table to read
            limit: Maximum number of rows to return

        Returns:
            PyArrow Table with table data
        """
        parquet_glob = self._get_parquet_glob(table_name)
        sql = f"SELECT * FROM read_parquet('{parquet_glob}')"
        if limit:
            sql += f" LIMIT {limit}"
        return self.query(sql)

    def get_by_id(self, table_name: str, record_id: str) -> Optional[dict[str, Any]]:
        """
        Fetch a single record by ID.

        Args:
            table_name: Name of the table
            record_id: ID of the record to fetch

        Returns:
            Record as dictionary, or None if not found
        """
        parquet_glob = self._get_parquet_glob(table_name)
        result = self.query(f"""
            SELECT * FROM read_parquet('{parquet_glob}')
            WHERE id = '{record_id}'
            LIMIT 1
        """)
        if len(result) == 0:
            return None
        # Convert to dict
        return {col: result.column(col)[0].as_py() for col in result.column_names}

    def list_recent(
        self,
        table_name: str,
        limit: int = 100,
        order_by: str = "created_at",
    ) -> pa.Table:
        """
        List recent records from a table.

        Args:
            table_name: Name of the table
            limit: Maximum number of records to return
            order_by: Column to order by (descending)

        Returns:
            PyArrow Table with recent records
        """
        parquet_glob = self._get_parquet_glob(table_name)
        return self.query(f"""
            SELECT * FROM read_parquet('{parquet_glob}')
            ORDER BY {order_by} DESC
            LIMIT {limit}
        """)

    def search(
        self,
        table_name: str,
        filters: Optional[dict[str, Any]] = None,
        limit: int = 100,
    ) -> pa.Table:
        """
        Search records with filters.

        Args:
            table_name: Name of the table
            filters: Dictionary of field=value filters (AND logic)
            limit: Maximum number of records to return

        Returns:
            PyArrow Table with matching records
        """
        parquet_glob = self._get_parquet_glob(table_name)
        sql = f"SELECT * FROM read_parquet('{parquet_glob}')"

        if filters:
            conditions = []
            for field, value in filters.items():
                if isinstance(value, str):
                    conditions.append(f"{field} = '{value}'")
                elif isinstance(value, (int, float)):
                    conditions.append(f"{field} = {value}")
                elif value is None:
                    conditions.append(f"{field} IS NULL")
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)

        sql += f" LIMIT {limit}"
        return self.query(sql)

    def count(self, table_name: str, filters: Optional[dict[str, Any]] = None) -> int:
        """
        Count records in a table.

        Args:
            table_name: Name of the table
            filters: Optional dictionary of field=value filters

        Returns:
            Number of matching records
        """
        parquet_glob = self._get_parquet_glob(table_name)
        sql = f"SELECT COUNT(*) as cnt FROM read_parquet('{parquet_glob}')"

        if filters:
            conditions = []
            for field, value in filters.items():
                if isinstance(value, str):
                    conditions.append(f"{field} = '{value}'")
                elif isinstance(value, (int, float)):
                    conditions.append(f"{field} = {value}")
            if conditions:
                sql += " WHERE " + " AND ".join(conditions)

        result = self.query(sql)
        return result.column("cnt")[0].as_py()

    def get_schema(self, table_name: str) -> pa.Schema:
        """
        Get the PyArrow schema for a table.

        Args:
            table_name: Name of the table

        Returns:
            PyArrow Schema
        """
        return get_table_schema(table_name)

    def list_tables(self) -> list[str]:
        """
        List all available tables.

        Returns:
            List of table names
        """
        return list(TABLES.keys())

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "ProviderClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()


# Convenience function for getting a client
def get_client(config: Optional[LakehouseConfig] = None) -> ProviderClient:
    """
    Get a ProviderClient instance.

    Args:
        config: Optional configuration. If None, loads from environment.

    Returns:
        ProviderClient instance
    """
    return ProviderClient(config)
