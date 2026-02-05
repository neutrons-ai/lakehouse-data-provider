"""
Configuration for lakehouse connection.

This module manages configuration for connecting to the lakehouse,
following the pattern from neutron-lakehouse.

Configuration is loaded from environment variables with sensible
defaults for local development.
"""

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

# Load .env file if present
load_dotenv()


@dataclass
class LakehouseConfig:
    """
    Configuration for connecting to the lakehouse.

    Attributes:
        s3_endpoint: S3/MinIO endpoint URL
        s3_access_key: S3 access key ID
        s3_secret_key: S3 secret access key
        s3_region: S3 region
        warehouse: Warehouse base path (e.g., s3a://lakehouse/warehouse)
        namespace: Default namespace for this provider
    """

    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str = "us-east-1"
    warehouse: str = "s3a://lakehouse/warehouse"
    namespace: str = "template_provider"

    @classmethod
    def from_env(cls) -> "LakehouseConfig":
        """
        Load configuration from environment variables.

        Environment variables:
            LAKEHOUSE_S3_ENDPOINT: S3/MinIO endpoint (default: http://localhost:9000)
            LAKEHOUSE_S3_ACCESS_KEY: S3 access key (default: admin)
            LAKEHOUSE_S3_SECRET_KEY: S3 secret key (default: password)
            LAKEHOUSE_S3_REGION: S3 region (default: us-east-1)
            LAKEHOUSE_WAREHOUSE: Warehouse path (default: s3a://lakehouse/warehouse)
            LAKEHOUSE_NAMESPACE: Provider namespace (default: template_provider)
        """
        return cls(
            s3_endpoint=os.getenv("LAKEHOUSE_S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key=os.getenv("LAKEHOUSE_S3_ACCESS_KEY", "admin"),
            s3_secret_key=os.getenv("LAKEHOUSE_S3_SECRET_KEY", "password"),
            s3_region=os.getenv("LAKEHOUSE_S3_REGION", "us-east-1"),
            warehouse=os.getenv("LAKEHOUSE_WAREHOUSE", "s3a://lakehouse/warehouse"),
            namespace=os.getenv("LAKEHOUSE_NAMESPACE", "template_provider"),
        )

    @property
    def s3_endpoint_host(self) -> str:
        """Get S3 endpoint without protocol (for DuckDB)."""
        return self.s3_endpoint.replace("http://", "").replace("https://", "")

    @property
    def warehouse_s3_path(self) -> str:
        """Get warehouse path as s3:// URL (for DuckDB)."""
        return self.warehouse.replace("s3a://", "s3://")

    def get_table_path(self, table_name: str, table_uuid: Optional[str] = None) -> str:
        """
        Get the S3 path for a table.

        Args:
            table_name: Name of the table
            table_uuid: Optional UUID suffix (assigned by Iceberg)

        Returns:
            S3 path to the table data
        """
        if table_uuid:
            table_dir = f"{table_name}_{table_uuid}"
        else:
            table_dir = table_name
        return f"{self.warehouse_s3_path}/{self.namespace}/{table_dir}"


# Global configuration instance (lazy-loaded)
_config: Optional[LakehouseConfig] = None


def get_config() -> LakehouseConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = LakehouseConfig.from_env()
    return _config


def set_config(config: LakehouseConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config
