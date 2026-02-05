# Lakehouse Data Provider Template - Design Document

## Overview

This package serves as a **template** for creating data providers that integrate with the **neutron-lakehouse** ecosystem. Each provider defines its own Iceberg table schema, provides a standalone schema registration script (compatible with `lakehouse init`), and includes a query client.

### Reference: neutron-lakehouse

The lakehouse CLI is implemented in `neutron-lakehouse` and provides:
- `lakehouse init -s <script.py>` - Register a namespace from an init script
- `lakehouse ingest` - Ingest data into registered namespaces
- `lakehouse registry` - View registered namespaces and tables
- `lakehouse describe <namespace>` - Show namespace details

The lakehouse uses:
- **Nessie** as the Iceberg catalog
- **S3/MinIO** for storage (`s3a://lakehouse/warehouse`)
- **Spark** for DDL operations (via spark-submit)
- **PyIceberg** for direct table access (queries)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Provider Package                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐ │
│  │   Init       │   │   Query      │   │   MCP Server         │ │
│  │   Script     │   │   Client     │   │   (Phase 2)          │ │
│  │   (Spark)    │   │   (DuckDB)   │   │                      │ │
│  └──────┬───────┘   └──────┬───────┘   └──────────┬───────────┘ │
│         │                  │                      │             │
│         ▼                  ▼                      ▼             │
│  ┌──────────────────────────────────────────────────────────────┐│
│  │              Core Types & Configuration                      ││
│  └──────────────────────────────────────────────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                           │
         ▼                           ▼
┌─────────────────┐         ┌─────────────────────────────────────┐
│  lakehouse CLI  │         │  Data Lakehouse                     │
│  (init, ingest) │         │  • Nessie Catalog                   │
│                 │         │  • S3/MinIO Storage                 │
│                 │         │  • Iceberg Tables                   │
└─────────────────┘         └─────────────────────────────────────┘
```

---

## Package Structure

```
lakehouse-data-provider/
├── pyproject.toml                    # Package configuration
├── README.md                         # Usage documentation
├── LICENSE
├── docs/
│   ├── project.md
│   └── design.md
├── scripts/
│   └── init_tables.py                # Standalone Spark init script
├── src/
│   └── lakehouse_provider/           # Main package (rename per provider)
│       ├── __init__.py
│       ├── schema.py                 # Schema constants (mirrors init script)
│       ├── config.py                 # Configuration management
│       ├── client.py                 # Query client using DuckDB
│       ├── types.py                  # Data models and type definitions
│       └── mcp/                      # Phase 2: MCP server
│           ├── __init__.py
│           ├── server.py             # MCP server implementation
│           └── tools.py              # MCP tool definitions
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_schema.py
    ├── test_client.py
    └── test_mcp.py                   # Phase 2
```

---

## Component Design

### 1. Init Script (`scripts/init_tables.py`)

A **self-contained PySpark script** that creates Iceberg tables. This follows the pattern established by `init_neutron_tables.py` in neutron-lakehouse.

**Key Requirements:**
- Uses **PySpark** (not PyArrow) for table creation
- NO imports from lakehouse_provider package (self-contained)
- Schema defined as **Spark DDL strings**
- Tables partitioned appropriately
- Compatible with `lakehouse init -s init_tables.py`

**Pattern from neutron-lakehouse:**
```python
#!/usr/bin/env python3
"""
Initialize Iceberg Tables for <Provider> Data

This is a SELF-CONTAINED script that creates Iceberg tables.
It has NO external dependencies beyond PySpark.

Usage:
    spark-submit init_tables.py --catalog nessie --database my_namespace
    # OR via lakehouse CLI:
    lakehouse init -s init_tables.py -n my_namespace
"""

import argparse
from pyspark.sql import SparkSession

# Schema as Spark DDL
MY_TABLE_DDL = """
    id STRING NOT NULL COMMENT 'Primary key',
    created_at STRING COMMENT 'Creation timestamp (ISO format)',
    ...
"""

TABLES = [
    ("my_table", MY_TABLE_DDL, "Description of my_table"),
]

def create_table(spark, catalog, database, table_name, ddl, description):
    full_table = f"{catalog}.{database}.{table_name}"
    sql = f"""
    CREATE TABLE {full_table} (
        {ddl}
    )
    USING iceberg
    PARTITIONED BY (some_partition_key)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'comment' = '{description}'
    )
    """
    spark.sql(sql)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="nessie")
    parser.add_argument("--database", default="my_namespace")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("MyTableInit").getOrCreate()
    # Create namespace and tables...
```

---

### 2. Schema Module (`schema.py`)

Mirrors the schema definitions from the init script for use in the Python package. Provides PyArrow schema equivalents for type-safe operations.

```python
"""Schema definitions for the provider's Iceberg tables."""
import pyarrow as pa

NAMESPACE = "my_namespace"
SCHEMA_VERSION = "1.0.0"

# PyArrow schema (equivalent to the Spark DDL in init_tables.py)
MY_TABLE_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("created_at", pa.string()),
    # ... other fields
])

TABLES = {
    "my_table": MY_TABLE_SCHEMA,
}
```

**Note:** A test ensures the init script and schema.py stay in sync.

---

### 3. Configuration (`config.py`)

Manages connection to the lakehouse, following the pattern from `neutron_lakehouse.iceberg.client`.

```python
"""Configuration for lakehouse connection."""
import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class LakehouseConfig:
    """Configuration for connecting to the lakehouse."""
    
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str = "us-east-1"
    warehouse: str = "s3a://lakehouse/warehouse"
    namespace: str = "my_namespace"
    
    @classmethod
    def from_env(cls) -> "LakehouseConfig":
        """Load configuration from environment variables."""
        return cls(
            s3_endpoint=os.getenv("LAKEHOUSE_S3_ENDPOINT", "http://localhost:9000"),
            s3_access_key=os.getenv("LAKEHOUSE_S3_ACCESS_KEY", "admin"),
            s3_secret_key=os.getenv("LAKEHOUSE_S3_SECRET_KEY", "password"),
            s3_region=os.getenv("LAKEHOUSE_S3_REGION", "us-east-1"),
            warehouse=os.getenv("LAKEHOUSE_WAREHOUSE", "s3a://lakehouse/warehouse"),
            namespace=os.getenv("LAKEHOUSE_NAMESPACE", "my_namespace"),
        )
```

**Environment Variables:**
| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_S3_ENDPOINT` | S3/MinIO endpoint | `http://localhost:9000` |
| `LAKEHOUSE_S3_ACCESS_KEY` | S3 access key | `admin` |
| `LAKEHOUSE_S3_SECRET_KEY` | S3 secret key | `password` |
| `LAKEHOUSE_S3_REGION` | S3 region | `us-east-1` |
| `LAKEHOUSE_WAREHOUSE` | Warehouse path | `s3a://lakehouse/warehouse` |
| `LAKEHOUSE_NAMESPACE` | Target namespace | Provider-specific |

---

### 4. Query Client (`client.py`)

DuckDB-based client for querying data directly from S3/Iceberg.

```python
"""Query client for provider data using DuckDB."""
import duckdb
import pyarrow as pa
from typing import Optional, Any

from .config import LakehouseConfig
from .schema import NAMESPACE

class ProviderClient:
    """Query client for provider-specific data."""
    
    def __init__(self, config: Optional[LakehouseConfig] = None):
        self.config = config or LakehouseConfig.from_env()
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
    
    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection with S3 configured."""
        if self._conn is None:
            self._conn = duckdb.connect()
            self._conn.execute("INSTALL httpfs; LOAD httpfs;")
            self._conn.execute(f"""
                SET s3_endpoint='{self.config.s3_endpoint.replace("http://", "")}';
                SET s3_access_key_id='{self.config.s3_access_key}';
                SET s3_secret_access_key='{self.config.s3_secret_key}';
                SET s3_region='{self.config.s3_region}';
                SET s3_url_style='path';
            """)
        return self._conn
    
    def query(self, sql: str) -> pa.Table:
        """Execute SQL and return PyArrow Table."""
        return self.conn.execute(sql).fetch_arrow_table()
    
    def query_df(self, sql: str):
        """Execute SQL and return pandas DataFrame."""
        return self.conn.execute(sql).fetchdf()
    
    # Provider-specific convenience methods
    def get_by_id(self, table: str, id: str) -> Optional[dict]:
        """Fetch a single record by ID."""
        result = self.query(f"""
            SELECT * FROM read_parquet('{self._table_path(table)}/**/*.parquet')
            WHERE id = '{id}'
            LIMIT 1
        """)
        if len(result) == 0:
            return None
        return result.to_pydict()
    
    def list_recent(self, table: str, limit: int = 100) -> pa.Table:
        """List recent records."""
        return self.query(f"""
            SELECT * FROM read_parquet('{self._table_path(table)}/**/*.parquet')
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
    
    def _table_path(self, table: str) -> str:
        """Get S3 path for a table."""
        # Note: Actual path includes UUID suffix assigned by Iceberg
        # This is a simplified version - real implementation needs metadata
        return f"{self.config.warehouse}/{self.config.namespace}/{table}"
```

**DuckDB Integration:**
- Uses `httpfs` extension for S3 access
- Reads Parquet files directly from S3
- Returns PyArrow Tables for efficiency
- Optional pandas support via `query_df()`

---

### 5. MCP Server (Phase 2) (`mcp/`)

Model Context Protocol server for AI assistant integration.

```python
# Tools exposed:
# - query_data: Execute SQL queries on provider tables
# - get_record: Fetch specific record by ID
# - search_records: Search with filters
# - list_tables: List available tables
# - get_schema: Return schema information
```

**Entry Point:**
```bash
# Run MCP server
python -m lakehouse_provider.mcp

# Or via installed script
lakehouse-provider-mcp
```

---

## Dependencies

### Phase 1 (Core)
```toml
[project]
dependencies = [
    "pyarrow>=14.0.0",
    "duckdb>=0.9.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
pandas = ["pandas>=2.0.0"]
s3 = ["s3fs>=2024.0.0"]
```

### Phase 2 (MCP)
```toml
[project.optional-dependencies]
mcp = [
    "mcp>=1.0.0",
]
all = [
    "lakehouse-provider[pandas,s3,mcp]",
]
```

**Note:** The init script only requires PySpark (provided by the lakehouse environment).

---

## Implementation Plan

### Phase 1: Core Data Provider

| # | Task | Description | Files |
|---|------|-------------|-------|
| 1.1 | Project Setup | Initialize pyproject.toml with hatchling build, dependencies | `pyproject.toml` |
| 1.2 | Type Definitions | Define data models using dataclasses | `src/lakehouse_provider/types.py` |
| 1.3 | Schema Definition | Create PyArrow schema with example fields | `src/lakehouse_provider/schema.py` |
| 1.4 | Init Script | Self-contained PySpark table init script | `scripts/init_tables.py` |
| 1.5 | Configuration | Environment-based config (match neutron-lakehouse) | `src/lakehouse_provider/config.py` |
| 1.6 | Query Client | DuckDB-based query client with S3 support | `src/lakehouse_provider/client.py` |
| 1.7 | Package Init | Public API exports | `src/lakehouse_provider/__init__.py` |
| 1.8 | Tests | Unit tests for schema and client | `tests/` |
| 1.9 | Documentation | README with usage examples | `README.md` |

#### Task Details

**1.1 Project Setup**
```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "lakehouse-provider"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "pyarrow>=14.0.0",
    "duckdb>=0.9.0",
    "python-dotenv>=1.0.0",
]

[project.optional-dependencies]
mcp = ["mcp>=1.0.0"]
dev = ["pytest>=7.0.0", "ruff>=0.1.0"]

[tool.hatch.build.targets.wheel]
packages = ["src/lakehouse_provider"]
```

**1.4 Init Script Pattern**
Follow `init_neutron_tables.py`:
- Define DDL strings for each table
- Create namespace with `CREATE NAMESPACE IF NOT EXISTS`
- Create tables with partitioning and compression settings
- Support `--catalog` and `--database` CLI args
- Compatible with `lakehouse init -s init_tables.py`

**1.5 Configuration Pattern**
Follow `neutron_lakehouse.iceberg.client`:
- Load from environment variables
- Support local development defaults
- Use dataclass for type safety

**1.6 Query Client Pattern**
Use DuckDB with httpfs extension:
- Configure S3 credentials
- Read Parquet files directly
- Return PyArrow tables

---

### Phase 2: MCP Server

| # | Task | Description | Files |
|---|------|-------------|-------|
| 2.1 | MCP Dependencies | Add mcp package to optional deps | `pyproject.toml` |
| 2.2 | Tool Definitions | Define MCP tools for data access | `src/lakehouse_provider/mcp/tools.py` |
| 2.3 | Server Implementation | MCP server setup and handlers | `src/lakehouse_provider/mcp/server.py` |
| 2.4 | MCP Init | Package exports | `src/lakehouse_provider/mcp/__init__.py` |
| 2.5 | Entry Point | CLI command to run server | `pyproject.toml` (entry point) |
| 2.6 | MCP Tests | Integration tests for MCP server | `tests/test_mcp.py` |
| 2.7 | MCP Documentation | Setup guide for Claude Desktop | `README.md` |

#### Tool Definitions

| Tool | Description | Parameters |
|------|-------------|------------|
| `query` | Execute SQL query | `sql: str` |
| `get_record` | Fetch record by ID | `table: str, id: str` |
| `search` | Search with filters | `table: str, filters: dict` |
| `list_tables` | List available tables | (none) |
| `get_schema` | Get table schema | `table: str` |

---

## Template Customization Guide

When creating a new data provider from this template:

1. **Rename the package**: `lakehouse_provider` → `your_provider_name`
2. **Update `scripts/init_tables.py`**: Define your Iceberg tables with Spark DDL
3. **Update `schema.py`**: Mirror the schema as PyArrow (keep in sync!)
4. **Update `types.py`**: Define your record types
5. **Update `client.py`**: Add domain-specific query methods
6. **Update `config.py`**: Set your default namespace
7. **Update MCP tools**: Customize tools for your data domain

---

## Security Considerations

- **No credentials in code**: Use environment variables
- **Read-only by default**: Query client should not modify data
- **MCP validation**: Validate all MCP tool inputs
- **SQL injection prevention**: Use parameterized queries where possible

---

## Integration with neutron-lakehouse

### Registering Your Namespace

```bash
# Clone your provider repo
cd /path/to/your-provider

# Register with the lakehouse
lakehouse init -s scripts/init_tables.py -n your_namespace

# Verify registration
lakehouse describe your_namespace
```

### Data Ingestion

Data ingestion is handled by the `lakehouse` CLI. Your provider focuses on:
1. Schema definition (what tables exist)
2. Querying (how to read the data)

---

## Future Considerations

- Schema migration tooling
- Multiple table support with cross-table queries
- Async query support
- Caching layer for repeated queries
- Metrics and observability
- Integration with lakehouse's ingest command
