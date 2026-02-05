# Lakehouse Data Provider Template

A template for creating data providers that integrate with the **neutron-lakehouse** ecosystem.

## Overview

This package provides:
- **Schema definitions** for Iceberg tables (PyArrow + Spark DDL)
- **Configuration management** for lakehouse connection
- **Query client** using DuckDB for efficient data access
- **Standalone init script** for schema registration via `lakehouse init`
- **Command-line interface** for quick data queries and exploration
- **MCP server** for AI assistant integration (Claude Desktop, etc.)

## Integration with neutron-lakehouse

This provider is designed to work with the [neutron-lakehouse](https://github.com/your-org/neutron-lakehouse) CLI:

```bash
# Initialize lakehouse with your provider
lakehouse init -s scripts/init_tables.py -n template_provider

# Ingest data (handled by lakehouse CLI)
lakehouse ingest --namespace template_provider --source data.parquet

# View registry
lakehouse registry
```

## Quick Start

### Installation

```bash
# Install from source
pip install -e .

# With optional dependencies
pip install -e ".[pandas,dev]"
```

**Note**: The CLI uses the same configuration as the Python client (environment variables or `.env` file).

### Register Your Schema

Register your tables with the lakehouse:

```bash
# From the neutron-lakehouse directory (for .env access)
cd ~/git/neutron-lakehouse
lakehouse init -s ~/git/lakehouse-data-provider/scripts/init_tables.py -n template_provider --use-driver

# Verify registration
lakehouse describe template_provider --use-driver
```

### Generate and Ingest Sample Data

Generate sample Parquet files:

```bash
# Generate sample data
cd ~/git/lakehouse-data-provider
python3 scripts/generate_sample_data.py --output data --records 100 --events 200
```

### Ingestion

The provider includes a schema-specific ingestion script ([scripts/ingest_data.py](scripts/ingest_data.py)) that handles routing Parquet files to the correct tables. This script:
- Reads `iceberg_table` metadata from Parquet file schemas
- Falls back to filename patterns (`records.parquet` → `records` table, `events.parquet` → `events` table)
- Supports both local and S3 paths

To integrate with neutron-lakehouse, the ingest script can be called via spark-submit:

```bash
# Via spark-submit (in Kubernetes environment)
spark-submit scripts/ingest_data.py \
  --input-dir s3a://lakehouse/data/provider-data \
  --namespace template_provider \
  --catalog nessie

# With specific table and mode
spark-submit scripts/ingest_data.py \
  --input-dir /path/to/records.parquet \
  --namespace template_provider \
  --table records \
  --mode overwrite

# Dry run to see routing
spark-submit scripts/ingest_data.py \
  --input-dir /path/to/data \
  --namespace template_provider \
  --dry-run
```

**Note**: The neutron-lakehouse CLI can be extended to use provider-specific ingest scripts, similar to how it uses init scripts.

### Query Data

```python
from lakehouse_provider import ProviderClient

# Create a client (loads config from environment)
client = ProviderClient()

# List available tables
tables = client.list_tables()
print(tables)  # ['records', 'events']

# Query data
result = client.query("SELECT * FROM read_parquet('s3://...') LIMIT 10")

# Get a specific record
record = client.get_by_id("records", "my-record-id")

# Search with filters
matches = client.search("records", filters={"category": "A"})
```

## Configuration

Configuration is loaded from environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `LAKEHOUSE_S3_ENDPOINT` | S3/MinIO endpoint | `http://localhost:9000` |
| `LAKEHOUSE_S3_ACCESS_KEY` | S3 access key | `admin` |
| `LAKEHOUSE_S3_SECRET_KEY` | S3 secret key | `password` |
| `LAKEHOUSE_S3_REGION` | S3 region | `us-east-1` |
| `LAKEHOUSE_WAREHOUSE` | Warehouse path | `s3a://lakehouse/warehouse` |
| `LAKEHOUSE_NAMESPACE` | Provider namespace | `template_provider` |

Or create a `.env` file:

```bash
LAKEHOUSE_S3_ENDPOINT=http://localhost:9000
LAKEHOUSE_S3_ACCESS_KEY=admin
LAKEHOUSE_S3_SECRET_KEY=password
LAKEHOUSE_NAMESPACE=template_provider
```

## Customizing for Your Provider

### 1. Update the Namespace

Edit `src/lakehouse_provider/schema.py`:

```python
NAMESPACE = "your_namespace"
```

And `scripts/init_tables.py`:

```python
parser.add_argument("--database", default="your_namespace", ...)
```

### 2. Define Your Schema

Edit `scripts/init_tables.py` with your Spark DDL:

```python
YOUR_TABLE_DDL = """
    id STRING NOT NULL COMMENT 'Primary key',
    category STRING NOT NULL COMMENT 'Partition key',
    -- Add your fields here
    your_field STRING COMMENT 'Description',
    ...
"""

TABLES = [
    ("your_table", YOUR_TABLE_DDL, "Description of your table"),
]
```

Then mirror the schema in `src/lakehouse_provider/schema.py` with PyArrow:

```python
YOUR_TABLE_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("category", pa.string(), nullable=False),
    pa.field("your_field", pa.string()),
    # ...
])

TABLES = {
    "your_table": YOUR_TABLE_SCHEMA,
}
```

## Command-Line Interface

The package includes a CLI for quick data access without needing the MCP server:

```bash
# List available tables
lakehouse-provider list-tables

# Get table schema
lakehouse-provider get-schema records

# Query data using DuckDB SQL
lakehouse-provider query "SELECT * FROM read_parquet('s3://bucket/path/*.parquet') LIMIT 10"

# Get a specific record by ID
lakehouse-provider get-record records rec-0001

# Search with filters (multiple filters supported)
lakehouse-provider search records --filter category=A --limit 10
lakehouse-provider search records --filter category=A --filter value=42.0

# List recent records (ordered by created_at)
lakehouse-provider list-recent events --limit 20

# Count records (with optional filters)
lakehouse-provider count records
lakehouse-provider count records --filter category=A

# Show configuration (without credentials)
lakehouse-provider config

# Output as JSON (add --json before the command)
lakehouse-provider --json list-tables
lakehouse-provider --json get-schema records
```

## MCP Server

The package includes an MCP (Model Context Protocol) server for AI assistant integration.

### Installation

```bash
# Install with MCP support
pip install -e ".[mcp]"
```

### Running the Server

```bash
# Run MCP server
lakehouse-provider-mcp

# Or via Python module
python -m lakehouse_provider.mcp
```

### Claude Desktop Configuration

Add to your Claude Desktop config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "lakehouse-provider": {
      "command": "lakehouse-provider-mcp",
      "env": {
        "LAKEHOUSE_S3_ENDPOINT": "http://localhost:9000",
        "LAKEHOUSE_S3_ACCESS_KEY": "your-access-key",
        "LAKEHOUSE_S3_SECRET_KEY": "your-secret-key",
        "LAKEHOUSE_NAMESPACE": "template_provider"
      }
    }
  }
}
```

### Available Tools

| Tool | Description |
|------|-------------|
| `query` | Execute SQL queries using DuckDB syntax |
| `get_record` | Fetch a single record by ID |
| `search` | Search records with filters |
| `list_recent` | List recent records by creation time |
| `count` | Count records with optional filters |
| `list_tables` | List available tables |
| `get_schema` | Get table schema (fields and types) |
| `get_config` | Get lakehouse configuration |

### Example Prompts

Once connected, you can ask Claude:

- "List all tables in the lakehouse"
- "Show me the schema for the records table"
- "Search for records with category 'A'"
- "Get the record with ID 'abc-123'"
- "How many events are in the events table?"


## License

MIT

