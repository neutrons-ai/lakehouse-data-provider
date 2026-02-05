# Lakehouse Data Provider Template

A template for creating data providers that integrate with the **neutron-lakehouse** ecosystem.

## Overview

This package provides:
- **Schema definitions** for Iceberg tables (PyArrow + Spark DDL)
- **Configuration management** for lakehouse connection
- **Query client** using DuckDB for efficient data access
- **Standalone init script** for schema registration via `lakehouse init`

## Quick Start

### Installation

```bash
# Install from source
pip install -e .

# With optional dependencies
pip install -e ".[pandas,dev]"
```

### Register Your Schema

Register your tables with the lakehouse:

```bash
# Using the lakehouse CLI
lakehouse init -s scripts/init_tables.py -n template_provider

# Verify registration
lakehouse describe template_provider
```

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

## Project Structure

```
lakehouse-data-provider/
├── pyproject.toml                    # Package configuration
├── README.md                         # This file
├── scripts/
│   └── init_tables.py                # Standalone Spark init script
├── src/
│   └── lakehouse_provider/
│       ├── __init__.py               # Public API
│       ├── schema.py                 # PyArrow schema definitions
│       ├── config.py                 # Configuration management
│       ├── client.py                 # DuckDB query client
│       └── types.py                  # Data models
└── tests/                            # Unit tests
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

### 3. Add Domain-Specific Methods

Extend the `ProviderClient` in `src/lakehouse_provider/client.py`:

```python
def get_by_category(self, category: str, limit: int = 100) -> pa.Table:
    """Get records by category."""
    return self.search("records", filters={"category": category}, limit=limit)
```

## Development

### Setup

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install with dev dependencies
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest
```

### Code Quality

```bash
# Linting
ruff check .

# Type checking
mypy src/
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

## License

MIT

