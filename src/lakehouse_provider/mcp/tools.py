"""
MCP tool definitions for the lakehouse provider.

This module defines the MCP tools that expose provider data
to AI assistants like Claude.
"""

from typing import Any, Optional

# Tool metadata for MCP registration
TOOLS = [
    {
        "name": "query",
        "description": "Execute a SQL query against the provider's Iceberg tables. "
        "Use DuckDB SQL syntax. Tables are accessed via read_parquet() function.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "SQL query to execute. Example: SELECT * FROM read_parquet('s3://...') LIMIT 10",
                },
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_record",
        "description": "Fetch a single record by its ID from a specified table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Name of the table (e.g., 'records', 'events')",
                },
                "id": {
                    "type": "string",
                    "description": "The unique ID of the record to fetch",
                },
            },
            "required": ["table", "id"],
        },
    },
    {
        "name": "search",
        "description": "Search for records in a table with optional filters. "
        "Returns matching records up to the specified limit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Name of the table to search",
                },
                "filters": {
                    "type": "object",
                    "description": "Key-value pairs for filtering. Example: {'category': 'A'}",
                    "additionalProperties": True,
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of records to return (default: 100)",
                    "default": 100,
                },
            },
            "required": ["table"],
        },
    },
    {
        "name": "list_recent",
        "description": "List the most recent records from a table, ordered by creation time.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Name of the table",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of records to return (default: 100)",
                    "default": 100,
                },
            },
            "required": ["table"],
        },
    },
    {
        "name": "count",
        "description": "Count records in a table, optionally with filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Name of the table",
                },
                "filters": {
                    "type": "object",
                    "description": "Optional key-value pairs for filtering",
                    "additionalProperties": True,
                },
            },
            "required": ["table"],
        },
    },
    {
        "name": "list_tables",
        "description": "List all available tables in this provider's namespace.",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_schema",
        "description": "Get the schema (field names and types) for a table.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Name of the table",
                },
            },
            "required": ["table"],
        },
    },
    {
        "name": "get_config",
        "description": "Get the current lakehouse configuration (without sensitive credentials).",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
]


def get_tool_by_name(name: str) -> Optional[dict]:
    """Get a tool definition by name."""
    for tool in TOOLS:
        if tool["name"] == name:
            return tool
    return None


def get_all_tools() -> list[dict]:
    """Get all tool definitions."""
    return TOOLS
