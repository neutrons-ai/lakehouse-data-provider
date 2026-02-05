#!/usr/bin/env python3
"""
Command-line interface for the lakehouse provider.

This CLI provides direct access to provider data without requiring
an MCP server or AI assistant.

Usage:
    lakehouse-provider --help
    lakehouse-provider list-tables
    lakehouse-provider get-schema records
    lakehouse-provider query "SELECT * FROM read_parquet('s3://...') LIMIT 10"
    lakehouse-provider get-record records rec-0001
    lakehouse-provider search records --filter category=A --limit 10
"""

import argparse
import json
import sys
from typing import Any, Optional

from lakehouse_provider import ProviderClient, get_config
from lakehouse_provider.schema import NAMESPACE, SCHEMA_VERSION, TABLE_DESCRIPTIONS


def format_json(data: Any) -> str:
    """Format data as pretty JSON."""
    return json.dumps(data, indent=2, default=str)


def format_table(result) -> str:
    """Format PyArrow table as readable text."""
    lines = []
    lines.append(f"Rows: {len(result)}")

    # Column headers
    headers = result.column_names
    lines.append(" | ".join(headers))
    lines.append("-" * (len(" | ".join(headers))))

    # Rows (limit to first 100 for readability)
    for row in result.to_pylist()[:100]:
        lines.append(" | ".join(str(row.get(h, "")) for h in headers))

    if len(result) > 100:
        lines.append(f"... and {len(result) - 100} more rows")

    return "\n".join(lines)


def cmd_list_tables(args: argparse.Namespace) -> int:
    """List all available tables."""
    client = ProviderClient()
    tables = client.list_tables()

    if args.json:
        table_info = []
        for table_name in tables:
            table_info.append(
                {
                    "name": table_name,
                    "full_name": f"{NAMESPACE}.{table_name}",
                    "description": TABLE_DESCRIPTIONS.get(table_name, ""),
                }
            )
        print(
            format_json(
                {
                    "namespace": NAMESPACE,
                    "schema_version": SCHEMA_VERSION,
                    "tables": table_info,
                }
            )
        )
    else:
        print(f"Namespace: {NAMESPACE}")
        print(f"Schema Version: {SCHEMA_VERSION}")
        print(f"\nTables ({len(tables)}):")
        for table_name in tables:
            description = TABLE_DESCRIPTIONS.get(table_name, "")
            print(f"  • {table_name}")
            if description:
                print(f"    {description}")

    return 0


def cmd_get_schema(args: argparse.Namespace) -> int:
    """Get the schema for a table."""
    client = ProviderClient()

    try:
        schema = client.get_schema(args.table)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    print(f"Table: {NAMESPACE}.{args.table}")
    print(f"Fields ({len(schema)}):")

    for field in schema:
        nullable = "nullable" if field.nullable else "NOT NULL"
        field_info = f"  • {field.name}: {field.type} ({nullable})"

        if field.metadata:
            comment = field.metadata.get(b"comment")
            if comment:
                field_info += f"\n    {comment.decode('utf-8')}"

        print(field_info)

    return 0


def cmd_query(args: argparse.Namespace) -> int:
    """Execute a SQL query."""
    client = ProviderClient()

    try:
        result = client.query(args.sql)
    except Exception as e:
        print(f"Query error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(format_json(result.to_pylist()))
    else:
        print(format_table(result))

    return 0


def cmd_get_record(args: argparse.Namespace) -> int:
    """Get a single record by ID."""
    client = ProviderClient()

    try:
        record = client.get_by_id(args.table, args.id)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if record is None:
        print(f"Record not found: {args.id}", file=sys.stderr)
        return 1

    if args.json:
        print(format_json(record))
    else:
        print(f"Record: {args.table}/{args.id}")
        print("-" * 50)
        for key, value in record.items():
            print(f"{key}: {value}")

    return 0


def cmd_search(args: argparse.Namespace) -> int:
    """Search records with filters."""
    client = ProviderClient()

    # Parse filters from command line
    filters = {}
    if args.filter:
        for filter_str in args.filter:
            if "=" not in filter_str:
                print(f"Invalid filter format: {filter_str}. Use key=value", file=sys.stderr)
                return 1
            key, value = filter_str.split("=", 1)
            # Try to parse as number if possible
            try:
                if "." in value:
                    filters[key] = float(value)
                else:
                    filters[key] = int(value)
            except ValueError:
                filters[key] = value

    try:
        result = client.search(args.table, filters=filters, limit=args.limit)
    except Exception as e:
        print(f"Search error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(format_json(result.to_pylist()))
    else:
        print(format_table(result))

    return 0


def cmd_list_recent(args: argparse.Namespace) -> int:
    """List recent records."""
    client = ProviderClient()

    try:
        result = client.list_recent(args.table, limit=args.limit)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(format_json(result.to_pylist()))
    else:
        print(format_table(result))

    return 0


def cmd_count(args: argparse.Namespace) -> int:
    """Count records in a table."""
    client = ProviderClient()

    # Parse filters
    filters = {}
    if args.filter:
        for filter_str in args.filter:
            if "=" not in filter_str:
                print(f"Invalid filter format: {filter_str}. Use key=value", file=sys.stderr)
                return 1
            key, value = filter_str.split("=", 1)
            # Try to parse as number
            try:
                if "." in value:
                    filters[key] = float(value)
                else:
                    filters[key] = int(value)
            except ValueError:
                filters[key] = value

    try:
        count = client.count(args.table, filters=filters)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(format_json({"table": args.table, "filters": filters, "count": count}))
    else:
        if filters:
            filter_str = ", ".join(f"{k}={v}" for k, v in filters.items())
            print(f"Count ({filter_str}): {count:,}")
        else:
            print(f"Total count: {count:,}")

    return 0


def cmd_config(args: argparse.Namespace) -> int:
    """Show current configuration."""
    config = get_config()

    print("Lakehouse Configuration:")
    print(f"  Namespace: {config.namespace}")
    print(f"  Warehouse: {config.warehouse}")
    print(f"  S3 Endpoint: {config.s3_endpoint}")
    print(f"  S3 Region: {config.s3_region}")
    # Don't show credentials

    return 0


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Lakehouse Provider CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all tables
  lakehouse-provider list-tables
  
  # Get schema for a table
  lakehouse-provider get-schema records
  
  # Execute SQL query
  lakehouse-provider query "SELECT * FROM read_parquet('s3://...') LIMIT 10"
  
  # Get a specific record
  lakehouse-provider get-record records rec-0001
  
  # Search with filters
  lakehouse-provider search records --filter category=A --limit 10
  
  # List recent records
  lakehouse-provider list-recent events --limit 20
  
  # Count records
  lakehouse-provider count records
  lakehouse-provider count records --filter category=A
        """,
    )

    parser.add_argument("--json", action="store_true", help="Output in JSON format")

    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # list-tables command
    list_tables_parser = subparsers.add_parser("list-tables", help="List all available tables")
    list_tables_parser.set_defaults(func=cmd_list_tables)

    # get-schema command
    get_schema_parser = subparsers.add_parser("get-schema", help="Get the schema for a table")
    get_schema_parser.add_argument("table", help="Table name")
    get_schema_parser.set_defaults(func=cmd_get_schema)

    # query command
    query_parser = subparsers.add_parser("query", help="Execute a SQL query")
    query_parser.add_argument("sql", help="SQL query to execute")
    query_parser.set_defaults(func=cmd_query)

    # get-record command
    get_record_parser = subparsers.add_parser("get-record", help="Get a single record by ID")
    get_record_parser.add_argument("table", help="Table name")
    get_record_parser.add_argument("id", help="Record ID")
    get_record_parser.set_defaults(func=cmd_get_record)

    # search command
    search_parser = subparsers.add_parser("search", help="Search records with filters")
    search_parser.add_argument("table", help="Table name")
    search_parser.add_argument(
        "--filter", "-f", action="append", help="Filter in key=value format (can be repeated)"
    )
    search_parser.add_argument(
        "--limit", "-l", type=int, default=100, help="Maximum number of results (default: 100)"
    )
    search_parser.set_defaults(func=cmd_search)

    # list-recent command
    list_recent_parser = subparsers.add_parser("list-recent", help="List recent records")
    list_recent_parser.add_argument("table", help="Table name")
    list_recent_parser.add_argument(
        "--limit", "-l", type=int, default=100, help="Maximum number of results (default: 100)"
    )
    list_recent_parser.set_defaults(func=cmd_list_recent)

    # count command
    count_parser = subparsers.add_parser("count", help="Count records in a table")
    count_parser.add_argument("table", help="Table name")
    count_parser.add_argument(
        "--filter", "-f", action="append", help="Filter in key=value format (can be repeated)"
    )
    count_parser.set_defaults(func=cmd_count)

    # config command
    config_parser = subparsers.add_parser("config", help="Show current configuration")
    config_parser.set_defaults(func=cmd_config)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if "--debug" in sys.argv:
            raise
        return 1


if __name__ == "__main__":
    sys.exit(main())
