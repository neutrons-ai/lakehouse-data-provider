"""
MCP Server implementation for the lakehouse provider.

This module implements a Model Context Protocol (MCP) server that
exposes the provider's data to AI assistants.
"""

import json
import logging
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from lakehouse_provider.client import ProviderClient
from lakehouse_provider.config import get_config
from lakehouse_provider.mcp.tools import TOOLS
from lakehouse_provider.schema import NAMESPACE, SCHEMA_VERSION, TABLE_DESCRIPTIONS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LakehouseProviderServer:
    """
    MCP Server for the lakehouse provider.

    Exposes data query capabilities to AI assistants via the
    Model Context Protocol.
    """

    def __init__(self):
        """Initialize the MCP server."""
        self.server = Server("lakehouse-provider")
        self.client: ProviderClient | None = None
        self._setup_handlers()

    def _get_client(self) -> ProviderClient:
        """Get or create the provider client."""
        if self.client is None:
            self.client = ProviderClient()
        return self.client

    def _setup_handlers(self) -> None:
        """Set up MCP request handlers."""

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """List available tools."""
            return [
                Tool(
                    name=tool["name"],
                    description=tool["description"],
                    inputSchema=tool["inputSchema"],
                )
                for tool in TOOLS
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
            """Handle tool calls."""
            try:
                result = await self._handle_tool(name, arguments)
                return [TextContent(type="text", text=result)]
            except Exception as e:
                logger.error(f"Error in tool {name}: {e}")
                return [TextContent(type="text", text=f"Error: {str(e)}")]

    async def _handle_tool(self, name: str, arguments: dict[str, Any]) -> str:
        """
        Handle a tool call and return the result as a string.

        Args:
            name: Name of the tool to call
            arguments: Tool arguments

        Returns:
            JSON-formatted result string
        """
        client = self._get_client()

        if name == "query":
            sql = arguments.get("sql", "")
            if not sql:
                return json.dumps({"error": "SQL query is required"})

            result = client.query(sql)
            # Convert PyArrow table to list of dicts
            records = result.to_pylist()
            return json.dumps(
                {
                    "row_count": len(records),
                    "records": records[:1000],  # Limit to 1000 rows for safety
                },
                default=str,
            )

        elif name == "get_record":
            table = arguments.get("table", "")
            record_id = arguments.get("id", "")
            if not table or not record_id:
                return json.dumps({"error": "Both 'table' and 'id' are required"})

            record = client.get_by_id(table, record_id)
            if record is None:
                return json.dumps({"error": f"Record not found: {record_id}"})
            return json.dumps({"record": record}, default=str)

        elif name == "search":
            table = arguments.get("table", "")
            if not table:
                return json.dumps({"error": "Table name is required"})

            filters = arguments.get("filters", {})
            limit = arguments.get("limit", 100)

            result = client.search(table, filters=filters, limit=limit)
            records = result.to_pylist()
            return json.dumps(
                {
                    "table": table,
                    "filters": filters,
                    "row_count": len(records),
                    "records": records,
                },
                default=str,
            )

        elif name == "list_recent":
            table = arguments.get("table", "")
            if not table:
                return json.dumps({"error": "Table name is required"})

            limit = arguments.get("limit", 100)
            result = client.list_recent(table, limit=limit)
            records = result.to_pylist()
            return json.dumps(
                {
                    "table": table,
                    "row_count": len(records),
                    "records": records,
                },
                default=str,
            )

        elif name == "count":
            table = arguments.get("table", "")
            if not table:
                return json.dumps({"error": "Table name is required"})

            filters = arguments.get("filters", {})
            count = client.count(table, filters=filters)
            return json.dumps(
                {
                    "table": table,
                    "filters": filters,
                    "count": count,
                }
            )

        elif name == "list_tables":
            tables = client.list_tables()
            table_info = []
            for table_name in tables:
                table_info.append(
                    {
                        "name": table_name,
                        "description": TABLE_DESCRIPTIONS.get(table_name, ""),
                        "full_name": f"{NAMESPACE}.{table_name}",
                    }
                )
            return json.dumps(
                {
                    "namespace": NAMESPACE,
                    "tables": table_info,
                }
            )

        elif name == "get_schema":
            table = arguments.get("table", "")
            if not table:
                return json.dumps({"error": "Table name is required"})

            try:
                schema = client.get_schema(table)
                fields = []
                for field in schema:
                    field_info = {
                        "name": field.name,
                        "type": str(field.type),
                        "nullable": field.nullable,
                    }
                    if field.metadata:
                        comment = field.metadata.get(b"comment")
                        if comment:
                            field_info["comment"] = comment.decode("utf-8")
                    fields.append(field_info)

                return json.dumps(
                    {
                        "table": table,
                        "namespace": NAMESPACE,
                        "schema_version": SCHEMA_VERSION,
                        "fields": fields,
                    }
                )
            except ValueError as e:
                return json.dumps({"error": str(e)})

        elif name == "get_config":
            config = get_config()
            # Return config without sensitive credentials
            return json.dumps(
                {
                    "namespace": config.namespace,
                    "warehouse": config.warehouse,
                    "s3_endpoint": config.s3_endpoint,
                    "s3_region": config.s3_region,
                    # Don't expose access keys
                }
            )

        else:
            return json.dumps({"error": f"Unknown tool: {name}"})

    async def run(self) -> None:
        """Run the MCP server."""
        logger.info(f"Starting lakehouse-provider MCP server (namespace: {NAMESPACE})")

        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options(),
            )


async def run_server() -> None:
    """Run the MCP server (async entry point)."""
    server = LakehouseProviderServer()
    await server.run()


def main() -> None:
    """Main entry point for the MCP server."""
    import asyncio

    asyncio.run(run_server())


if __name__ == "__main__":
    main()
