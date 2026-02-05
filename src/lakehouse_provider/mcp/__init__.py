"""
MCP Server for the lakehouse provider.

This module provides an MCP (Model Context Protocol) server that
exposes the provider's data to AI assistants like Claude.

Usage:
    # Run the MCP server
    python -m lakehouse_provider.mcp

    # Or via the installed script
    lakehouse-provider-mcp
"""

from lakehouse_provider.mcp.server import (
    LakehouseProviderServer,
    main,
    run_server,
)
from lakehouse_provider.mcp.tools import TOOLS, get_all_tools, get_tool_by_name

__all__ = [
    # Server
    "LakehouseProviderServer",
    "run_server",
    "main",
    # Tools
    "TOOLS",
    "get_all_tools",
    "get_tool_by_name",
]
