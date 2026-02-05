"""
Tests for MCP server and tools.
"""

import json

import pytest

from lakehouse_provider.mcp.tools import TOOLS, get_all_tools, get_tool_by_name


class TestToolDefinitions:
    """Test MCP tool definitions."""

    def test_tools_list_not_empty(self):
        """TOOLS list should contain tool definitions."""
        assert len(TOOLS) > 0

    def test_all_tools_have_required_fields(self):
        """All tools should have name, description, and inputSchema."""
        for tool in TOOLS:
            assert "name" in tool, f"Tool missing 'name': {tool}"
            assert "description" in tool, f"Tool {tool.get('name')} missing 'description'"
            assert "inputSchema" in tool, f"Tool {tool.get('name')} missing 'inputSchema'"

    def test_tool_names_are_unique(self):
        """Tool names should be unique."""
        names = [tool["name"] for tool in TOOLS]
        assert len(names) == len(set(names)), "Duplicate tool names found"

    def test_input_schemas_are_valid(self):
        """Input schemas should be valid JSON Schema objects."""
        for tool in TOOLS:
            schema = tool["inputSchema"]
            assert isinstance(schema, dict)
            assert schema.get("type") == "object"
            assert "properties" in schema

    def test_required_tools_exist(self):
        """Required tools should be defined."""
        required = ["query", "get_record", "search", "list_tables", "get_schema"]
        tool_names = [tool["name"] for tool in TOOLS]
        for name in required:
            assert name in tool_names, f"Required tool '{name}' not found"


class TestToolHelpers:
    """Test tool helper functions."""

    def test_get_tool_by_name_found(self):
        """get_tool_by_name should return tool when found."""
        tool = get_tool_by_name("query")
        assert tool is not None
        assert tool["name"] == "query"

    def test_get_tool_by_name_not_found(self):
        """get_tool_by_name should return None when not found."""
        tool = get_tool_by_name("nonexistent_tool")
        assert tool is None

    def test_get_all_tools(self):
        """get_all_tools should return all tools."""
        tools = get_all_tools()
        assert tools == TOOLS


class TestQueryTool:
    """Test the query tool definition."""

    def test_query_tool_schema(self):
        """Query tool should have correct schema."""
        tool = get_tool_by_name("query")
        assert tool is not None

        schema = tool["inputSchema"]
        assert "sql" in schema["properties"]
        assert "sql" in schema.get("required", [])


class TestGetRecordTool:
    """Test the get_record tool definition."""

    def test_get_record_schema(self):
        """get_record tool should require table and id."""
        tool = get_tool_by_name("get_record")
        assert tool is not None

        schema = tool["inputSchema"]
        assert "table" in schema["properties"]
        assert "id" in schema["properties"]
        assert set(schema.get("required", [])) == {"table", "id"}


class TestSearchTool:
    """Test the search tool definition."""

    def test_search_schema(self):
        """search tool should have table, filters, and limit."""
        tool = get_tool_by_name("search")
        assert tool is not None

        schema = tool["inputSchema"]
        assert "table" in schema["properties"]
        assert "filters" in schema["properties"]
        assert "limit" in schema["properties"]
        assert "table" in schema.get("required", [])


class TestListTablesTool:
    """Test the list_tables tool definition."""

    def test_list_tables_schema(self):
        """list_tables tool should have no required parameters."""
        tool = get_tool_by_name("list_tables")
        assert tool is not None

        schema = tool["inputSchema"]
        assert schema.get("required", []) == [] or "required" not in schema


class TestGetSchemaTool:
    """Test the get_schema tool definition."""

    def test_get_schema_tool(self):
        """get_schema tool should require table name."""
        tool = get_tool_by_name("get_schema")
        assert tool is not None

        schema = tool["inputSchema"]
        assert "table" in schema["properties"]
        assert "table" in schema.get("required", [])


# Integration tests for the server (require mcp package)
class TestServerImport:
    """Test that server module can be imported."""

    def test_import_server(self):
        """Should be able to import server module."""
        try:
            from lakehouse_provider.mcp import LakehouseProviderServer, main

            assert LakehouseProviderServer is not None
            assert main is not None
        except ImportError as e:
            pytest.skip(f"MCP package not installed: {e}")

    def test_server_instantiation(self):
        """Should be able to create server instance."""
        try:
            from lakehouse_provider.mcp import LakehouseProviderServer

            server = LakehouseProviderServer()
            assert server is not None
            assert server.server is not None
        except ImportError as e:
            pytest.skip(f"MCP package not installed: {e}")
