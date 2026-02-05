"""
Tests for the CLI.
"""

import json
import subprocess
import sys

import pytest


class TestCLI:
    """Test CLI commands."""

    def test_cli_help(self):
        """Should show help message."""
        result = subprocess.run(
            ["lakehouse-provider", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Lakehouse Provider CLI" in result.stdout
        assert "list-tables" in result.stdout

    def test_list_tables(self):
        """Should list tables."""
        result = subprocess.run(
            ["lakehouse-provider", "list-tables"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "template_provider" in result.stdout
        assert "records" in result.stdout
        assert "events" in result.stdout

    def test_list_tables_json(self):
        """Should list tables in JSON format."""
        result = subprocess.run(
            ["lakehouse-provider", "--json", "list-tables"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        data = json.loads(result.stdout)
        assert data["namespace"] == "template_provider"
        assert len(data["tables"]) == 2

    def test_get_schema(self):
        """Should show table schema."""
        result = subprocess.run(
            ["lakehouse-provider", "get-schema", "records"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "template_provider.records" in result.stdout
        assert "id: string (NOT NULL)" in result.stdout
        assert "category: string (NOT NULL)" in result.stdout

    def test_get_schema_unknown_table(self):
        """Should error on unknown table."""
        result = subprocess.run(
            ["lakehouse-provider", "get-schema", "nonexistent"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 1
        assert "Error" in result.stderr

    def test_config(self):
        """Should show configuration."""
        result = subprocess.run(
            ["lakehouse-provider", "config"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "Lakehouse Configuration" in result.stdout
        assert "Namespace:" in result.stdout
        assert "template_provider" in result.stdout


class TestCLICommands:
    """Test CLI command structure."""

    def test_search_help(self):
        """Search command should have help."""
        result = subprocess.run(
            ["lakehouse-provider", "search", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--filter" in result.stdout
        assert "--limit" in result.stdout

    def test_count_help(self):
        """Count command should have help."""
        result = subprocess.run(
            ["lakehouse-provider", "count", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "table" in result.stdout
        assert "--filter" in result.stdout

    def test_query_help(self):
        """Query command should have help."""
        result = subprocess.run(
            ["lakehouse-provider", "query", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "sql" in result.stdout
