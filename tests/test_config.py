"""
Tests for configuration module.
"""

import os

import pytest

from lakehouse_provider.config import LakehouseConfig, get_config, set_config


class TestLakehouseConfig:
    """Test LakehouseConfig class."""

    def test_create_config(self):
        """Should create config with all fields."""
        config = LakehouseConfig(
            s3_endpoint="http://localhost:9000",
            s3_access_key="access",
            s3_secret_key="secret",
            s3_region="us-west-2",
            warehouse="s3a://bucket/warehouse",
            namespace="my_namespace",
        )

        assert config.s3_endpoint == "http://localhost:9000"
        assert config.s3_access_key == "access"
        assert config.s3_secret_key == "secret"
        assert config.s3_region == "us-west-2"
        assert config.warehouse == "s3a://bucket/warehouse"
        assert config.namespace == "my_namespace"

    def test_default_values(self):
        """Should use default values for optional fields."""
        config = LakehouseConfig(
            s3_endpoint="http://localhost:9000",
            s3_access_key="access",
            s3_secret_key="secret",
        )

        assert config.s3_region == "us-east-1"
        assert config.warehouse == "s3a://lakehouse/warehouse"
        assert config.namespace == "template_provider"


class TestConfigFromEnv:
    """Test loading config from environment."""

    def test_from_env_with_all_vars(self, monkeypatch):
        """Should load all values from environment."""
        monkeypatch.setenv("LAKEHOUSE_S3_ENDPOINT", "http://minio:9000")
        monkeypatch.setenv("LAKEHOUSE_S3_ACCESS_KEY", "env_access")
        monkeypatch.setenv("LAKEHOUSE_S3_SECRET_KEY", "env_secret")
        monkeypatch.setenv("LAKEHOUSE_S3_REGION", "eu-west-1")
        monkeypatch.setenv("LAKEHOUSE_WAREHOUSE", "s3a://data/wh")
        monkeypatch.setenv("LAKEHOUSE_NAMESPACE", "env_namespace")

        config = LakehouseConfig.from_env()

        assert config.s3_endpoint == "http://minio:9000"
        assert config.s3_access_key == "env_access"
        assert config.s3_secret_key == "env_secret"
        assert config.s3_region == "eu-west-1"
        assert config.warehouse == "s3a://data/wh"
        assert config.namespace == "env_namespace"

    def test_from_env_defaults(self, monkeypatch):
        """Should use defaults when env vars not set."""
        # Clear any existing env vars
        for var in [
            "LAKEHOUSE_S3_ENDPOINT",
            "LAKEHOUSE_S3_ACCESS_KEY",
            "LAKEHOUSE_S3_SECRET_KEY",
            "LAKEHOUSE_S3_REGION",
            "LAKEHOUSE_WAREHOUSE",
            "LAKEHOUSE_NAMESPACE",
        ]:
            monkeypatch.delenv(var, raising=False)

        config = LakehouseConfig.from_env()

        assert config.s3_endpoint == "http://localhost:9000"
        assert config.s3_access_key == "admin"
        assert config.s3_secret_key == "password"
        assert config.s3_region == "us-east-1"


class TestConfigHelpers:
    """Test config helper properties."""

    def test_s3_endpoint_host_http(self):
        """Should strip http:// from endpoint."""
        config = LakehouseConfig(
            s3_endpoint="http://localhost:9000",
            s3_access_key="a",
            s3_secret_key="s",
        )
        assert config.s3_endpoint_host == "localhost:9000"

    def test_s3_endpoint_host_https(self):
        """Should strip https:// from endpoint."""
        config = LakehouseConfig(
            s3_endpoint="https://s3.amazonaws.com",
            s3_access_key="a",
            s3_secret_key="s",
        )
        assert config.s3_endpoint_host == "s3.amazonaws.com"

    def test_warehouse_s3_path(self):
        """Should convert s3a:// to s3://."""
        config = LakehouseConfig(
            s3_endpoint="http://localhost:9000",
            s3_access_key="a",
            s3_secret_key="s",
            warehouse="s3a://mybucket/warehouse",
        )
        assert config.warehouse_s3_path == "s3://mybucket/warehouse"


class TestGlobalConfig:
    """Test global config functions."""

    def test_get_set_config(self, monkeypatch):
        """Should get and set global config."""
        # Clear cached config
        from lakehouse_provider import config as config_module

        config_module._config = None

        custom_config = LakehouseConfig(
            s3_endpoint="http://custom:9000",
            s3_access_key="custom_access",
            s3_secret_key="custom_secret",
        )

        set_config(custom_config)
        result = get_config()

        assert result == custom_config
        assert result.s3_endpoint == "http://custom:9000"
