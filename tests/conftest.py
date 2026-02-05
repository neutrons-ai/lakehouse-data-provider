"""
Pytest configuration and fixtures for lakehouse_provider tests.
"""

import pytest

from lakehouse_provider.config import LakehouseConfig


@pytest.fixture
def mock_config() -> LakehouseConfig:
    """Create a mock configuration for testing."""
    return LakehouseConfig(
        s3_endpoint="http://localhost:9000",
        s3_access_key="test_access_key",
        s3_secret_key="test_secret_key",
        s3_region="us-east-1",
        warehouse="s3a://test-bucket/warehouse",
        namespace="test_provider",
    )


@pytest.fixture
def sample_record_data() -> dict:
    """Sample record data for testing."""
    return {
        "id": "test-123",
        "category": "test",
        "created_at": "2026-02-05T10:00:00Z",
        "updated_at": "2026-02-05T10:00:00Z",
        "name": "Test Record",
        "description": "A test record for unit tests",
        "value": 42.0,
        "tags": ["test", "sample"],
        "attributes": {"key1": "value1", "key2": 123},
        "source_file": "test.json",
        "ingestion_time": "2026-02-05T10:00:00Z",
    }
