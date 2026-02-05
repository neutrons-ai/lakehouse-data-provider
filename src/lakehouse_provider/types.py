"""
Type definitions for the lakehouse provider.

This module defines data models used throughout the package.
Customize these types for your specific data domain.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class Record:
    """
    Base record type for provider data.

    Customize this class for your specific data model.
    """

    id: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Record":
        """Create a Record from a dictionary."""
        return cls(
            id=data.get("id", ""),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            metadata=data.get("metadata", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert the Record to a dictionary."""
        return {
            "id": self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata,
        }


@dataclass
class QueryResult:
    """Result of a query operation."""

    records: list[Record]
    total_count: int
    has_more: bool = False

    @property
    def is_empty(self) -> bool:
        """Check if the result is empty."""
        return len(self.records) == 0


@dataclass
class TableInfo:
    """Information about a table in the lakehouse."""

    name: str
    namespace: str
    description: Optional[str] = None
    row_count: Optional[int] = None

    @property
    def full_name(self) -> str:
        """Get the fully qualified table name."""
        return f"{self.namespace}.{self.name}"
