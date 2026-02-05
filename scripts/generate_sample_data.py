#!/usr/bin/env python3
"""
Generate Sample Data for Template Provider

This script generates sample Parquet files with data that matches
the schema defined in init_tables.py. The Parquet files can then be
ingested into the lakehouse using the `lakehouse ingest` command.

Usage:
    python scripts/generate_sample_data.py --output data/
    lakehouse ingest data/ --namespace template_provider --use-driver
"""

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def generate_records_data(num_records: int = 100) -> pa.Table:
    """
    Generate sample records data.

    Matches the schema in init_tables.py for the 'records' table.
    """
    now = datetime.now()
    categories = ["A", "B", "C", "D"]

    records = []
    for i in range(num_records):
        record_id = f"rec-{i:04d}"
        category = categories[i % len(categories)]
        created_at = (now - timedelta(days=num_records - i)).isoformat()

        record = {
            "id": record_id,
            "category": category,
            "created_at": created_at,
            "updated_at": created_at,
            "name": f"Sample Record {i}",
            "description": f"This is sample record number {i} for category {category}",
            "value": float(i * 3.14),
            "tags": [f"tag-{i % 5}", f"category-{category.lower()}"],
            "attributes": {
                "key1": f"value-{i}",
                "key2": i * 100,
            },
            "source_file": f"sample_batch_{i // 10}.csv",
            "ingestion_time": now.isoformat(),
        }
        records.append(record)

    # Convert to PyArrow Table
    # Need to match the exact schema from init_tables.py
    schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("created_at", pa.string()),
            pa.field("updated_at", pa.string()),
            pa.field("name", pa.string()),
            pa.field("description", pa.string()),
            pa.field("value", pa.float64()),
            pa.field("tags", pa.list_(pa.string())),
            pa.field(
                "attributes",
                pa.struct(
                    [
                        pa.field("key1", pa.string()),
                        pa.field("key2", pa.int64()),
                    ]
                ),
            ),
            pa.field("source_file", pa.string()),
            pa.field("ingestion_time", pa.string()),
        ]
    )

    return pa.Table.from_pylist(records, schema=schema)


def generate_events_data(num_events: int = 200) -> pa.Table:
    """
    Generate sample events data.

    Matches the schema in init_tables.py for the 'events' table.
    """
    now = datetime.now()
    categories = ["A", "B", "C", "D"]
    event_types = ["created", "updated", "processed", "archived"]

    events = []
    for i in range(num_events):
        # Link events to records (assuming 100 records)
        record_id = f"rec-{i % 100:04d}"
        category = categories[i % len(categories)]
        event_time = (now - timedelta(hours=num_events - i)).isoformat()
        event_type = event_types[i % len(event_types)]

        event_data = {
            "user": f"user-{i % 10}",
            "status": "success" if i % 7 != 0 else "failed",
            "duration_ms": i * 10 + 50,
        }

        event = {
            "id": f"evt-{i:05d}",
            "record_id": record_id,
            "category": category,
            "event_time": event_time,
            "event_type": event_type,
            "event_data": json.dumps(event_data),
        }
        events.append(event)

    # Convert to PyArrow Table
    schema = pa.schema(
        [
            pa.field("id", pa.string(), nullable=False),
            pa.field("record_id", pa.string(), nullable=False),
            pa.field("category", pa.string(), nullable=False),
            pa.field("event_time", pa.string()),
            pa.field("event_type", pa.string()),
            pa.field("event_data", pa.string()),
        ]
    )

    return pa.Table.from_pylist(events, schema=schema)


def write_parquet(table: pa.Table, output_path: Path, table_name: str) -> None:
    """
    Write PyArrow table to Parquet file.

    Args:
        table: PyArrow table to write
        output_path: Output directory
        table_name: Name of the table (used for filename)
    """
    output_path.mkdir(parents=True, exist_ok=True)

    # Write with compression
    filename = output_path / f"{table_name}.parquet"
    pq.write_table(
        table,
        filename,
        compression="zstd",
        use_dictionary=True,
    )

    print(f"  ✓ Wrote {len(table)} rows to {filename}")


def main():
    parser = argparse.ArgumentParser(description="Generate sample data for the template provider")
    parser.add_argument(
        "--output",
        "-o",
        default="data",
        help="Output directory for Parquet files (default: data/)",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=100,
        help="Number of records to generate (default: 100)",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=200,
        help="Number of events to generate (default: 200)",
    )
    args = parser.parse_args()

    output_path = Path(args.output)

    print(f"Generating sample data...")
    print(f"  Output directory: {output_path}")
    print(f"  Records: {args.records}")
    print(f"  Events: {args.events}")
    print()

    # Generate and write records
    print("Generating records table...")
    records_table = generate_records_data(args.records)
    write_parquet(records_table, output_path, "records")

    # Generate and write events
    print("\nGenerating events table...")
    events_table = generate_events_data(args.events)
    write_parquet(events_table, output_path, "events")

    print(f"\n✓ Sample data generation complete!")
    print(f"\nTo ingest this data into the lakehouse:")
    print(f"  cd ~/git/neutron-lakehouse")
    print(f"  lakehouse ingest ~/git/lakehouse-data-provider/{output_path} \\")
    print(f"    --namespace template_provider --use-driver")


if __name__ == "__main__":
    main()
