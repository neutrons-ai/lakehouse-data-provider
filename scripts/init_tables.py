#!/usr/bin/env python3
"""
Initialize Iceberg Tables for Provider Data

This is a SELF-CONTAINED script that creates Iceberg tables for the
data provider. It has NO external dependencies beyond PySpark.

This script is designed to be run via the lakehouse CLI:
    lakehouse init -s init_tables.py -n template_provider

Or directly via spark-submit:
    spark-submit init_tables.py --catalog nessie --database template_provider

Tables created:
- records: Primary data records with attributes and metadata
- events: Time-series events associated with records
"""

import argparse
import sys

from pyspark.sql import SparkSession

# =============================================================================
# Table Schemas (as Spark DDL strings)
# =============================================================================

# Primary data table - customize for your domain
RECORDS_DDL = """
    id STRING NOT NULL COMMENT 'Unique record identifier',
    category STRING NOT NULL COMMENT 'Category (partition key)',
    created_at STRING COMMENT 'Creation timestamp (ISO format)',
    updated_at STRING COMMENT 'Last update timestamp (ISO format)',
    name STRING COMMENT 'Record name',
    description STRING COMMENT 'Record description',
    value DOUBLE COMMENT 'Numeric value',
    tags ARRAY<STRING> COMMENT 'List of tags',
    attributes STRUCT<
        key1: STRING,
        key2: BIGINT
    > COMMENT 'Additional attributes',
    source_file STRING COMMENT 'Original source filename',
    ingestion_time STRING COMMENT 'Ingestion timestamp (ISO format)'
"""

# Events table - for time-series data
EVENTS_DDL = """
    id STRING NOT NULL COMMENT 'Event ID',
    record_id STRING NOT NULL COMMENT 'Reference to primary record',
    category STRING NOT NULL COMMENT 'Category (partition key)',
    event_time STRING COMMENT 'Event timestamp (ISO format)',
    event_type STRING COMMENT 'Type of event',
    event_data STRING COMMENT 'Event payload (JSON)'
"""

# Table definitions: (name, ddl, description)
TABLES = [
    ("records", RECORDS_DDL, "Primary data records with attributes and metadata"),
    ("events", EVENTS_DDL, "Time-series events associated with records"),
]


def table_exists(spark, catalog: str, database: str, table: str) -> bool:
    """Check if a table exists."""
    full_table = f"{catalog}.{database}.{table}"
    try:
        spark.sql(f"DESCRIBE TABLE {full_table}")
        return True
    except Exception:
        return False


def create_table(
    spark, catalog: str, database: str, table_name: str, ddl: str, description: str
) -> bool:
    """Create an Iceberg table if it doesn't exist."""
    full_table = f"{catalog}.{database}.{table_name}"

    if table_exists(spark, catalog, database, table_name):
        print(f"  Table {full_table} already exists")
        return True

    sql = f"""
    CREATE TABLE {full_table} (
        {ddl}
    )
    USING iceberg
    PARTITIONED BY (category)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'comment' = '{description}'
    )
    """

    try:
        spark.sql(sql)
        print(f"  ✓ Created table: {full_table}")
        return True
    except Exception as e:
        print(f"  ✗ Failed to create {full_table}: {e}")
        return False


def create_all_tables(spark, catalog: str, database: str) -> list:
    """Create all provider tables."""
    full_namespace = f"{catalog}.{database}"

    # Create the namespace if it doesn't exist
    print(f"Creating namespace: {full_namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {full_namespace}")

    created_tables = []
    print(f"\nCreating tables in {full_namespace}:")

    for table_name, ddl, description in TABLES:
        if create_table(spark, catalog, database, table_name, ddl, description):
            created_tables.append(table_name)

    return created_tables


def main():
    parser = argparse.ArgumentParser(
        description="Initialize Iceberg tables for provider data"
    )
    parser.add_argument(
        "--catalog", default="nessie", help="Iceberg catalog name (default: nessie)"
    )
    parser.add_argument(
        "--database", default="template_provider", help="Database/namespace name (default: template_provider)"
    )
    args = parser.parse_args()

    print("Starting Spark session...")
    spark = SparkSession.builder.appName("ProviderTableInit").getOrCreate()

    try:
        print("\nInitializing provider tables...")
        print(f"  Catalog: {args.catalog}")
        print(f"  Database: {args.database}")

        created_tables = create_all_tables(spark, args.catalog, args.database)

        print(f"\n✓ Created {len(created_tables)} table(s): {', '.join(created_tables)}")
        print("\nTable initialization complete!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
