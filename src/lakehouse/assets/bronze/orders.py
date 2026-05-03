"""Bronze asset: ingest raw orders data into Delta Lake."""

from __future__ import annotations

import os

import dagster
from pyspark.sql import functions as F

from lakehouse.resources.spark import SparkResource


@dagster.asset(
    group_name="bronze",
    description="Ingest raw orders from source into bronze Delta table.",
)
def bronze_orders(spark: SparkResource) -> dagster.Output[None]:
    """Read raw order data (CSV/Parquet) and write to bronze Delta table.

    Bronze layer: minimal transformation, append-only, schema-on-read.
    Adds ingestion timestamp for lineage tracking.
    """
    session = spark.get_session()

    source_path = os.getenv("BRONZE_ORDERS_SOURCE", "data/raw/orders/")
    catalog_name = os.getenv("CATALOG_NAME", "unity")
    table_name = f"{catalog_name}.bronze.orders"

    df = session.read.option("header", "true").csv(source_path)

    # Add ingestion metadata
    df_with_metadata = df.withColumn("ingested_at", F.current_timestamp())

    # Write via Unity Catalog if configured, otherwise fall back to file path
    if os.getenv("UC_SERVER_URL"):
        df_with_metadata.write.format("delta").mode("append").saveAsTable(table_name)
    else:
        target_path = os.getenv("BRONZE_ORDERS_TARGET", "data/bronze/orders/")
        df_with_metadata.write.format("delta").mode("append").save(target_path)

    return dagster.Output(
        None,
        metadata={
            "row_count": dagster.MetadataValue.int(df_with_metadata.count()),
            "table": dagster.MetadataValue.text(table_name),
        },
    )
