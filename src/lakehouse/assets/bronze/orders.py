"""Bronze asset: ingest raw orders data into Delta Lake."""

from __future__ import annotations

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

    # Example: read from a source path (configurable via env)
    import os

    source_path = os.getenv("BRONZE_ORDERS_SOURCE", "data/raw/orders/")
    target_path = os.getenv("BRONZE_ORDERS_TARGET", "data/bronze/orders/")

    df = session.read.option("header", "true").csv(source_path)

    # Add ingestion metadata
    df_with_metadata = df.withColumn("ingested_at", F.current_timestamp())

    df_with_metadata.write.format("delta").mode("append").save(target_path)

    return dagster.Output(
        None,
        metadata={
            "row_count": dagster.MetadataValue.int(df_with_metadata.count()),
            "target_path": dagster.MetadataValue.text(target_path),
        },
    )
