"""Silver asset: cleansed, deduplicated, schema-enforced orders."""

from __future__ import annotations

import os

import dagster
from pyspark.sql import functions as F

from lakehouse.lib.dedup import dedup_by_key
from lakehouse.lib.schema import enforce_schema
from lakehouse.models.schemas import ORDERS_SILVER_SCHEMA
from lakehouse.resources.spark import SparkResource


@dagster.asset(
    group_name="silver",
    deps=["bronze_orders"],
    description="Cleansed, deduplicated, and schema-enforced orders.",
)
def silver_orders(spark: SparkResource) -> dagster.Output[None]:
    """Transform bronze orders into silver layer.

    Applies:
    1. Deduplication by order_id (keep latest ingested)
    2. Schema enforcement (cast types, validate columns)
    3. Computed columns (total_amount)
    4. Write as Delta table
    """
    session = spark.get_session()

    source_path = os.getenv("BRONZE_ORDERS_TARGET", "data/bronze/orders/")
    target_path = os.getenv("SILVER_ORDERS_TARGET", "data/silver/orders/")

    # Read from bronze
    bronze_df = session.read.format("delta").load(source_path)

    # Deduplicate: keep latest ingestion per order_id
    deduped = dedup_by_key(
        bronze_df,
        key_columns=["order_id"],
        order_column="ingested_at",
        ascending=False,
    )

    # Compute derived columns before schema enforcement
    enriched = deduped.withColumn(
        "total_amount",
        F.col("quantity").cast("double") * F.col("unit_price").cast("double"),
    ).withColumn("processed_at", F.current_timestamp())

    # Enforce silver schema (cast types, validate columns)
    conformed = enforce_schema(enriched, ORDERS_SILVER_SCHEMA)

    conformed.write.format("delta").mode("overwrite").save(target_path)

    return dagster.Output(
        None,
        metadata={
            "row_count": dagster.MetadataValue.int(conformed.count()),
            "target_path": dagster.MetadataValue.text(target_path),
        },
    )
