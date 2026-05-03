"""Gold asset: daily sales aggregation."""

from __future__ import annotations

import os

import dagster
from pyspark.sql import functions as F

from lakehouse.resources.spark import SparkResource


@dagster.asset(
    group_name="gold",
    deps=["silver_orders"],
    description="Daily sales aggregation by product.",
)
def gold_daily_sales(spark: SparkResource) -> dagster.Output[None]:
    """Aggregate silver orders into daily sales summary per product.

    Computes: total_quantity, total_revenue, order_count per (date, product).
    """
    session = spark.get_session()

    source_path = os.getenv("SILVER_ORDERS_TARGET", "data/silver/orders/")
    target_path = os.getenv("GOLD_DAILY_SALES_TARGET", "data/gold/daily_sales/")

    silver_df = session.read.format("delta").load(source_path)

    daily_sales = silver_df.groupBy(
        F.col("order_date").alias("sales_date"),
        "product_id",
    ).agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
    )

    daily_sales.write.format("delta").mode("overwrite").save(target_path)

    return dagster.Output(
        None,
        metadata={
            "row_count": dagster.MetadataValue.int(daily_sales.count()),
            "target_path": dagster.MetadataValue.text(target_path),
        },
    )
