"""Gold asset: per-product revenue summary computed with DuckDB.

DuckDB reads the silver Delta table directly — no JVM, no SparkSession.
Results are written back as a Delta table using delta-rs (``deltalake``).

This demonstrates the multi-engine pattern at the heart of the stack:
  - Unity Catalog  — single metadata/governance layer for all engines
  - Delta Lake     — open storage format readable by Spark, DuckDB, and others
  - Apache Spark   — heavy, distributed transformations (silver layer, ML)
  - DuckDB         — fast, in-process SQL for smaller aggregations (this asset)
"""

from __future__ import annotations

import os

import dagster
import duckdb
from deltalake import write_deltalake


@dagster.asset(
    group_name="gold",
    deps=["silver_orders"],
    description=(
        "Per-product revenue summary computed with DuckDB. "
        "No Spark or JVM required — reads Delta files directly via delta-rs."
    ),
)
def gold_product_summary() -> dagster.Output[None]:
    """Aggregate silver orders into a per-product revenue summary using DuckDB.

    Reads the silver Delta table with DuckDB's native ``delta`` extension,
    runs an in-process aggregation, then writes the result as a new Delta table
    using ``write_deltalake`` from delta-rs.

    Suitable for datasets that fit comfortably in local memory (up to ~tens of
    GBs).  For larger volumes, prefer the Spark-based ``gold_daily_sales`` asset.
    """
    silver_path = os.getenv("SILVER_ORDERS_TARGET", "data/silver/orders/")
    target_path = os.getenv("GOLD_PRODUCT_SUMMARY_TARGET", "data/gold/product_summary/")

    con = duckdb.connect()
    con.install_extension("delta")
    con.load_extension("delta")

    # Read the silver Delta table — DuckDB resolves the Delta log automatically.
    # No SparkSession or JVM is started at any point.
    arrow_result = con.execute(
        f"""
        SELECT
            product_id,
            COUNT(order_id)   AS order_count,
            SUM(quantity)     AS total_quantity,
            SUM(total_amount) AS total_revenue,
            MIN(order_date)   AS first_order_date,
            MAX(order_date)   AS last_order_date
        FROM delta_scan('{silver_path}')
        GROUP BY product_id
        ORDER BY total_revenue DESC
        """
    ).arrow()

    # Write the Arrow result as a Delta table.
    # delta-rs handles Delta log creation/update without Spark.
    write_deltalake(target_path, arrow_result, mode="overwrite")

    row_count = arrow_result.num_rows

    return dagster.Output(
        None,
        metadata={
            "row_count": dagster.MetadataValue.int(row_count),
            "engine": dagster.MetadataValue.text("DuckDB + delta-rs"),
            "silver_source": dagster.MetadataValue.text(silver_path),
            "target_path": dagster.MetadataValue.text(target_path),
        },
    )
