"""StructType schema definitions for lakehouse tables.

These are the source of truth — enforced at the silver layer via lib/schema.py.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Orders ─────────────────────────────────────────────────────────────────

ORDERS_BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_date", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", StringType(), nullable=True),
        StructField("unit_price", StringType(), nullable=True),
        StructField("ingested_at", TimestampType(), nullable=False),
    ]
)

ORDERS_SILVER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("order_date", DateType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("total_amount", DoubleType(), nullable=False),
        StructField("ingested_at", TimestampType(), nullable=False),
        StructField("processed_at", TimestampType(), nullable=False),
    ]
)

# ─── Daily Sales (Gold) ─────────────────────────────────────────────────────

DAILY_SALES_SCHEMA = StructType(
    [
        StructField("sales_date", DateType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("total_quantity", IntegerType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=False),
        StructField("order_count", IntegerType(), nullable=False),
    ]
)
