"""Deduplication utilities for Spark DataFrames."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedup_by_key(
    df: DataFrame,
    key_columns: list[str],
    order_column: str,
    *,
    ascending: bool = False,
) -> DataFrame:
    """Deduplicate rows by composite key, keeping one row per key.

    Uses a window function to rank rows within each key partition,
    ordered by the specified column, and retains only the top-ranked row.

    Args:
        df: Input DataFrame with potential duplicates.
        key_columns: Columns forming the composite business key.
        order_column: Column to determine which duplicate to keep.
        ascending: If True, keep the earliest (min) row. Default keeps latest (max).

    Returns:
        DataFrame with exactly one row per unique key combination.
    """
    order_expr = F.col(order_column).asc() if ascending else F.col(order_column).desc()

    window = Window.partitionBy(key_columns).orderBy(order_expr)

    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def dedup_exact(df: DataFrame) -> DataFrame:
    """Remove exact duplicate rows (all columns identical).

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with duplicate rows removed.
    """
    return df.dropDuplicates()
