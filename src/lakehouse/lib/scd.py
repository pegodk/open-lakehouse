"""Slowly Changing Dimension implementations using Delta Lake MERGE."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from delta.tables import DeltaTable


def scd_type1(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    key_columns: list[str],
    *,
    target_alias: str = "target",
    source_alias: str = "source",
) -> None:
    """Apply SCD Type 1 (overwrite) using Delta MERGE.

    Updates existing rows with new values. Inserts rows that don't exist.
    No history is preserved — the current state is simply overwritten.

    Args:
        spark: Active SparkSession.
        target_path: Path to the Delta table (or table name if using catalog).
        source_df: DataFrame containing the new/updated records.
        key_columns: Columns that uniquely identify a record.
        target_alias: Alias for the target table in the merge condition.
        source_alias: Alias for the source DataFrame in the merge condition.
    """
    merge_condition = " AND ".join(
        f"{target_alias}.{col} = {source_alias}.{col}" for col in key_columns
    )

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias(target_alias)
            .merge(source_df.alias(source_alias), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        source_df.write.format("delta").mode("overwrite").save(target_path)


def scd_type2(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    key_columns: list[str],
    tracked_columns: list[str],
    *,
    effective_date_col: str = "effective_date",
    end_date_col: str = "end_date",
    is_current_col: str = "is_current",
    target_alias: str = "target",
    source_alias: str = "source",
) -> None:
    """Apply SCD Type 2 (historical tracking) using Delta MERGE.

    When tracked columns change:
    - Closes the existing record (sets end_date, is_current=False)
    - Inserts a new record with the updated values (is_current=True)

    When no change is detected, the row is left untouched.
    New rows (not in target) are inserted as current.

    Args:
        spark: Active SparkSession.
        target_path: Path to the Delta table.
        source_df: DataFrame containing new/updated records.
        key_columns: Business key columns identifying a logical entity.
        tracked_columns: Columns whose changes trigger a new version.
        effective_date_col: Column name for the row's start date.
        end_date_col: Column name for the row's end date (None = current).
        is_current_col: Column name for the boolean current flag.
        target_alias: Alias for the target table.
        source_alias: Alias for the source DataFrame.
    """
    # Prepare source with SCD2 metadata
    staged = (
        source_df.withColumn(effective_date_col, F.current_date())
        .withColumn(end_date_col, F.lit(None).cast("date"))
        .withColumn(is_current_col, F.lit(True))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        staged.write.format("delta").mode("overwrite").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)

    merge_condition = " AND ".join(
        f"{target_alias}.{col} = {source_alias}.{col}" for col in key_columns
    )
    # Only match current records
    merge_condition += f" AND {target_alias}.{is_current_col} = true"

    # Detect changes in tracked columns
    change_condition = " OR ".join(
        f"{target_alias}.{col} != {source_alias}.{col}" for col in tracked_columns
    )

    (
        target.alias(target_alias)
        .merge(staged.alias(source_alias), merge_condition)
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                end_date_col: F.current_date(),
                is_current_col: F.lit(False),
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Insert new current rows for the changed records
    # Re-read target to find closed records from this batch
    closed_keys = (
        DeltaTable.forPath(spark, target_path)
        .toDF()
        .filter(
            (F.col(is_current_col) == F.lit(False))
            & (F.col(end_date_col) == F.current_date())
        )
        .select(key_columns)
    )

    new_current_rows = staged.join(closed_keys, on=key_columns, how="inner")
    new_current_rows.write.format("delta").mode("append").save(target_path)
