"""Generic Delta Lake MERGE (upsert) wrapper."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def upsert(
    spark: SparkSession,
    target_path: str,
    source_df: DataFrame,
    key_columns: list[str],
    *,
    update_columns: list[str] | None = None,
    insert_columns: list[str] | None = None,
    delete_condition: str | None = None,
    target_alias: str = "target",
    source_alias: str = "source",
) -> None:
    """Perform an upsert (MERGE) into a Delta table.

    Matches on key_columns. Updates matched rows, inserts unmatched rows.
    Optionally deletes matched rows meeting a condition.

    Args:
        spark: Active SparkSession.
        target_path: Path to the target Delta table.
        source_df: DataFrame with new/updated data.
        key_columns: Columns to match source and target rows.
        update_columns: Columns to update on match. None = update all.
        insert_columns: Columns to insert for new rows. None = insert all.
        delete_condition: SQL expression; matched rows meeting this are deleted.
        target_alias: Alias for target in merge expressions.
        source_alias: Alias for source in merge expressions.
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return

    target = DeltaTable.forPath(spark, target_path)

    merge_condition = " AND ".join(
        f"{target_alias}.{col} = {source_alias}.{col}" for col in key_columns
    )

    merge_builder = target.alias(target_alias).merge(
        source_df.alias(source_alias), merge_condition
    )

    # Handle deletes first (if specified)
    if delete_condition:
        merge_builder = merge_builder.whenMatchedDelete(condition=delete_condition)

    # Handle updates
    if update_columns:
        update_set = {col: f"{source_alias}.{col}" for col in update_columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    # Handle inserts
    if insert_columns:
        insert_set = {col: f"{source_alias}.{col}" for col in insert_columns}
        merge_builder = merge_builder.whenNotMatchedInsert(values=insert_set)
    else:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()
