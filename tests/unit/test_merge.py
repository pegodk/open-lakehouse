"""Unit tests for lakehouse.lib.merge — Delta MERGE wrapper."""

import os
import shutil
import tempfile

from pyspark.sql import SparkSession

from lakehouse.lib.merge import upsert


class TestUpsert:
    """Tests for the generic upsert function."""

    def test_initial_load_creates_table(self, spark: SparkSession) -> None:
        """First upsert into non-existent path should create the table."""
        target_path = os.path.join(tempfile.mkdtemp(), "merge_init")
        try:
            source = spark.createDataFrame(
                [("1", "Alice", 100), ("2", "Bob", 200)],
                ["id", "name", "amount"],
            )

            upsert(spark, target_path, source, key_columns=["id"])

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 2
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)

    def test_updates_existing_rows(self, spark: SparkSession) -> None:
        """Matched rows should be updated."""
        target_path = os.path.join(tempfile.mkdtemp(), "merge_update")
        try:
            initial = spark.createDataFrame(
                [("1", "Alice", 100)],
                ["id", "name", "amount"],
            )
            initial.write.format("delta").mode("overwrite").save(target_path)

            update = spark.createDataFrame(
                [("1", "Alice", 999)],
                ["id", "name", "amount"],
            )

            upsert(spark, target_path, update, key_columns=["id"])

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 1
            assert result.collect()[0]["amount"] == 999
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)

    def test_inserts_new_rows(self, spark: SparkSession) -> None:
        """Unmatched rows should be inserted."""
        target_path = os.path.join(tempfile.mkdtemp(), "merge_insert")
        try:
            initial = spark.createDataFrame(
                [("1", "Alice", 100)],
                ["id", "name", "amount"],
            )
            initial.write.format("delta").mode("overwrite").save(target_path)

            new_data = spark.createDataFrame(
                [("2", "Bob", 200)],
                ["id", "name", "amount"],
            )

            upsert(spark, target_path, new_data, key_columns=["id"])

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 2
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)

    def test_selective_column_update(self, spark: SparkSession) -> None:
        """When update_columns specified, only those columns should be updated."""
        target_path = os.path.join(tempfile.mkdtemp(), "merge_selective")
        try:
            initial = spark.createDataFrame(
                [("1", "Alice", 100)],
                ["id", "name", "amount"],
            )
            initial.write.format("delta").mode("overwrite").save(target_path)

            update = spark.createDataFrame(
                [("1", "CHANGED", 999)],
                ["id", "name", "amount"],
            )

            upsert(
                spark, target_path, update, key_columns=["id"], update_columns=["amount"]
            )

            result = spark.read.format("delta").load(target_path)
            row = result.collect()[0]
            assert row["amount"] == 999
            # name should remain unchanged since it's not in update_columns
            assert row["name"] == "Alice"
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)
