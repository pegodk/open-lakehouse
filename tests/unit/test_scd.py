"""Unit tests for lakehouse.lib.scd — Slowly Changing Dimensions."""

import os
import shutil
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.lib.scd import scd_type1, scd_type2


class TestSCDType1:
    """Tests for SCD Type 1 (overwrite)."""

    def test_initial_load_creates_table(self, spark: SparkSession) -> None:
        """First load should create the Delta table from source."""
        target_path = os.path.join(tempfile.mkdtemp(), "scd1_test")
        try:
            source = spark.createDataFrame(
                [("1", "Alice", "alice@example.com"), ("2", "Bob", "bob@example.com")],
                ["customer_id", "name", "email"],
            )

            scd_type1(spark, target_path, source, key_columns=["customer_id"])

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 2
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)

    def test_update_overwrites_existing(self, spark: SparkSession) -> None:
        """Existing rows should be overwritten with new values."""
        target_path = os.path.join(tempfile.mkdtemp(), "scd1_update")
        try:
            initial = spark.createDataFrame(
                [("1", "Alice", "old@example.com")],
                ["customer_id", "name", "email"],
            )
            initial.write.format("delta").mode("overwrite").save(target_path)

            update = spark.createDataFrame(
                [("1", "Alice", "new@example.com")],
                ["customer_id", "name", "email"],
            )

            scd_type1(spark, target_path, update, key_columns=["customer_id"])

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 1
            row = result.collect()[0]
            assert row["email"] == "new@example.com"
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)

    def test_insert_new_rows(self, spark: SparkSession) -> None:
        """New rows (not matching key) should be inserted."""
        target_path = os.path.join(tempfile.mkdtemp(), "scd1_insert")
        try:
            initial = spark.createDataFrame(
                [("1", "Alice", "alice@example.com")],
                ["customer_id", "name", "email"],
            )
            initial.write.format("delta").mode("overwrite").save(target_path)

            new_data = spark.createDataFrame(
                [("2", "Bob", "bob@example.com")],
                ["customer_id", "name", "email"],
            )

            scd_type1(spark, target_path, new_data, key_columns=["customer_id"])

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 2
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)


class TestSCDType2:
    """Tests for SCD Type 2 (history tracking)."""

    def test_initial_load(self, spark: SparkSession) -> None:
        """First load should create all rows as current."""
        target_path = os.path.join(tempfile.mkdtemp(), "scd2_init")
        try:
            source = spark.createDataFrame(
                [("1", "Alice", "100 Main St"), ("2", "Bob", "200 Oak Ave")],
                ["customer_id", "name", "address"],
            )

            scd_type2(
                spark,
                target_path,
                source,
                key_columns=["customer_id"],
                tracked_columns=["address"],
            )

            result = spark.read.format("delta").load(target_path)
            assert result.count() == 2
            assert result.filter(F.col("is_current") == True).count() == 2  # noqa: E712
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)

    def test_change_creates_new_version(self, spark: SparkSession) -> None:
        """Changing a tracked column should close old record and create new one."""
        target_path = os.path.join(tempfile.mkdtemp(), "scd2_change")
        try:
            initial = spark.createDataFrame(
                [("1", "Alice", "100 Main St")],
                ["customer_id", "name", "address"],
            )

            scd_type2(
                spark,
                target_path,
                initial,
                key_columns=["customer_id"],
                tracked_columns=["address"],
            )

            updated = spark.createDataFrame(
                [("1", "Alice", "300 New Blvd")],
                ["customer_id", "name", "address"],
            )

            scd_type2(
                spark,
                target_path,
                updated,
                key_columns=["customer_id"],
                tracked_columns=["address"],
            )

            result = spark.read.format("delta").load(target_path)
            # Should have 2 rows: old (closed) + new (current)
            assert result.count() == 2
            assert result.filter(F.col("is_current") == True).count() == 1  # noqa: E712
            assert result.filter(F.col("is_current") == False).count() == 1  # noqa: E712

            current = result.filter(F.col("is_current") == True).collect()[0]  # noqa: E712
            assert current["address"] == "300 New Blvd"
        finally:
            shutil.rmtree(os.path.dirname(target_path), ignore_errors=True)
