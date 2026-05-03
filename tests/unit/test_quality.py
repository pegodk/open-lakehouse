"""Unit tests for lakehouse.lib.quality — Data quality checks."""

from pyspark.sql import SparkSession

from lakehouse.lib.quality import (
    check_not_null,
    check_range,
    check_referential_integrity,
    check_unique,
)


class TestCheckNotNull:
    """Tests for null value checks."""

    def test_passes_when_no_nulls(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("1", "Alice"), ("2", "Bob")], ["id", "name"])
        result = check_not_null(df, ["id", "name"])
        assert result.passed is True
        assert result.failed_rows == 0

    def test_fails_when_nulls_present(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("1", "Alice"), ("2", None)], ["id", "name"])
        result = check_not_null(df, ["name"])
        assert result.passed is False
        assert result.failed_rows == 1

    def test_checks_multiple_columns(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(None, "Alice"), ("2", None)], ["id", "name"])
        result = check_not_null(df, ["id", "name"])
        assert result.passed is False
        assert result.failed_rows == 2


class TestCheckUnique:
    """Tests for uniqueness checks."""

    def test_passes_when_unique(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("1", "A"), ("2", "B")], ["id", "cat"])
        result = check_unique(df, ["id"])
        assert result.passed is True

    def test_fails_when_duplicates(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([("1", "A"), ("1", "B"), ("2", "C")], ["id", "val"])
        result = check_unique(df, ["id"])
        assert result.passed is False
        assert result.failed_rows == 1  # 3 total - 2 distinct = 1 duplicate

    def test_composite_uniqueness(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [("1", "A", "x"), ("1", "B", "y"), ("1", "A", "z")],
            ["id", "cat", "val"],
        )
        result = check_unique(df, ["id", "cat"])
        assert result.passed is False
        assert result.failed_rows == 1


class TestCheckRange:
    """Tests for numeric range checks."""

    def test_passes_within_range(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, 50), (2, 75), (3, 100)], ["id", "score"])
        result = check_range(df, "score", min_value=0, max_value=100)
        assert result.passed is True

    def test_fails_below_minimum(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, -5), (2, 50)], ["id", "score"])
        result = check_range(df, "score", min_value=0)
        assert result.passed is False
        assert result.failed_rows == 1

    def test_fails_above_maximum(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1, 50), (2, 150)], ["id", "score"])
        result = check_range(df, "score", max_value=100)
        assert result.passed is False
        assert result.failed_rows == 1


class TestCheckReferentialIntegrity:
    """Tests for referential integrity checks."""

    def test_passes_all_references_valid(self, spark: SparkSession) -> None:
        fact = spark.createDataFrame([("1", "P1"), ("2", "P2")], ["id", "product_id"])
        dim = spark.createDataFrame([("P1",), ("P2",), ("P3",)], ["product_id"])
        result = check_referential_integrity(fact, "product_id", dim, "product_id")
        assert result.passed is True

    def test_fails_orphan_references(self, spark: SparkSession) -> None:
        fact = spark.createDataFrame(
            [("1", "P1"), ("2", "P_MISSING")], ["id", "product_id"]
        )
        dim = spark.createDataFrame([("P1",), ("P2",)], ["product_id"])
        result = check_referential_integrity(fact, "product_id", dim, "product_id")
        assert result.passed is False
        assert result.failed_rows == 1
