"""Unit tests for lakehouse.lib.dedup — Deduplication utilities."""

from pyspark.sql import SparkSession

from lakehouse.lib.dedup import dedup_by_key, dedup_exact


class TestDedupByKey:
    """Tests for dedup_by_key."""

    def test_keeps_latest_by_default(self, spark: SparkSession) -> None:
        """Should keep the row with the highest order_column value."""
        df = spark.createDataFrame(
            [
                ("1", "2024-01-01", "v1"),
                ("1", "2024-01-05", "v2"),
                ("1", "2024-01-03", "v3"),
                ("2", "2024-01-01", "v1"),
            ],
            ["id", "updated_at", "value"],
        )

        result = dedup_by_key(df, key_columns=["id"], order_column="updated_at")
        rows = {row["id"]: row for row in result.collect()}

        assert len(rows) == 2
        assert rows["1"]["value"] == "v2"  # latest date
        assert rows["2"]["value"] == "v1"

    def test_keeps_earliest_when_ascending(self, spark: SparkSession) -> None:
        """With ascending=True, should keep the row with the lowest order_column value."""
        df = spark.createDataFrame(
            [
                ("1", "2024-01-05", "latest"),
                ("1", "2024-01-01", "earliest"),
            ],
            ["id", "updated_at", "value"],
        )

        result = dedup_by_key(
            df, key_columns=["id"], order_column="updated_at", ascending=True
        )
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["value"] == "earliest"

    def test_composite_key(self, spark: SparkSession) -> None:
        """Should deduplicate on composite keys."""
        df = spark.createDataFrame(
            [
                ("1", "A", "2024-01-01", "old"),
                ("1", "A", "2024-01-05", "new"),
                ("1", "B", "2024-01-01", "only"),
            ],
            ["id", "category", "updated_at", "value"],
        )

        result = dedup_by_key(
            df, key_columns=["id", "category"], order_column="updated_at"
        )

        assert result.count() == 2

    def test_no_duplicates_unchanged(self, spark: SparkSession) -> None:
        """DataFrame without duplicates should pass through unchanged."""
        df = spark.createDataFrame(
            [("1", "2024-01-01", "a"), ("2", "2024-01-02", "b")],
            ["id", "ts", "val"],
        )

        result = dedup_by_key(df, key_columns=["id"], order_column="ts")
        assert result.count() == 2


class TestDedupExact:
    """Tests for dedup_exact."""

    def test_removes_exact_duplicates(self, spark: SparkSession) -> None:
        """Should remove rows that are identical across all columns."""
        df = spark.createDataFrame(
            [("1", "Alice"), ("1", "Alice"), ("2", "Bob")],
            ["id", "name"],
        )

        result = dedup_exact(df)
        assert result.count() == 2

    def test_keeps_partial_duplicates(self, spark: SparkSession) -> None:
        """Rows with same key but different values should be kept."""
        df = spark.createDataFrame(
            [("1", "Alice"), ("1", "Alicia")],
            ["id", "name"],
        )

        result = dedup_exact(df)
        assert result.count() == 2
