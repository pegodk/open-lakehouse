"""Unit tests for lakehouse.lib.schema — Schema enforcement."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from lakehouse.lib.schema import SchemaValidationError, enforce_schema, validate_schema


@pytest.fixture
def expected_schema() -> StructType:
    """Sample schema for testing."""
    return StructType(
        [
            StructField("id", StringType(), nullable=False),
            StructField("name", StringType(), nullable=True),
            StructField("amount", DoubleType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
        ]
    )


class TestEnforceSchema:
    """Tests for enforce_schema."""

    def test_matching_schema_passes_through(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        """DataFrame matching schema should pass through unchanged."""
        df = spark.createDataFrame(
            [("1", "Widget", 9.99, 5)],
            schema=expected_schema,
        )

        result = enforce_schema(df, expected_schema)
        assert result.schema == expected_schema
        assert result.count() == 1

    def test_casts_mismatched_types(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        """String columns should be cast to expected numeric types."""
        df = spark.createDataFrame(
            [("1", "Widget", "9.99", "5")],
            ["id", "name", "amount", "quantity"],
        )

        result = enforce_schema(df, expected_schema, cast_types=True)

        row = result.collect()[0]
        assert row["amount"] == 9.99
        assert row["quantity"] == 5
        assert result.schema["amount"].dataType == DoubleType()
        assert result.schema["quantity"].dataType == IntegerType()

    def test_adds_missing_columns_as_null(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        """Missing columns should be added with null values."""
        df = spark.createDataFrame(
            [("1", "Widget")],
            ["id", "name"],
        )

        result = enforce_schema(df, expected_schema)

        assert "amount" in result.columns
        assert "quantity" in result.columns
        row = result.collect()[0]
        assert row["amount"] is None
        assert row["quantity"] is None

    def test_drops_extra_columns(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        """Extra columns not in schema should be dropped."""
        df = spark.createDataFrame(
            [("1", "Widget", 9.99, 5, "extra_value")],
            ["id", "name", "amount", "quantity", "extra_col"],
        )

        result = enforce_schema(df, expected_schema, drop_extra_columns=True)

        assert "extra_col" not in result.columns
        assert len(result.columns) == 4

    def test_strict_mode_raises_on_mismatch(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        """Strict mode should raise SchemaValidationError on type mismatch."""
        df = spark.createDataFrame(
            [("1", "Widget", "not_a_number", "5")],
            ["id", "name", "amount", "quantity"],
        )

        with pytest.raises(SchemaValidationError) as exc_info:
            enforce_schema(df, expected_schema, strict=True)

        assert "amount" in str(exc_info.value)

    def test_strict_mode_raises_on_missing_column(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        """Strict mode should raise on missing columns."""
        df = spark.createDataFrame([("1", "Widget")], ["id", "name"])

        with pytest.raises(SchemaValidationError) as exc_info:
            enforce_schema(df, expected_schema, strict=True)

        assert "amount" in str(exc_info.value) or "Missing" in str(exc_info.value)


class TestValidateSchema:
    """Tests for validate_schema (non-mutating)."""

    def test_valid_schema_returns_empty_list(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        df = spark.createDataFrame(
            [("1", "Widget", 9.99, 5)],
            schema=expected_schema,
        )

        errors = validate_schema(df, expected_schema)
        assert errors == []

    def test_missing_column_reported(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        df = spark.createDataFrame([("1", "Widget")], ["id", "name"])

        errors = validate_schema(df, expected_schema)
        assert len(errors) == 2  # amount and quantity missing
        assert any("amount" in e for e in errors)

    def test_type_mismatch_reported(
        self, spark: SparkSession, expected_schema: StructType
    ) -> None:
        df = spark.createDataFrame(
            [("1", "Widget", "9.99", "5")],
            ["id", "name", "amount", "quantity"],
        )

        errors = validate_schema(df, expected_schema)
        assert any("amount" in e and "mismatch" in e.lower() for e in errors)
