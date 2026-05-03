"""Data quality check functions for Spark DataFrames."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class QualityCheckResult:
    """Result of a data quality check."""

    check_name: str
    passed: bool
    total_rows: int
    failed_rows: int
    message: str


def check_not_null(df: DataFrame, columns: list[str]) -> QualityCheckResult:
    """Check that specified columns contain no null values.

    Args:
        df: DataFrame to validate.
        columns: Column names that must not be null.

    Returns:
        QualityCheckResult with pass/fail status and failure count.
    """
    total = df.count()
    null_condition = F.lit(False)
    for col in columns:
        null_condition = null_condition | F.col(col).isNull()

    failed = df.filter(null_condition).count()
    passed = failed == 0

    return QualityCheckResult(
        check_name="not_null",
        passed=passed,
        total_rows=total,
        failed_rows=failed,
        message=f"Columns {columns}: {failed}/{total} rows have nulls",
    )


def check_unique(df: DataFrame, columns: list[str]) -> QualityCheckResult:
    """Check that the combination of columns is unique across all rows.

    Args:
        df: DataFrame to validate.
        columns: Columns forming the uniqueness constraint.

    Returns:
        QualityCheckResult with duplicate count.
    """
    total = df.count()
    distinct = df.select(columns).distinct().count()
    duplicates = total - distinct
    passed = duplicates == 0

    return QualityCheckResult(
        check_name="unique",
        passed=passed,
        total_rows=total,
        failed_rows=duplicates,
        message=f"Columns {columns}: {duplicates} duplicate rows",
    )


def check_range(
    df: DataFrame,
    column: str,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> QualityCheckResult:
    """Check that a numeric column's values fall within an expected range.

    Args:
        df: DataFrame to validate.
        column: Numeric column to check.
        min_value: Minimum allowed value (inclusive). None = no lower bound.
        max_value: Maximum allowed value (inclusive). None = no upper bound.

    Returns:
        QualityCheckResult with out-of-range row count.
    """
    total = df.count()
    condition = F.lit(False)

    if min_value is not None:
        condition = condition | (F.col(column) < min_value)
    if max_value is not None:
        condition = condition | (F.col(column) > max_value)

    failed = df.filter(condition).count()
    passed = failed == 0

    return QualityCheckResult(
        check_name="range",
        passed=passed,
        total_rows=total,
        failed_rows=failed,
        message=f"Column '{column}' [{min_value}, {max_value}]: {failed}/{total} out of range",
    )


def check_referential_integrity(
    df: DataFrame,
    column: str,
    reference_df: DataFrame,
    reference_column: str,
) -> QualityCheckResult:
    """Check that all values in a column exist in a reference DataFrame.

    Args:
        df: DataFrame to validate (child/fact table).
        column: Column containing foreign key values.
        reference_df: Reference DataFrame (parent/dimension table).
        reference_column: Column in reference containing valid values.

    Returns:
        QualityCheckResult with orphan row count.
    """
    total = df.count()

    valid_values = reference_df.select(F.col(reference_column).alias("_ref_val")).distinct()
    orphans = df.join(
        valid_values,
        df[column] == valid_values["_ref_val"],
        "left_anti",
    ).count()

    passed = orphans == 0

    return QualityCheckResult(
        check_name="referential_integrity",
        passed=passed,
        total_rows=total,
        failed_rows=orphans,
        message=f"Column '{column}': {orphans}/{total} rows have no match in reference",
    )
