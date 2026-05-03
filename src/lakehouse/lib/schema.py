"""Schema enforcement and evolution helpers.

Schema enforcement is applied at the silver layer to ensure data conforms
to the expected StructType before being written to Delta tables.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class SchemaValidationError(Exception):
    """Raised when a DataFrame does not conform to the expected schema."""

    def __init__(self, missing_columns: list[str], type_mismatches: dict[str, tuple[str, str]]):
        self.missing_columns = missing_columns
        self.type_mismatches = type_mismatches
        parts = []
        if missing_columns:
            parts.append(f"Missing columns: {missing_columns}")
        if type_mismatches:
            mismatches = [
                f"{col}: expected {exp}, got {act}" for col, (exp, act) in type_mismatches.items()
            ]
            parts.append(f"Type mismatches: {mismatches}")
        super().__init__("; ".join(parts))


def enforce_schema(
    df: DataFrame,
    expected_schema: StructType,
    *,
    drop_extra_columns: bool = True,
    cast_types: bool = True,
    strict: bool = False,
) -> DataFrame:
    """Enforce a StructType schema on a DataFrame.

    Validates that all expected columns exist and optionally casts types.
    Used at the silver layer to gate data quality.

    Args:
        df: Input DataFrame (typically from bronze).
        expected_schema: Target StructType defining expected columns and types.
        drop_extra_columns: If True, remove columns not in the expected schema.
        cast_types: If True, attempt safe type casting for mismatched types.
        strict: If True, raise SchemaValidationError on any mismatch instead of casting.

    Returns:
        DataFrame conforming to expected_schema.

    Raises:
        SchemaValidationError: If strict=True and schema doesn't match.
    """
    expected_fields = {field.name: field for field in expected_schema.fields}
    actual_fields = {field.name: field for field in df.schema.fields}

    # Check for missing columns
    missing = [name for name in expected_fields if name not in actual_fields]

    # Check for type mismatches
    type_mismatches: dict[str, tuple[str, str]] = {}
    for name, field in expected_fields.items():
        if name in actual_fields and actual_fields[name].dataType != field.dataType:
            type_mismatches[name] = (
                str(field.dataType),
                str(actual_fields[name].dataType),
            )

    if strict and (missing or type_mismatches):
        raise SchemaValidationError(missing, type_mismatches)

    # Apply transformations
    result = df

    # Add missing columns as null with correct type
    for col_name in missing:
        field = expected_fields[col_name]
        result = result.withColumn(col_name, F.lit(None).cast(field.dataType))

    # Cast type mismatches
    if cast_types:
        for col_name, (_expected_type_str, _) in type_mismatches.items():
            target_type = expected_fields[col_name].dataType
            result = result.withColumn(col_name, F.col(col_name).cast(target_type))

    # Select columns in schema order, optionally dropping extras
    if drop_extra_columns:
        result = result.select([field.name for field in expected_schema.fields])

    return result


def validate_schema(df: DataFrame, expected_schema: StructType) -> list[str]:
    """Validate a DataFrame against an expected schema without modifying it.

    Returns a list of validation error messages (empty if valid).

    Args:
        df: DataFrame to validate.
        expected_schema: Expected StructType.

    Returns:
        List of error message strings. Empty list means valid.
    """
    errors: list[str] = []
    expected_fields = {field.name: field for field in expected_schema.fields}
    actual_fields = {field.name: field for field in df.schema.fields}

    for name, field in expected_fields.items():
        if name not in actual_fields:
            errors.append(f"Missing column: '{name}' (expected {field.dataType})")
        elif actual_fields[name].dataType != field.dataType:
            errors.append(
                f"Type mismatch for '{name}': "
                f"expected {field.dataType}, got {actual_fields[name].dataType}"
            )

    return errors
