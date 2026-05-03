"""Shared pytest fixtures for unit and integration tests."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake support for testing.

    Session-scoped to avoid the overhead of starting/stopping Spark per test.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("open-lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()
