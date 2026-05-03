"""Shared pytest fixtures for unit and integration tests."""

import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Ensure PySpark workers use the same Python interpreter as the test runner
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
os.environ.setdefault(
    "JAVA_HOME", r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create a local SparkSession with Delta Lake support for testing.

    Session-scoped to avoid the overhead of starting/stopping Spark per test.
    Uses delta-spark's configure_spark_with_delta_pip to ensure JARs are available.
    """
    builder = (
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
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
