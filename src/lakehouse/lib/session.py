"""SparkSession builder supporting local, cluster, and Unity Catalog modes."""

from __future__ import annotations

import os

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "open-lakehouse",
    env_file: str | None = None,
) -> SparkSession:
    """Build a SparkSession configured for Delta Lake and optionally Unity Catalog.

    Reads configuration from environment variables (loaded from .env if present).
    """
    load_dotenv(env_file)

    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    environment = os.getenv("ENV", "local")
    catalog_name = os.getenv("CATALOG_NAME", "main")
    uc_server_url = os.getenv("UC_SERVER_URL", "")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # Unity Catalog configuration (non-local environments or when UC_SERVER_URL is set)
    if uc_server_url:
        builder = (
            builder.config(
                f"spark.sql.catalog.{catalog_name}",
                "io.unitycatalog.spark.UCSingleCatalog",
            )
            .config(f"spark.sql.catalog.{catalog_name}.uri", uc_server_url)
            .config(f"spark.sql.catalog.{catalog_name}.token", "")
        )

    # S3/MinIO storage configuration
    if minio_endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    # Local mode optimizations
    if environment == "local":
        builder = (
            builder.config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
        )

    return configure_spark_with_delta_pip(builder).getOrCreate()
