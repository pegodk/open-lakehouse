"""Dagster resource wrapping the SparkSession builder."""

from __future__ import annotations

from dagster import ConfigurableResource
from pyspark.sql import SparkSession

from lakehouse.lib.session import get_spark_session


class SparkResource(ConfigurableResource):
    """Dagster-managed SparkSession resource.

    Reads configuration from environment variables via Dagster's EnvVar.
    Provides a SparkSession to assets that depend on it.
    """

    spark_master: str = "local[*]"
    app_name: str = "open-lakehouse"

    def get_session(self) -> SparkSession:
        """Return a configured SparkSession."""
        return get_spark_session(app_name=self.app_name)
