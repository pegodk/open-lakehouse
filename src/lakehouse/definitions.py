"""Dagster Definitions — entry point for the open-lakehouse project."""

from dagster import Definitions, load_assets_from_package_module

from lakehouse import assets
from lakehouse.resources.spark import SparkResource

all_assets = load_assets_from_package_module(assets)

defs = Definitions(
    assets=all_assets,
    resources={
        "spark": SparkResource(),
    },
)
