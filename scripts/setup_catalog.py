"""Bootstrap Unity Catalog from declarative YAML configuration.

Reads catalog/, schemas/, and tables YAML files and creates them
via the Unity Catalog REST API.

Usage:
    uv run python scripts/setup_catalog.py
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

try:
    import requests
except ImportError:
    import urllib.request
    import json

    class _SimpleRequests:
        """Minimal requests-like wrapper using urllib (avoids extra dependency)."""

        @staticmethod
        def post(url: str, json_data: dict, headers: dict | None = None) -> None:
            data = json.dumps(json_data).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json", **(headers or {})},
                method="POST",
            )
            try:
                urllib.request.urlopen(req)  # noqa: S310
            except urllib.error.HTTPError as e:
                if e.code == 409:
                    pass  # Already exists — idempotent
                else:
                    raise

    requests = _SimpleRequests()  # type: ignore[assignment]


def main() -> None:
    load_dotenv()

    uc_url = os.getenv("UC_SERVER_URL", "http://localhost:8080/api/2.1/unity-catalog")
    catalog_dir = Path(__file__).parent.parent / "catalog"

    # Create catalogs
    catalogs_file = catalog_dir / "catalogs.yaml"
    if catalogs_file.exists():
        with open(catalogs_file) as f:
            config = yaml.safe_load(f)
        for catalog in config.get("catalogs", []):
            print(f"Creating catalog: {catalog['name']}")
            requests.post(
                f"{uc_url}/catalogs",
                json_data={"name": catalog["name"], "comment": catalog.get("comment", "")},
            )

    # Create schemas
    schemas_file = catalog_dir / "schemas.yaml"
    if schemas_file.exists():
        with open(schemas_file) as f:
            config = yaml.safe_load(f)
        for schema in config.get("schemas", []):
            print(f"Creating schema: {schema['catalog']}.{schema['name']}")
            requests.post(
                f"{uc_url}/schemas",
                json_data={
                    "name": schema["name"],
                    "catalog_name": schema["catalog"],
                    "comment": schema.get("comment", ""),
                },
            )

    # Create tables
    tables_file = catalog_dir / "tables.yaml"
    if tables_file.exists():
        with open(tables_file) as f:
            config = yaml.safe_load(f)
        for table in config.get("tables", []):
            full_name = f"{table['catalog']}.{table['schema']}.{table['name']}"
            print(f"Creating table: {full_name}")
            requests.post(
                f"{uc_url}/tables",
                json_data={
                    "name": table["name"],
                    "catalog_name": table["catalog"],
                    "schema_name": table["schema"],
                    "table_type": table.get("type", "EXTERNAL"),
                    "storage_location": table.get("storage_location", ""),
                    "comment": table.get("comment", ""),
                    "data_source_format": "DELTA",
                },
            )

    print("Catalog setup complete.")


if __name__ == "__main__":
    main()
