# Open Lakehouse

An open-source data lakehouse built on **PySpark**, **Delta Lake**, and **Unity Catalog OSS**. Orchestrated with **Dagster** using a medallion architecture (bronze → silver → gold) and backed by S3-compatible storage (MinIO for local dev).

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌───────────────┐
│   Dagster   │────▶│  PySpark + Delta │────▶│  MinIO (S3)   │
│ Orchestrator│     │  Processing      │     │  Object Store │
└─────────────┘     └──────────────────┘     └───────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │  Unity Catalog   │
                    │  (Metadata)      │
                    └──────────────────┘
```

**Medallion layers:**

| Layer | Purpose |
|-------|---------|
| Bronze | Raw ingestion with `ingested_at` timestamp |
| Silver | Deduplication, schema enforcement, enrichment |
| Gold | Business-level aggregations |

## Function Library

Reusable Spark functions in `src/lakehouse/lib/`:

| Module | Functions |
|--------|-----------|
| `scd.py` | `scd_type1` (overwrite merge), `scd_type2` (history tracking) |
| `dedup.py` | `dedup_by_key` (window-based), `dedup_exact` (drop duplicates) |
| `merge.py` | `upsert` (configurable Delta MERGE) |
| `quality.py` | `check_not_null`, `check_unique`, `check_range`, `check_referential_integrity` |
| `schema.py` | `enforce_schema` (cast/add/drop columns), `validate_schema` (non-mutating) |

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Latest | Run UC, MinIO, and Spark cluster |
| [uv](https://docs.astral.sh/uv/) | ≥ 0.2 | Python package & environment manager |
| [Java JDK](https://adoptium.net/) | 17 | Required by PySpark |
| [Hadoop winutils](https://github.com/cdarlint/winutils) | 3.3.x | Windows only — place in `C:\hadoop\bin\` |

> **Note:** Python 3.11 is required. PySpark 3.5 has a worker crash bug on Python 3.12+ on Windows. The `.python-version` file pins this automatically when using `uv`.

## Getting Started

### 1. Clone and install

```bash
git clone https://github.com/pegodk/open-lakehouse.git
cd open-lakehouse
uv sync --all-extras
```

### 2. Environment variables

```bash
cp .env.example .env
# Edit .env if needed (defaults work for local Docker setup)
```

### 3. Start infrastructure

```bash
docker compose up -d
```

This starts:
- **Unity Catalog** — http://localhost:8080
- **MinIO** — http://localhost:9000 (API), http://localhost:9001 (console, `minioadmin`/`minioadmin`)
- **Spark master** — spark://localhost:7077, UI at http://localhost:8081
- **Spark worker** — connects to master automatically

### 4. Run unit tests

Tests run with an embedded local Spark (no Docker required):

```bash
uv run pytest tests/ -v
```

### 5. Run Dagster

```bash
uv run dagster dev
```

Open http://localhost:3000 to materialize assets.

## Project Structure

```
open-lakehouse/
├── src/lakehouse/
│   ├── assets/              # Dagster assets (bronze/silver/gold)
│   ├── lib/                 # Reusable Spark functions
│   ├── models/              # Schema definitions (StructTypes)
│   ├── resources/           # Dagster resources (SparkResource)
│   └── definitions.py       # Dagster Definitions entry point
├── tests/
│   ├── unit/                # Unit tests (local Spark, no infra needed)
│   └── integration/         # Integration tests (requires Docker)
├── docker-compose.yml        # UC + MinIO + Spark cluster
├── .env.example              # Environment variable template
├── pyproject.toml            # Dependencies and tool config
└── .github/workflows/        # CI (lint + tests on PR, integration on merge)
```

## Configuration

The `SPARK_MASTER` env var controls where Spark runs:

| Value | Mode | When to use |
|-------|------|-------------|
| `local[*]` | Embedded | Unit tests, quick iteration |
| `spark://localhost:7077` | Standalone cluster | Running Dagster assets against the Docker cluster |

## CI/CD

- **PR:** Lint (ruff) + unit tests on Python 3.11/3.12 matrix
- **Merge to main:** Integration tests with Docker Compose services

## License

MIT
