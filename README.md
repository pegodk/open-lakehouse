# Open Lakehouse

An open-source data lakehouse built on **PySpark**, **Delta Lake**, and **Unity Catalog OSS**. Orchestrated with **Dagster** using a medallion architecture (bronze → silver → gold) and backed by S3-compatible storage (MinIO for local dev).

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌───────────────┐
│   Dagster   │────▶│  PySpark + Delta │────▶│  MinIO (S3)   │
│ Orchestrator│     │  (large jobs)    │     │  Object Store │
│             │────▶│  DuckDB + Delta  │     │               │
│             │     │  (small jobs)    │     │               │
└─────────────┘     └──────────────────┘     └───────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │  Unity Catalog   │
                    │  (Metadata /     │
                    │   Governance)    │
                    └──────────────────┘
```

**Medallion layers:**

| Layer | Purpose | Engine |
|-------|---------|--------|
| Bronze | Raw ingestion with `ingested_at` timestamp | Spark |
| Silver | Deduplication, schema enforcement, enrichment | Spark |
| Gold | Business-level aggregations | Spark or **DuckDB** |

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
- **Unity Catalog API** — http://localhost:8080
- **Unity Catalog UI** — http://localhost:3000
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
│   ├── assets/
│   │   ├── bronze/          # Raw ingestion (Spark)
│   │   ├── silver/          # Cleansed / enriched (Spark)
│   │   └── gold/            # Aggregations (Spark + DuckDB)
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

## Tech Stack Rationale

### The four-component model

| Component | Role | "Why this one?" |
|-----------|------|-----------------|
| **Unity Catalog OSS** | Metadata & governance | Single namespace (`catalog.schema.table`) across every engine; fine-grained ACLs; open REST API |
| **Delta Lake** | Storage format | ACID transactions, time-travel, Z-ordering, Change Data Feed; works equally well from Spark, DuckDB, Rust, and Python |
| **Apache Spark** | Heavy compute | Distributed processing for large ingestion, complex joins, and ML feature pipelines (bronze → silver) |
| **DuckDB** | Lightweight compute | In-process SQL engine for smaller aggregations and ad-hoc analysis with no JVM overhead (smaller gold jobs) |

The key insight is that **Delta Lake is the integration layer**. Because every engine can read and write the same Delta files, you can pick the right tool per job without copying data or maintaining separate stores.

```
┌─────────────────────────────────────────────────────────┐
│                   Unity Catalog (metadata)              │
│   catalog.bronze.*   catalog.silver.*   catalog.gold.*  │
└───────────────────────────┬─────────────────────────────┘
                            │ same Delta files
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
         Apache Spark    DuckDB      delta-rs / Arrow
         (large jobs)  (small jobs)  (Python / Rust)
```

### Alternatives and trade-offs

#### Apache Polaris + Apache Iceberg

[Apache Polaris](https://polaris.apache.org/) is the open-source catalog originally developed by Snowflake. [Apache Iceberg](https://iceberg.apache.org/) is the table format it most naturally pairs with.

| Dimension | Polaris + Iceberg | Unity Catalog + Delta Lake |
|-----------|------------------|---------------------------|
| **Catalog maturity** | Newer project (Apache incubation); smaller community | UC OSS backed by Databricks; large OSS community |
| **Spark integration** | Good via `IcebergSparkSessionExtensions` | Native — Delta is a first-class Spark data source |
| **DuckDB reading** | Via Iceberg REST catalog or file scanning | Via `delta` extension or direct Parquet scan |
| **ACID guarantees** | Full (v2 row-level deletes) | Full (optimistic concurrency, MVCC) |
| **Time travel** | Yes (snapshot-based) | Yes (version + timestamp-based) |
| **Merge / UPSERT** | `MERGE INTO` in Spark/Flink | `MERGE INTO` + `upsert()` helper in this repo |
| **Change Data Feed** | Not built-in (use Flink CDC) | Built-in `SHOW CHANGES` / CDF |
| **Vendor neutrality** | Purely Apache-governed | Apache-licensed but Databricks-led |
| **Multi-engine** | Strong (Flink, Trino, Spark, Snowflake) | Strong (Spark, DuckDB, Trino, Athena) |

**When Polaris + Iceberg makes sense:** you need tight Flink integration, you run workloads on Snowflake or Trino as primary engines, or you require a purely Apache-governed stack.

**When Unity Catalog + Delta Lake makes sense (this project):** you use Spark as the primary processing engine, want Change Data Feed for incremental pipelines, value the simpler DuckDB/Python integration via `deltalake` (delta-rs), and prefer the larger Delta Lake OSS ecosystem.

#### Apache Hive Metastore + Parquet / ORC

The traditional Hadoop-era approach. Still common in legacy clusters but lacks ACID semantics, time travel, and schema evolution. No meaningful path to supporting multiple compute engines cleanly.

#### Project Nessie + Apache Iceberg

[Nessie](https://projectnessie.org/) adds Git-like branching to Iceberg — you can create a branch, run ETL, and merge it like a pull request. Compelling for complex multi-team pipelines but introduces operational overhead most projects don't need at the start.

#### AWS Glue Data Catalog + Iceberg

Fully managed, zero-ops, and integrates well with Athena and EMR. The trade-off is vendor lock-in: Glue is AWS-specific, and migrating away is painful.

## License

MIT
