# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Airflow ETL pipeline that collects cloud cost reports from AWS, OCI, GCP, and Huawei Cloud in the [FOCUS](https://focus.finops.org/) (FinOps Open Cost and Usage Specification) standard, loads them into PostgreSQL, and backs up raw files to S3-compatible storage.

## Development Setup

```bash
pip install -r requirements.txt
# Optional for testing:
pip install pytest hypothesis
python -m pytest
```

No Docker or CI/CD config is included — this repo focuses exclusively on ETL logic. The expected runtime is Apache Airflow >= 2.7.0 with the `plugins/` directory mounted to Airflow's plugins folder and `dags/` to its dags folder.

**Airflow Variables:** Copy `airflow_variables.example.json` and import it into Airflow UI (Admin → Variables) or via CLI:
```bash
airflow variables import airflow_variables.json
```

**Required temp directories** (must be persistent volumes in Kubernetes):
```
/tmp/aws_downloads
/tmp/oci_downloads
/tmp/gcp_downloads
/tmp/hpc_downloads
```

## Architecture

### Data Flow
```
Cloud Bucket → list_processed_keys → download_new_objects → unpack_files
                                                           → load_to_db → upload_to_ecs → clean_temp_dir
```

Each DAG is an independently scheduled pipeline for one provider. GCP skips the unpack step (CSVs are uncompressed). Deduplication is enforced via `tb_processed_files` in PostgreSQL, keyed by `(provider, relative_path_with_original_extension)`.

### Key Modules

| Path | Responsibility |
|------|---------------|
| `dags/` | 4 Airflow DAGs — one per provider. All paused on creation, max 1 concurrent run. |
| `plugins/tasks.py` | Task functions wired into DAGs. Contains the full ETL logic per step. |
| `plugins/loaders/` | Provider-specific data loading: parsing, transformation, upsert to PostgreSQL. `BaseLoader` defines the template method (`load()`, `is_file_processed()`, `register_processed_file()`); subclasses override `read_files()`, `transform()`, `load_to_db()`. |
| `plugins/storages/` | Provider-specific cloud storage clients (list/download/upload). `DellECSStorage` is the backup target — uses S3-compatible API with custom SigV4 signing. |
| `plugins/utils/` | Shared utilities: structured logging, file decompression/sorting, DB helpers, DataFrame helpers, DAG callbacks. |
| `plugins/interfaces/` | Abstract base contracts for storages. |

Both storages and loaders use the **Factory pattern**: `StorageFactory` and `LoaderFactory` map provider strings to concrete classes. Add a new provider by implementing the interfaces and registering in the factories.

### Provider Summary

| Provider | Format | Compression | Schedule |
|----------|--------|-------------|----------|
| AWS | Parquet/CSV | `.gz` | 4× daily (04/10/16/22h) |
| OCI | CSV | `.gz` | Every 2 hours |
| GCP | CSV | none | Daily 10h |
| Huawei (HPC) | CSV | `.zip` | Daily 06h |

### AWS Special Logic
AWS FOCUS reports use snapshot versioning with multi-part chunks (e.g., `data/00001.csv.gz`, `00002.csv.gz`). `AWSLoader._select_latest_snapshot_files()` keeps only the newest snapshot per billing period and marks obsolete snapshots as processed without loading. On the first chunk (`00001.csv`), `clean_data()` deletes existing records for that billing period before re-inserting.

### Logging
`FinOpsLogger` in `plugins/utils/logging_config.py` writes structured logs to **stdout** (not stderr) to avoid Airflow false positives. It adds context fields (provider, phase, duration_ms, records_count) and emoji prefixes (🚀 start, 🏁 end, ❌ error, ✅ info). Use `logger.log_operation()` as a context manager for automatic timing, or `@log_execution()` as a decorator.

### Database
`plugins/utils/database.py` — all DB access goes through `get_engine()` which reads credentials from Airflow Variables. `upsert_table()` uses `ON CONFLICT DO UPDATE` for idempotency. `clean_data()` deletes by dynamic WHERE conditions (used by AWS to purge stale billing-period data).
