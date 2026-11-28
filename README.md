# ScholarStream: Incremental Topic Classification Pipeline for Scholarly Data

An end-to-end data platform that ingests research papers from [OpenAlex](https://openalex.org), processes them through a bronze/silver/gold (medallion) data lake, and feeds a multi-label classifier that learns new research topics incrementally — without retraining from scratch.

> **Focus:** This is primarily a **data engineering** project. The ML component exists to demonstrate how a data platform serves a continuously-learning model in production-like conditions.

> **Status:** Early stage. Working locally: incremental ingestion into bronze, bronze → silver, the quality gate, Airflow DAGs chaining them, and the dbt gold star schema with its tests, built by Airflow after the quality gate. ML, API, dashboard and AWS infrastructure are planned.

---

## Why this project

New research papers are published every day, and the set of topics we care about grows over time. Retraining a classifier from scratch every time is expensive, and old training data may not be fully available. This project explores how to build a pipeline where:

- Data arrives **incrementally** and is processed idempotently
- Data quality is **enforced** before reaching analytics or ML consumers
- A multi-label classifier **learns new topics in stages** (multi-label class-incremental learning, MLCIL) while retaining knowledge of old ones

## Architecture

```mermaid
flowchart TD
    A[OpenAlex API] --> B[Incremental ingestion<br/>Python + watermark]
    B --> C[Bronze: raw JSON<br/>S3 / MinIO]
    C --> D[Silver: cleaned, abstracts rebuilt<br/>Polars → Parquet]
    D --> E{Data quality gate<br/>Polars checks}
    E -->|pass| F[Gold: star schema<br/>dbt]
    E -->|fail| X[Alert + stop DAG]
    F --> G[Dashboard<br/>Superset / Metabase]
    F --> H[Task split + embeddings]
    H --> I[Incremental training<br/>replay buffer + pseudo-labels]
    I --> J[Model registry<br/>MLflow]
    J --> K[Prediction API<br/>FastAPI]
    K --> L[Monitoring]
```

All stages are orchestrated by **Apache Airflow**.

## Tech stack

| Layer | Local | Cloud (AWS) |
|---|---|---|
| Orchestration | Airflow (Docker Compose) | Airflow on EC2 |
| Storage | MinIO | S3 + Glue Data Catalog |
| Processing | Polars | Polars on EC2 |
| Transformation | dbt + DuckDB | dbt + Athena |
| Data quality | Polars quality gate (silver), dbt tests (gold) | same |
| ML tracking | MLflow | MLflow (artifacts on S3) |
| Training | Kaggle (one-time backfill) | SageMaker Training Job (CPU) |
| Serving | FastAPI (Docker) | ECR + App Runner |
| CI/CD | GitHub Actions | GitHub Actions (OIDC → AWS) |
| Infrastructure | Docker Compose | Terraform |

## Pipeline design

### 1. Incremental ingestion
- Pulls works from the OpenAlex API, filtered by field and publication date, using cursor-based pagination
- Stores a **watermark** (last successful load date) in Postgres so each run continues where the last one stopped
- **Late-arriving data:** OpenAlex keeps adding works to a publication day for weeks, and filtering by index date (`from_created_date`) needs a paid plan. So every daily run re-fetches the last 7 days, and a weekly `refresh_openalex` DAG re-fetches days 8–60 back (about 6% of works arrived or changed after the 7-day window, measured on Oct–Nov 2026)
- **Idempotent:** re-running the same date range produces no duplicates
- Supports **backfill** for arbitrary historical date ranges

### 2. Bronze → Silver
- Raw JSON lands in the bronze layer unchanged, partitioned by publication date, so re-running a day replaces its partition
- Silver layer:
  - Reconstructs abstracts from OpenAlex's `abstract_inverted_index` format
  - Deduplicates records, keeping the latest version per `work_id`
  - Flattens nested topic arrays into a many-to-many table

### 3. Data quality gate
Plain Polars checks run on each silver day before it is promoted to gold. A day that passes gets a `_QUALITY_OK` marker; if any check fails, the DAG stops. Checks include:
- `work_id` uniqueness and not-null constraints
- Referential integrity between works and topics, and a complete topic hierarchy
- Publication date equal to the partition day and not in the future
- Row count against the median of the same weekday over the previous 4 weeks

### 4. Gold layer (dbt)
dbt + DuckDB builds a star schema as Parquet in the `gold` bucket, reading **only silver days marked `_QUALITY_OK`**. Tables are rebuilt in full on every run, so a re-run is idempotent:
- `fact_work_topics`: one row per (work, topic), with rank and a primary-topic flag
- `dim_works` (one row per work, with `has_abstract`), `dim_topics` (subfield → field → domain), `dim_date`

dbt tests check keys, relationships from the fact to every dimension, the fact grain, exactly one primary topic per work, and that no unchecked day reaches gold.

Planned: `dim_authors`, `dim_institutions`, and marts such as topic trends by year.

### 5. Pipeline monitoring
Every DAG records what it did in two Postgres tables, next to the watermark:
- `pipeline_task_runs`: one row per task run, with status, start/finish time, try number, the error of a failed try, and the OpenAlex credits used and left
- `pipeline_day_metrics`: one row per task run, layer and publication day, with the row count and the verdict (`loaded`, `transformed`, `passed` / `deferred` / `failed` with the gate's reasons, `built`)

Writes are upserts on the run's key, so a retried or re-run task replaces its rows instead of adding new ones, and they are best-effort: a failure to record metrics never stops the pipeline.

Shared task callbacks record each outcome. A final failure also logs an `ALERT` line (alerts go to the log only for now); a retry is only a warning. A daily `pipeline_health` DAG (06:00 UTC) checks results rather than task states, so it also catches a pipeline that stopped without failing: the latest gold day is at most 3 days old, the last successful ingest at most 26 hours old, no silver day past the lookback window still lacks `_QUALITY_OK`, and the API budget did not run out twice in a row. It runs inside the same Airflow, so during a full outage it cannot run either and reports the gap once the stack is back; an outside heartbeat is planned with the move to the cloud.

### 6. ML pipeline
- **Task split:** the top-N topics are divided into sequential tasks (e.g. 10 tasks × 10 topics), simulating new topics arriving over time
- **Embeddings:** computed once with a frozen scientific-text encoder (SPECTER2); only daily new records are embedded afterwards
- **Incremental training:** a lightweight multi-label classifier trained task by task, using a replay buffer of past samples and pseudo-labels for old topics. The design is inspired by KBK (Knowing Beyond the Known)
- **Cost-aware design:** GPU is needed only for the one-time historical backfill; steady-state embedding and training run on CPU

### 7. Serving & monitoring
- FastAPI endpoint: input an abstract, get predicted topics with confidence scores
- Prediction logs are stored and monitored for distribution shifts and low-confidence rates

## Repository structure

```
.
├── dags/                  # Airflow DAGs
├── ingestion/             # OpenAlex API client, watermark logic
├── transform/             # Polars bronze → silver jobs
├── dbt/                   # dbt project: staging + gold models, tests
├── quality/               # Quality gate between silver and gold (Polars)
├── monitoring/            # Run metrics (Postgres), task callbacks, pipeline health checks
├── ml/
│   ├── embeddings/        # Embedding computation
│   └── training/          # Incremental training, replay buffer
├── api/                   # FastAPI service
├── infra/                 # Terraform (AWS)
├── tests/                 # pytest unit tests
├── .github/workflows/     # CI/CD pipelines
├── docker-compose.yml     # Local stack
├── .env.example           # Environment variable template
└── .gitignore
```

## Getting started

> The local stack is being built incrementally; the commands below describe the target setup.

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- At least 8 GB RAM recommended (Airflow is memory-hungry)

### Run locally

```bash
git clone https://github.com/<your-username>/scholarstream.git
cd scholarstream
cp .env.example .env        # fill in your settings
docker compose up -d
```

Then open:
- Airflow UI: http://localhost:8080
- MinIO console: http://localhost:9001
Trigger the `ingest_openalex` DAG to start the pipeline (`refresh_openalex` re-fetches older days every Sunday); `bronze_to_silver` (with the quality gate) runs after it, then `silver_to_gold` (dbt build + tests) once new silver days pass the gate. Unpause `pipeline_health` for the daily health check (new DAGs start paused); `python -m monitoring.health [--as-of YYYY-MM-DD]` runs the same checks from the shell.

### Build and test gold

dbt runs in its own environment and does not read `.env`, so export it first:

```bash
python3 -m venv .venv-dbt && .venv-dbt/bin/pip install -r requirements-dbt.txt
set -a && . ./.env && set +a
cd dbt
../.venv-dbt/bin/dbt build --profiles-dir .   # build gold and run its tests
../.venv-dbt/bin/dbt test --profiles-dir .    # tests only, on the existing gold
```

### Run tests

```bash
.venv/bin/python -m pytest
.venv/bin/ruff check .
```

## Evaluation

Results will be reported here as the pipeline matures. The project is measured on two fronts:

- **Data platform:** volume of records processed, end-to-end daily run time, data quality test coverage, and idempotency of re-runs
- **Incremental learning:** average mAP across tasks, and mAP on old topics after the final task (to quantify forgetting) compared with a retrain-from-scratch baseline

## Design decisions & trade-offs

- **Polars over Pandas:** lazy evaluation and lower memory usage; handles millions of rows on a single machine
- **DuckDB/Athena over Spark:** at this data scale, a distributed engine adds overhead without benefit. Spark would be reconsidered at 100× scale
- **SQL (dbt) for table-to-table transforms, Python for procedural logic:** API calls, JSON parsing and abstract reconstruction in Python; modeling and tests in SQL
- **Self-hosted Airflow over MWAA:** managed Airflow is cost-prohibitive for a personal project
- **Precomputed embeddings:** decouples expensive GPU work from frequent retraining

## What would change at 100× scale

- Replace single-node Polars with Spark or distributed processing
- Move from scheduled batch to event-driven ingestion
- Partition gold tables by publication year and field for query pruning
- Run Airflow on Kubernetes with autoscaling workers
- Use a dedicated feature store for embeddings

## Limitations

- Topic labels come from OpenAlex's own automated classifier, not human annotation, so the model learns to approximate another model's labeling
- Each work has at most three topics, so labels are incomplete by nature
- Incremental tasks are simulated by splitting a fixed topic taxonomy, rather than topics genuinely emerging over time

## Acknowledgments

- Data: [OpenAlex](https://openalex.org) (CC0)
- MLCIL method inspiration: Zhang et al., *Knowing Beyond the Known: Reinforced Knowledge Specification for Multi-Label Class-Incremental Learning*

## License

MIT