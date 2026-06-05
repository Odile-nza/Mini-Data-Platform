# Mini Data Platform

A fully-containerised data platform covering the complete data lifecycle: **Ingestion → Processing → Storage → Visualisation**.

## Architecture

| Service | Technology | Port | Purpose |
|---------|-----------|------|---------|
| Database | PostgreSQL 15 | 5432 | Structured storage for processed sales data |
| Processing | Apache Airflow 2.9 | 8088 | Pipeline orchestration |
| Object Storage | MinIO | 9000 / 9001 | S3-compatible raw CSV ingestion |
| Dashboards | Metabase v0.49 | 3000 | Business intelligence & KPI dashboards |

```
data_generator → MinIO (raw/) → Airflow DAG → PostgreSQL → Metabase
```

## Quick Start

### Prerequisites

- Docker Engine 24+ and Docker Compose v2.20+
- Python 3.11+ (for the data generator)

### 1. Clone and configure

```bash
cp .env.example .env
# Edit .env and fill in AIRFLOW_FERNET_KEY (see below)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 2. Start the platform

```bash
docker compose up -d
```

Airflow initialises its database on first boot (`airflow-init` container). Wait ~2 minutes, then verify all services are healthy:

```bash
docker compose ps
```

### 3. Generate test data

```bash
pip install -r data_generator/requirements.txt
MINIO_ENDPOINT=http://localhost:9000 python data_generator/generate_sales_data.py --rows 500
```

### 4. Trigger the pipeline

Open the Airflow UI at **http://localhost:8080** (admin / admin by default), enable the `sales_pipeline` DAG, and trigger a run — or trigger it via the API:

```bash
curl -X POST http://localhost:8080/api/v1/dags/sales_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{"conf": {}}'
```

### 5. Verify data

```bash
docker compose exec postgres psql -U postgres -d platform \
  -c "SELECT category, COUNT(*) FROM sales GROUP BY 1 ORDER BY 2 DESC;"
```

### 6. Build Metabase dashboard

1. Open **http://localhost:3000** and complete the setup wizard.
2. Add a PostgreSQL data source:
   - Host: `postgres` (service name, not `localhost`)
   - Port: `5432`
   - Database: `platform`
   - Username: `platform_user` / Password: `platform_pass`
3. Create a dashboard with recommended KPIs:
   - Total revenue by category (bar chart)
   - Daily sales trend (line chart)
   - Revenue by region (map / pie)
   - Top 10 products by revenue (table)

### MinIO Console

Browse raw and processed CSV files at **http://localhost:9001** (minioadmin / minioadmin).

---

## Repository Layout

```
├── dags/
│   └── sales_pipeline.py       # ETL DAG: MinIO → clean → PostgreSQL
├── data_generator/
│   ├── generate_sales_data.py  # Synthetic sales data with dirty rows
│   └── requirements.txt
├── config/
│   ├── postgres/
│   │   └── init.sql            # Creates users, databases, sales table
│   └── airflow/
│       └── requirements.txt    # boto3, pandas, psycopg2-binary
├── .github/
│   └── workflows/
│       └── main.yml            # CI lint → build → integration test → deploy
├── Dockerfile.airflow          # Custom Airflow image with extra packages
├── docker-compose.yml
├── .env.example
└── .gitignore
```

## Pipeline Detail

The `sales_pipeline` DAG runs every hour:

1. **list_new_files** — Lists `raw/*.csv` objects in the `sales-data` MinIO bucket. Short-circuits if nothing to process.
2. **process_and_load** — Downloads each CSV, cleans data (drops null required fields, coerces types, normalises whitespace), and upserts rows into `public.sales` using `ON CONFLICT (sale_id) DO UPDATE` for idempotency.
3. **archive_processed_files** — Moves processed CSVs from `raw/` to `processed/` within the bucket.

## CI/CD

The GitHub Actions workflow (`.github/workflows/main.yml`) runs on every push:

| Job | Trigger | Steps |
|-----|---------|-------|
| **lint** | all branches | flake8 on Python files, hadolint on Dockerfile, `docker compose config` |
| **build** | all branches | Build Airflow image (cached), pull service images |
| **integration-test** | `main` only | Full stack up, generate data, trigger DAG, verify PostgreSQL rows, check Metabase `/api/health` |
| **deploy** | `main` push | SSH deploy to test server (requires `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY` secrets) |

## Environment Variables

See `.env.example`. All variables have safe defaults for local development.

| Variable | Description |
|----------|-------------|
| `AIRFLOW_FERNET_KEY` | Encryption key for Airflow connections (generate once, keep stable) |
| `AIRFLOW_ADMIN_PASSWORD` | Airflow web UI admin password |
| `POSTGRES_PASSWORD` | PostgreSQL superuser password |
| `MINIO_ROOT_USER` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | MinIO secret key |
| `AIRFLOW_UID` | UID for Airflow container user (use `id -u` on Linux) |

## Stopping and Resetting

```bash
# Stop without removing data
docker compose down

# Stop and delete all volumes (full reset)
docker compose down -v
```
