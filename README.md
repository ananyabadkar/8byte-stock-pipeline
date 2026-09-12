# Dockerized Stock Market Data Pipeline (Airflow)

Fetches daily stock prices from the [Alpha Vantage](https://www.alphavantage.co/) API on a
scheduled basis, parses the JSON response, and upserts the results into a PostgreSQL table —
orchestrated by Apache Airflow, fully Dockerized.

## Architecture

```
                 ┌────────────────────┐
                 │   Alpha Vantage     │
                 │   (stock JSON API)  │
                 └─────────┬───────────┘
                           │ requests (with retries)
                           ▼
┌──────────────────────────────────────────┐        ┌─────────────────────┐
│  Airflow (Docker)                         │        │  PostgreSQL (Docker) │
│  - Scheduler runs "stock_market_pipeline" │  psycopg2   - airflow  (metadata)│
│    DAG hourly                             │───────▶│  - stockdata (app data)│
│  - PythonOperator calls                   │        │      stock_prices     │
│    scripts/fetch_stock_data.py            │        │      stock_fetch_log  │
└──────────────────────────────────────────┘        └─────────────────────┘
```

One Postgres container serves two logical databases: `airflow` (Airflow's own metadata) and
`stockdata` (the table this pipeline updates) — kept separate so pipeline data is never mixed
with orchestrator internals.

## Project layout

```
.
├── docker-compose.yml         # Brings up Postgres + Airflow (init, webserver, scheduler)
├── Dockerfile                 # Extends the official Airflow image with our deps
├── requirements.txt           # requests, psycopg2-binary
├── .env.example                # Template for required environment variables
├── dags/
│   └── stock_pipeline_dag.py  # Airflow DAG: schedule + retry policy
├── scripts/
│   └── fetch_stock_data.py    # Fetch -> validate -> upsert logic (importable + standalone)
├── sql/
│   └── create_table.sql       # Creates stock_prices / stock_fetch_log on first boot
└── init-db/
    └── init-airflow-db.sh     # Creates the separate "airflow" database on first boot
```

## Prerequisites

- Docker and Docker Compose installed
- A free Alpha Vantage API key: https://www.alphavantage.co/support/#api-key

## Setup

1. Copy the env template and fill in your values:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` and set `ALPHA_VANTAGE_API_KEY`, `STOCK_SYMBOLS`, and strong values for the
   Postgres/Airflow passwords. `.env` is gitignored — never commit real credentials.

2. Build and start everything with a single command:
   ```bash
   docker compose up --build
   ```
   This will:
   - Start Postgres and create both the `airflow` and `stockdata` databases
   - Run `airflow-init` once to migrate the metadata DB and create an admin user
   - Start the Airflow webserver (http://localhost:8080) and scheduler

3. Log in to the Airflow UI at `http://localhost:8080` using `AIRFLOW_ADMIN_USER` /
   `AIRFLOW_ADMIN_PASSWORD` from your `.env`. The `stock_market_pipeline` DAG is unpaused by
   default and runs hourly (`@hourly` in `dags/stock_pipeline_dag.py`) — you can also trigger it
   manually from the UI for an immediate run.

4. Inspect the results directly in Postgres:
   ```bash
   docker compose exec postgres psql -U "$POSTGRES_USER" -d stockdata \
     -c "SELECT * FROM stock_prices ORDER BY fetched_at DESC LIMIT 20;"
   docker compose exec postgres psql -U "$POSTGRES_USER" -d stockdata \
     -c "SELECT * FROM stock_fetch_log ORDER BY run_ts DESC LIMIT 20;"
   ```

5. Shut everything down:
   ```bash
   docker compose down          # stop containers, keep data
   docker compose down -v       # also wipe the Postgres volume
   ```

## How the pipeline works

- **Schedule**: the DAG runs `@hourly` (edit `schedule_interval` in `stock_pipeline_dag.py` for
  daily/other cadences).
- **Fetch**: `fetch_daily_series()` calls Alpha Vantage's `TIME_SERIES_DAILY` endpoint per symbol,
  with up to 3 retries and exponential backoff on network errors, and a short delay between
  symbols to respect the free-tier rate limit (5 requests/minute).
- **Parse**: `parse_daily_series()` validates the response shape before touching the DB. It
  distinguishes three failure modes so they're diagnosable from `stock_fetch_log`:
  - `API_ERROR` — the request itself failed (no response)
  - `NO_DATA` — the API responded but had an error message, a rate-limit note, or an empty/
    missing time series
  - Malformed individual daily records are skipped (not the whole run) and logged
- **Store**: `upsert_rows()` uses `INSERT ... ON CONFLICT (symbol, trade_date) DO UPDATE`, so
  re-running the DAG (or Airflow's own retries) is idempotent — no duplicate rows.
- **Error handling / resilience**:
  - Airflow-level: the DAG's `default_args` retry a failed task 3 times with exponential backoff
    before it's marked failed.
  - Script-level: try/except around the API call, the JSON parsing, and the DB write
    independently, so one bad symbol or one bad daily record doesn't abort the whole run.
  - Every run's outcome per symbol is written to `stock_fetch_log` for auditability.
- **Secrets**: `ALPHA_VANTAGE_API_KEY` and all Postgres credentials are read only from
  environment variables (via `.env` / Docker Compose), never hard-coded.
- **Scalability**: adding more symbols is a one-line change to `STOCK_SYMBOLS` in `.env`; moving
  to a higher-throughput setup (e.g. `CeleryExecutor` with multiple workers, or a paid API tier
  with higher rate limits) doesn't require changing the DAG or script logic.

## Extending

- Swap `TIME_SERIES_DAILY` for `TIME_SERIES_INTRADAY` in `fetch_stock_data.py` for finer-grained
  data if running more frequently than hourly.
- To use Yahoo Finance instead, replace `fetch_daily_series()`'s request/parsing logic — the
  DAG and upsert logic are unaffected since they operate on the same `(symbol, trade_date, ...)`
  row shape.
