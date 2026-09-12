"""
fetch_stock_data.py

Fetches daily stock price data from the Alpha Vantage API and upserts it
into a PostgreSQL table. Designed to be imported by the Airflow DAG
(dags/stock_pipeline_dag.py) but also runnable standalone:

    python fetch_stock_data.py

All secrets (API key, DB credentials) are read from environment variables
only -- nothing sensitive is hard-coded.
"""

import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import requests
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("stock_pipeline")

ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


# --------------------------------------------------------------------------
# Configuration (all from environment variables -- see README / .env.example)
# --------------------------------------------------------------------------
def get_config() -> Dict[str, str]:
    required = ["ALPHA_VANTAGE_API_KEY", "POSTGRES_HOST", "POSTGRES_DB",
                "POSTGRES_USER", "POSTGRES_PASSWORD"]
    config = {key: os.environ.get(key) for key in required}
    missing = [k for k, v in config.items() if not v]
    if missing:
        raise EnvironmentError(f"Missing required environment variables: {missing}")

    config["POSTGRES_PORT"] = os.environ.get("POSTGRES_PORT", "5432")
    config["STOCK_SYMBOLS"] = os.environ.get("STOCK_SYMBOLS", "IBM,AAPL,MSFT")
    return config


# --------------------------------------------------------------------------
# API interaction
# --------------------------------------------------------------------------
def fetch_daily_series(symbol: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Calls Alpha Vantage TIME_SERIES_DAILY for a symbol, with retries on
    transient network errors. Returns the parsed JSON body, or None if the
    request could not be completed after retries.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact",  # last 100 data points
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                ALPHA_VANTAGE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Attempt %s/%s failed fetching %s: %s", attempt, MAX_RETRIES, symbol, exc
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            else:
                logger.error("Giving up on %s after %s attempts", symbol, MAX_RETRIES)
                return None
    return None


# --------------------------------------------------------------------------
# Parsing / validation
# --------------------------------------------------------------------------
def parse_daily_series(symbol: str, raw: Dict[str, Any]) -> Tuple[List[Tuple], str, str]:
    """
    Validates and extracts rows from the raw API response.

    Returns (rows, status, detail) where status is one of:
      SUCCESS   -- rows extracted successfully (rows may still be empty if
                   the API genuinely returned nothing new)
      NO_DATA   -- API responded but had no usable time series (rate limit
                   note, invalid symbol, empty payload, etc.)
      API_ERROR -- raw response was None (network/API failure upstream)
    """
    if raw is None:
        return [], "API_ERROR", "No response received from Alpha Vantage"

    # Alpha Vantage returns 200 OK even for errors/rate limits, signalled
    # via these keys instead of an HTTP status code -- handle explicitly.
    if "Error Message" in raw:
        return [], "NO_DATA", f"API error: {raw['Error Message']}"
    if "Note" in raw:
        return [], "NO_DATA", f"API rate limit note: {raw['Note']}"
    if "Information" in raw:
        return [], "NO_DATA", f"API info (likely rate-limited): {raw['Information']}"

    series = raw.get("Time Series (Daily)")
    if not series:
        return [], "NO_DATA", "Response missing 'Time Series (Daily)' key"

    rows: List[Tuple] = []
    skipped = 0
    for date_str, values in series.items():
        try:
            trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            open_price = float(values["1. open"])
            high_price = float(values["2. high"])
            low_price = float(values["3. low"])
            close_price = float(values["4. close"])
            volume = int(values["5. volume"])
            rows.append((symbol, trade_date, open_price, high_price, low_price, close_price, volume))
        except (KeyError, ValueError, TypeError) as exc:
            # Missing/malformed data for a single day shouldn't kill the
            # whole run -- skip that day and keep going.
            skipped += 1
            logger.warning("Skipping malformed record for %s on %s: %s", symbol, date_str, exc)
            continue

    if not rows:
        return [], "NO_DATA", f"All {skipped} records were malformed or empty"

    detail = f"Parsed {len(rows)} rows" + (f", skipped {skipped} malformed" if skipped else "")
    return rows, "SUCCESS", detail


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
def get_connection(config: Dict[str, str]):
    return psycopg2.connect(
        host=config["POSTGRES_HOST"],
        port=config["POSTGRES_PORT"],
        dbname=config["POSTGRES_DB"],
        user=config["POSTGRES_USER"],
        password=config["POSTGRES_PASSWORD"],
    )


def upsert_rows(conn, rows: List[Tuple]) -> int:
    """Upserts rows into stock_prices, updating on (symbol, trade_date) conflict."""
    if not rows:
        return 0
    query = """
        INSERT INTO stock_prices
            (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
        VALUES %s
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            open_price  = EXCLUDED.open_price,
            high_price  = EXCLUDED.high_price,
            low_price   = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume      = EXCLUDED.volume,
            fetched_at  = NOW();
    """
    with conn.cursor() as cur:
        execute_values(cur, query, rows)
    conn.commit()
    return len(rows)


def log_result(conn, symbol: str, status: str, rows_upserted: int, detail: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO stock_fetch_log (symbol, status, rows_upserted, detail)
            VALUES (%s, %s, %s, %s)
            """,
            (symbol, status, rows_upserted, detail),
        )
    conn.commit()


# --------------------------------------------------------------------------
# Orchestration entry point (called by the Airflow task)
# --------------------------------------------------------------------------
def run_pipeline(symbols: Optional[List[str]] = None) -> Dict[str, str]:
    config = get_config()
    symbol_list = symbols or [s.strip() for s in config["STOCK_SYMBOLS"].split(",") if s.strip()]

    results: Dict[str, str] = {}
    conn = None
    try:
        conn = get_connection(config)
    except psycopg2.OperationalError as exc:
        logger.error("Could not connect to database: %s", exc)
        raise

    try:
        for symbol in symbol_list:
            logger.info("Fetching %s...", symbol)
            try:
                raw = fetch_daily_series(symbol, config["ALPHA_VANTAGE_API_KEY"])
                rows, status, detail = parse_daily_series(symbol, raw)
                rows_upserted = 0
                if status == "SUCCESS":
                    try:
                        rows_upserted = upsert_rows(conn, rows)
                    except psycopg2.Error as db_exc:
                        conn.rollback()
                        status, detail = "DB_ERROR", f"DB error during upsert: {db_exc}"
                        logger.error(detail)

                log_result(conn, symbol, status, rows_upserted, detail)
                results[symbol] = status
                logger.info("%s -> %s (%s)", symbol, status, detail)

            except Exception as exc:  # noqa: BLE001 - one bad symbol must not kill the run
                logger.exception("Unexpected error processing %s", symbol)
                try:
                    log_result(conn, symbol, "API_ERROR", 0, str(exc))
                except psycopg2.Error:
                    conn.rollback()
                results[symbol] = "API_ERROR"

            # Alpha Vantage free tier is rate-limited (5 req/min) -- be polite.
            time.sleep(12)
    finally:
        conn.close()

    return results


if __name__ == "__main__":
    outcome = run_pipeline()
    logger.info("Pipeline run complete: %s", outcome)
