# dags/global_stock/sync_global_eod_prices_dag.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.db_utils import insert_dynamic_records

CONFIG = load_yaml_config("global_stock_configs/global_eod_prices.yml")["global_eod_prices"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = 400  # keep number of batches under core.max_map_length
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")


def _build_date_range(execution_date: datetime) -> Dict[str, str]:
    """
    Compute from and to dates based on execution_date & lookback_days.
    """
    lookback_days = API_CFG.get("lookback_days", 30)
    to_date = execution_date.date()
    from_date = (execution_date - timedelta(days=lookback_days)).date()
    from_param = API_CFG.get("from_param", "from")
    to_param = API_CFG.get("to_param", "to")
    return {
        from_param: from_date.strftime("%Y-%m-%d"),
        to_param: to_date.strftime("%Y-%m-%d"),
    }


def _enrich_records(
    records: List[Dict[str, Any]],
    company_id: int,
    ticker: str,
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        row = dict(rec)
        row["company_id"] = company_id
        row["ticker"] = ticker
        enriched.append(row)
    return enriched


def _sync_one(ticker: str, company_id: int, logical_date: datetime, conn) -> None:
    """
    Fetch and insert EOD data for a single ticker within the lookback window.
    """
    date_range = _build_date_range(logical_date)
    params = {
        "fmt": API_CFG.get("fmt", "json"),
        **date_range,
    }
    if API_KEY:
        params["api_token"] = API_KEY

    url = API_CFG["url"].format(ticker=ticker)
    payload = request_json(
        url,
        params=params,
        timeout=API_CFG.get("timeout", 30),
    )
    if payload is None:
        logging.warning("No EOD payload for %s", ticker)
        return

    if isinstance(payload, dict):
        if payload.get("message") and payload.get("code"):
            logging.warning("EODHD error for %s: %s", ticker, payload.get("message"))
            return
        records = payload.get("data", payload)
    else:
        records = payload

    if not records:
        logging.info("No EOD records for %s", ticker)
        return
    if not isinstance(records, list):
        logging.warning("Unexpected EOD payload format for %s: %s", ticker, type(records))
        return

    enriched = _enrich_records(records, company_id, ticker)
    if not enriched:
        logging.info("No valid EOD records for %s", ticker)
        return

    insert_dynamic_records(
        postgres_conn_id=DB_CFG["postgres_conn_id"],
        table=DB_CFG["price_table"],
        records=enriched,
        columns_map=DB_CFG["columns"],
        conflict_keys=DB_CFG["conflict_keys"],
        on_conflict_do_update=False,
        conn=conn,
    )

    logging.info(
        "Inserted %s EOD records for %s into %s",
        len(enriched),
        ticker,
        DB_CFG["price_table"],
    )

    # Basic throttle to avoid hitting the API too aggressively
    time.sleep(API_CFG.get("throttle_seconds", 1))


with DAG(
    dag_id="sync_global_eod_stock_prices_dag",
    description="Sync global EOD stock prices from EODHD API to Postgres",
    default_args={
        "owner": "global-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 3 * * *",  # daily at 03:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "global", "eod", "eodhd"],
) as dag:

    @task
    def get_companies() -> List[Dict[str, Any]]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        cursor = conn.cursor()
        id_col = DB_CFG.get("company_id_column", "id")
        ticker_col = DB_CFG.get("company_ticker_column", "ticker")
        table = DB_CFG["company_table"]
        where_clause = ""
        if DB_CFG.get("only_active", True):
            where_clause = " WHERE is_active = TRUE"
        try:
            cursor.execute(f"SELECT {id_col}, {ticker_col} FROM {table}{where_clause}")
            companies = [
                {"company_id": row[0], "ticker": row[1]} for row in cursor.fetchall()
            ]
        finally:
            cursor.close()
            conn.close()
        logging.info("Fetched %s companies from %s", len(companies), table)
        return companies

    @task
    def chunk_companies(companies: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        return [companies[i : i + CHUNK_SIZE] for i in range(0, len(companies), CHUNK_SIZE)]

    @task
    def sync_company_batch(companies: List[Dict[str, Any]]) -> None:
        context = get_current_context()
        logical_date = context["logical_date"]
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for company in companies:
                ticker = company.get("ticker")
                company_id = company.get("company_id")
                if not ticker or company_id is None:
                    continue
                _sync_one(ticker, company_id, logical_date, conn)
        finally:
            conn.close()

    companies = get_companies()
    company_batches = chunk_companies(companies)
    sync_company_batch.expand(companies=company_batches)
