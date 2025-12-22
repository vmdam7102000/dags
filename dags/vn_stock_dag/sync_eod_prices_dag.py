# dags/stock/sync_eod_prices_dag.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.db_utils import get_all_stock_codes, insert_dynamic_records

CONFIG = load_yaml_config("eod_prices.yml")["eod_prices"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = 400  # keep number of batches under core.max_map_length
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")


def _build_date_range(execution_date: datetime) -> Dict[str, str]:
    """
    Compute from-date and to-date based on execution_date & lookback_days.
    """
    lookback_days = API_CFG.get("lookback_days", 50)
    to_date = execution_date.date()
    from_date = (execution_date - timedelta(days=lookback_days)).date()
    return {
        "from-date": from_date.strftime("%Y-%m-%d"),
        "to-date": to_date.strftime("%Y-%m-%d"),
    }


def _sync_one(code: str, logical_date: datetime, conn) -> None:
    """
    Fetch and insert EOD data for a single code within the lookback window.
    """
    date_range = _build_date_range(logical_date)

    params = {
        "code": code,
        **date_range,
    }
    if API_KEY:
        params["apikey"] = API_KEY

    payload = request_json(
        API_CFG["url"],
        params=params,
        timeout=API_CFG.get("timeout", 30),
    )
    if payload is None:
        logging.warning("No EOD payload for %s", code)
        return

    if isinstance(payload, dict):
        records = payload.get("data", payload)
    else:
        records = payload

    if not records:
        logging.info("No EOD records for %s", code)
        return

    insert_dynamic_records(
        postgres_conn_id=DB_CFG["postgres_conn_id"],
        table=DB_CFG["price_table"],
        records=records,
        columns_map=DB_CFG["columns"],
        conflict_keys=DB_CFG["conflict_keys"],
        on_conflict_do_update=False,
        conn=conn,
    )

    logging.info(
        "Inserted %s EOD records for %s into %s",
        len(records),
        code,
        DB_CFG["price_table"],
    )

    # Basic throttle to avoid hitting the API too aggressively
    time.sleep(1)


with DAG(
    dag_id="sync_eod_vn_stock_prices_dag",
    description="Sync EOD stock prices from Wifeed API to Postgres (YAML mapping)",
    default_args={
        "owner": "vn-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 3 * * *",  # daily at 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "wifeed", "eod"],
) as dag:

    @task
    def get_codes() -> List[str]:
        codes = get_all_stock_codes(
            postgres_conn_id=DB_CFG["postgres_conn_id"],
            stock_list_table=DB_CFG["stock_list_table"],
            code_column=DB_CFG.get("stock_list_code_column", "code"),
        )
        logging.info("Fetched %s stock codes from %s", len(codes), DB_CFG["stock_list_table"])
        return codes

    @task
    def chunk_codes(codes: List[str]) -> List[List[str]]:
        return [codes[i : i + CHUNK_SIZE] for i in range(0, len(codes), CHUNK_SIZE)]

    @task
    def sync_code_batch(codes: List[str]) -> None:
        context = get_current_context()
        logical_date = context["logical_date"]
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for code in codes:
                _sync_one(code, logical_date, conn)
        finally:
            conn.close()

    codes = get_codes()
    code_batches = chunk_codes(codes)
    sync_code_batch.expand(codes=code_batches)
