# dags/stock/sync_stock_list_dag.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from vn_stock_data.api_utils import request_json
from vn_stock_data.config_loader import load_yaml_config
from vn_stock_data.db_utils import insert_dynamic_records

CONFIG = load_yaml_config("stock_list.yml")["stock_list"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]


def _fetch_exchange(exchange: str) -> List[Dict[str, Any]]:
    payload = request_json(
        API_CFG["url"],
        params={"san": exchange},
        timeout=API_CFG.get("timeout", 30),
    )
    if not isinstance(payload, dict):
        logging.warning("No payload returned for %s", exchange)
        return []

    data = payload.get("data", []) or []
    logging.info("Fetched %s stocks from %s", len(data), exchange)
    return data


with DAG(
    dag_id="sync_stock_list",
    description="Sync stock list from Wifeed API to Postgres (YAML mapping)",
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 1 * * *",  # daily at 01:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "wifeed", "sync", "stock_list"],
) as dag:

    @task
    def fetch_stock_list() -> List[Dict[str, Any]]:
        all_stocks: List[Dict[str, Any]] = []
        for exchange in API_CFG["exchanges"]:
            all_stocks.extend(_fetch_exchange(exchange))

        if not all_stocks:
            raise ValueError("No stock data fetched from any exchange")

        logging.info("Total stocks fetched: %s", len(all_stocks))
        return all_stocks

    @task
    def upsert_stock_list(records: List[Dict[str, Any]]) -> None:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            insert_dynamic_records(
                postgres_conn_id=DB_CFG["postgres_conn_id"],
                table=DB_CFG["table"],
                records=records,
                columns_map=DB_CFG["columns"],
                conflict_keys=DB_CFG["conflict_keys"],
                on_conflict_do_update=True,
                conn=conn,
            )
            logging.info("Upserted %s stock rows into %s", len(records), DB_CFG["table"])
        finally:
            conn.close()

    data = fetch_stock_list()
    upsert_stock_list(data)
