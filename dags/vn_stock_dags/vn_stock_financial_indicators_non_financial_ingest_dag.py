# dags/vn_stock_dag/sync_financial_indicators_non_financial_dag.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.db_utils import get_all_stock_codes, insert_dynamic_records

CONFIG = load_yaml_config("vn_stock_configs/financial_indicators_non_financial.yml")[
    "financial_indicators_non_financial"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = 400  # keep number of batches under core.max_map_length
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")


def _build_columns_map(columns: Sequence[Any]) -> List[Dict[str, str]]:
    if not columns:
        return []
    if isinstance(columns[0], dict):
        return list(columns)
    return [{"json_key": str(col), "column": str(col)} for col in columns]


COLUMNS_MAP = _build_columns_map(DB_CFG["columns"])


def _normalize_records(records: List[Dict[str, Any]], code: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        if not rec.get("mack"):
            rec = dict(rec)
            rec["mack"] = code
        normalized.append(rec)
    return normalized


def _fetch_financial_indicators(code: str) -> List[Dict[str, Any]]:
    params = {
        "code": code,
        "type": API_CFG.get("type", "ttm"),
    }
    if API_KEY:
        params["apikey"] = API_KEY

    payload = request_json(
        API_CFG["url"],
        params=params,
        timeout=API_CFG.get("timeout", 30),
    )
    if payload is None:
        logging.warning("No financial indicators payload for %s", code)
        return []

    if isinstance(payload, dict):
        records = payload.get("data", payload)
    else:
        records = payload

    if not records:
        logging.info("No financial indicators records for %s", code)
        return []

    if isinstance(records, dict):
        return _normalize_records([records], code)
    if isinstance(records, list):
        return _normalize_records([r for r in records if isinstance(r, dict)], code)

    logging.info("Unexpected financial indicators payload type for %s", code)
    return []


with DAG(
    dag_id="sync_financial_indicators_non_financial",
    description="Sync financial indicators (TTM) from Wifeed API to Postgres",
    default_args={
        "owner": "vn-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 4 * * *",  # daily at 04:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "wifeed", "financial-indicators"],
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
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for code in codes:
                records = _fetch_financial_indicators(code)
                if not records:
                    time.sleep(API_CFG.get("throttle_seconds", 1))
                    continue

                insert_dynamic_records(
                    postgres_conn_id=DB_CFG["postgres_conn_id"],
                    table=DB_CFG["target_table"],
                    records=records,
                    columns_map=COLUMNS_MAP,
                    conflict_keys=DB_CFG["conflict_keys"],
                    on_conflict_do_update=DB_CFG.get("on_conflict_do_update", True),
                    conn=conn,
                )

                logging.info(
                    "Inserted %s financial indicator rows for %s into %s",
                    len(records),
                    code,
                    DB_CFG["target_table"],
                )

                time.sleep(API_CFG.get("throttle_seconds", 1))
        finally:
            conn.close()

    codes = get_codes()
    code_batches = chunk_codes(codes)
    sync_code_batch.expand(codes=code_batches)
