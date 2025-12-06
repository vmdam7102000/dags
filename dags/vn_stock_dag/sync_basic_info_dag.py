# dags/stock/sync_basic_info_dag.py
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from vn_stock_data.api_utils import request_json
from vn_stock_data.config_loader import load_yaml_config
from vn_stock_data.db_utils import get_all_stock_codes, insert_dynamic_records

CONFIG = load_yaml_config("stock_basic_info.yml")["stock_basic_info"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = 400
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")


def _normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Chuẩn hóa record trước khi insert: json.dumps cho trường donvikiemtoan nếu không phải string.
    """
    normalized = dict(rec)
    dvkt = normalized.get("donvikiemtoan")
    if dvkt is not None and not isinstance(dvkt, str):
        try:
            normalized["donvikiemtoan"] = json.dumps(dvkt)
        except Exception:
            normalized["donvikiemtoan"] = None
    return normalized


def _fetch_basic_info(code: str) -> Dict[str, Any] | None:
    params = {"code": code}
    if API_KEY:
        params["apikey"] = API_KEY

    payload = request_json(
        API_CFG["url"],
        params=params,
        timeout=API_CFG.get("timeout", 30),
    )
    if payload is None:
        logging.warning("No basic info payload for %s", code)
        return None

    record: Dict[str, Any] | None = None
    if isinstance(payload, dict):
        if "data" in payload:
            data = payload.get("data")
            if isinstance(data, list) and data:
                record = data[0]
            elif isinstance(data, dict):
                record = data
        else:
            record = payload
    elif isinstance(payload, list) and payload:
        first = payload[0]
        record = first if isinstance(first, dict) else None

    if not record:
        logging.info("No basic info record for %s", code)
        return None

    return _normalize_record(record)


with DAG(
    dag_id="sync_stock_basic_info",
    description="Sync stock basic info from API to Postgres",
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 3 * * *",  # daily at 03:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "basic-info", "wifeed"],
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
                record = _fetch_basic_info(code)
                if not record:
                    continue

                insert_dynamic_records(
                    postgres_conn_id=DB_CFG["postgres_conn_id"],
                    table=DB_CFG["target_table"],
                    records=[record],
                    columns_map=DB_CFG["columns"],
                    conflict_keys=DB_CFG["conflict_keys"],
                    on_conflict_do_update=True,
                    conn=conn,
                )

                time.sleep(API_CFG.get("throttle_seconds", 1))
        finally:
            conn.close()

    codes = get_codes()
    code_batches = chunk_codes(codes)
    sync_code_batch.expand(codes=code_batches)
