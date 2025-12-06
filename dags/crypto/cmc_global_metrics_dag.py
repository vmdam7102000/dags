# dags/crypto/cmc_global_metrics_dag.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from vn_stock_data.api_utils import request_json
from vn_stock_data.config_loader import load_yaml_config
from vn_stock_data.db_utils import insert_dynamic_records

CONFIG = load_yaml_config("cmc_global_metrics.yml")["cmc_global_metrics"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")
COLUMN_MAPPINGS = DB_CFG["columns"]


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_from_path(entry: Dict[str, Any], path: List[str]) -> Any:
    value: Any = entry
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
        if value is None:
            return None
    return value


def _normalize_timestamp(ts: Any) -> Optional[int]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    dt_utc = dt.astimezone(timezone.utc)
    normalized_dt = datetime(
        dt_utc.year,
        dt_utc.month,
        dt_utc.day,
        tzinfo=timezone.utc,
    )
    return int(normalized_dt.timestamp() * 1000)


def _normalize_quote(entry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    record: Dict[str, Any] = {}
    for mapping in COLUMN_MAPPINGS:
        json_key = mapping["json_key"]
        path = mapping.get("source_path") or [json_key]
        raw_value = _extract_from_path(entry, path)

        if mapping.get("value_type") == "float":
            record[json_key] = _safe_float(raw_value)
        else:
            record[json_key] = raw_value

    if "timestamp" in record:
        record["timestamp"] = _normalize_timestamp(record["timestamp"])
    if "quote_timestamp" in record:
        record["quote_timestamp"] = _normalize_timestamp(record["quote_timestamp"])

    if not record.get("timestamp"):
        return None
    return record


with DAG(
    dag_id="cmc_global_metrics",
    description="Sync CoinMarketCap global metrics historical quotes",
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 5 * * *",  # daily at 05:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "cmc", "metrics"],
) as dag:

    @task
    def extract() -> Dict[str, Any]:
        if not API_KEY:
            raise ValueError("Missing API key: configure Variable cmc_api_key")

        params = {
            "interval": API_CFG.get("interval", "1d"),
            "count": API_CFG.get("count", 365),
        }
        headers = {
            "X-CMC_PRO_API_KEY": API_KEY,
            "Accept": "application/json",
        }

        payload = request_json(
            API_CFG["url"],
            params=params,
            headers=headers,
            timeout=API_CFG.get("timeout", 30),
        )
        if payload is None:
            raise ValueError("No payload returned from CMC global metrics API")

        status = payload.get("status", {})
        if status.get("error_message"):
            raise ValueError(f"CMC API error: {status.get('error_message')}")

        quotes = (payload.get("data") or {}).get("quotes") or []
        logging.info("Fetched %s global metric quotes", len(quotes))
        return payload

    @task
    def transform(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        quotes = (payload.get("data") or {}).get("quotes") or []
        records: List[Dict[str, Any]] = []

        for entry in quotes:
            record = _normalize_quote(entry)
            if record:
                records.append(record)

        logging.info("Transformed %s records for cmc_global_metrics", len(records))
        return records

    @task
    def load(records: List[Dict[str, Any]]) -> None:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            if DB_CFG.get("truncate_before_load"):
                with conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {DB_CFG['target_table']}")
                conn.commit()

            if not records:
                logging.warning("No global metric records to load")
                return

            insert_dynamic_records(
                postgres_conn_id=DB_CFG["postgres_conn_id"],
                table=DB_CFG["target_table"],
                records=records,
                columns_map=DB_CFG["columns"],
                conflict_keys=DB_CFG["conflict_keys"],
                on_conflict_do_update=True,
                conn=conn,
            )

            logging.info("Loaded %s rows into %s", len(records), DB_CFG["target_table"])
        finally:
            conn.close()

    load(transform(extract()))
