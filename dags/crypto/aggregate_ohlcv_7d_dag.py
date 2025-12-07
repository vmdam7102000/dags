# dags/crypto/aggregate_crypto_ohlcv_7d_dag_dag.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("ohlcv_7d.yml")["ohlcv_7d"]
DB_CFG = CONFIG["db"]
LOOKBACK_DAYS: int = int(DB_CFG.get("lookback_days", 365))
BATCH_DAYS: int = max(1, int(DB_CFG.get("batch_days", 90)))
CONFLICT_KEYS = DB_CFG.get("conflict_keys", ["symbol", "exchange", "timestamp"])
SOURCE_TABLE = DB_CFG["source_table"]
TARGET_TABLE = DB_CFG["target_table"]
BUCKET_MINUTES = int(DB_CFG.get("bucket_minutes", 10080))
BUCKET_MS = BUCKET_MINUTES * 60 * 1000
SCHEDULE = CONFIG.get("schedule", "30 3 * * 0")

logger = logging.getLogger("aggregate_crypto_ohlcv_7d_dag")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _bucket_start_ts(ts_ms: int) -> int:
    return (ts_ms // BUCKET_MS) * BUCKET_MS


def _parse_conf_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _ensure_utc(dt_value: datetime) -> datetime:
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc)


def aggregate_window(conn, start_ts_ms: int, end_ts_ms: int) -> int:
    sql = f"""
    WITH base AS (
        SELECT
            symbol,
            exchange,
            ({SOURCE_TABLE}.timestamp / {BUCKET_MS}) * {BUCKET_MS} AS bucket_ts,
            {SOURCE_TABLE}.timestamp AS ts,
            open,
            high,
            low,
            close,
            volume,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, exchange, ({SOURCE_TABLE}.timestamp / {BUCKET_MS})
                ORDER BY {SOURCE_TABLE}.timestamp ASC
            ) AS rn_asc,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, exchange, ({SOURCE_TABLE}.timestamp / {BUCKET_MS})
                ORDER BY {SOURCE_TABLE}.timestamp DESC
            ) AS rn_desc
        FROM {SOURCE_TABLE}
        WHERE {SOURCE_TABLE}.timestamp >= %s
          AND {SOURCE_TABLE}.timestamp < %s
    ),
    aggregated AS (
        SELECT
            symbol,
            exchange,
            bucket_ts AS timestamp,
            MAX(open) FILTER (WHERE rn_asc = 1) AS open,
            MAX(high) AS high,
            MIN(low) AS low,
            MAX(close) FILTER (WHERE rn_desc = 1) AS close,
            SUM(volume) AS volume,
            to_timestamp(bucket_ts / 1000)::timestamptz AS datetime
        FROM base
        GROUP BY symbol, exchange, bucket_ts
    )
    INSERT INTO {TARGET_TABLE} (
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    )
    SELECT
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    FROM aggregated
    ON CONFLICT ({", ".join(CONFLICT_KEYS)})
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        datetime = EXCLUDED.datetime;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (start_ts_ms, end_ts_ms))
        inserted = cursor.rowcount
    conn.commit()
    return inserted


with DAG(
    dag_id="aggregate_crypto_ohlcv_7d_dag",
    description="Aggregate 3m OHLCV candles into 7d timeframe",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "ohlcv", "aggregation"],
) as dag:

    @task
    def aggregate() -> Dict[str, int]:
        context = get_current_context()
        logical_date = _ensure_utc(context["logical_date"])

        dag_run = context.get("dag_run")
        conf = dict(dag_run.conf or {}) if dag_run and dag_run.conf else {}

        conf_since = _parse_conf_dt(conf.get("since"))
        conf_until = _parse_conf_dt(conf.get("until") or conf.get("end"))

        start_dt_raw = conf_since or (logical_date - timedelta(days=LOOKBACK_DAYS))
        end_dt_raw = conf_until or logical_date
        start_dt = _ensure_utc(start_dt_raw)
        end_dt = _ensure_utc(end_dt_raw)

        if end_dt <= start_dt:
            raise ValueError("End time must be greater than start time for aggregation window")

        start_ts_ms = _bucket_start_ts(int(start_dt.timestamp() * 1000))
        end_ts_ms = _bucket_start_ts(int(end_dt.timestamp() * 1000))
        if end_ts_ms <= start_ts_ms:
            end_ts_ms = start_ts_ms + BUCKET_MS

        batch_ms = max(BATCH_DAYS, 1) * 24 * 60 * 60 * 1000
        total_inserted = 0

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            window_start = start_ts_ms
            while window_start < end_ts_ms:
                window_end = min(window_start + batch_ms, end_ts_ms)
                inserted = aggregate_window(conn, window_start, window_end)
                total_inserted += inserted
                logger.info(
                    "Aggregated %s rows for window %s - %s",
                    inserted,
                    datetime.fromtimestamp(window_start / 1000, tz=timezone.utc),
                    datetime.fromtimestamp(window_end / 1000, tz=timezone.utc),
                )
                window_start = window_end

            logger.info(
                "Total aggregated rows: %s for range %s - %s",
                total_inserted,
                datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc),
                datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc),
            )
            return {"inserted": total_inserted}
        finally:
            conn.close()

    aggregate()
