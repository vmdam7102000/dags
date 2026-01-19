# dags/crypto/aggregate_crypto_ohlcv_1d_7d_xmr_backdate_dag.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.config_loader import load_yaml_config

CONFIG_1D = load_yaml_config("crypto_configs/ohlcv_1d.yml")["ohlcv_1d"]
CONFIG_7D = load_yaml_config("crypto_configs/ohlcv_7d.yml")["ohlcv_7d"]

DB_CFG_1D = CONFIG_1D["db"]
DB_CFG_7D = CONFIG_7D["db"]

LOOKBACK_DAYS_1D: int = int(DB_CFG_1D.get("lookback_days", 90))
BATCH_DAYS_1D: int = max(1, int(DB_CFG_1D.get("batch_days", 30)))
CONFLICT_KEYS_1D = DB_CFG_1D.get("conflict_keys", ["symbol", "exchange", "timestamp"])
BUCKET_MINUTES_1D = int(DB_CFG_1D.get("bucket_minutes", 1440))

LOOKBACK_DAYS_7D: int = int(DB_CFG_7D.get("lookback_days", 365))
BATCH_DAYS_7D: int = max(1, int(DB_CFG_7D.get("batch_days", 90)))
CONFLICT_KEYS_7D = DB_CFG_7D.get("conflict_keys", ["symbol", "exchange", "timestamp"])
BUCKET_MINUTES_7D = int(DB_CFG_7D.get("bucket_minutes", 10080))

SYMBOL = "XMR"
SCHEDULE = None

logger = logging.getLogger("aggregate_crypto_ohlcv_1d_7d_xmr_backdate_dag")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _bucket_start_ts(ts_ms: int, bucket_ms: int) -> int:
    return (ts_ms // bucket_ms) * bucket_ms


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


def aggregate_window(
    conn,
    start_ts_ms: int,
    end_ts_ms: int,
    source_table: str,
    target_table: str,
    conflict_keys,
    bucket_ms: int,
    symbol: str,
) -> int:
    sql = f"""
    WITH base AS (
        SELECT
            symbol,
            exchange,
            ({source_table}.timestamp / {bucket_ms}) * {bucket_ms} AS bucket_ts,
            {source_table}.timestamp AS ts,
            open,
            high,
            low,
            close,
            volume,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, exchange, ({source_table}.timestamp / {bucket_ms})
                ORDER BY {source_table}.timestamp ASC
            ) AS rn_asc,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, exchange, ({source_table}.timestamp / {bucket_ms})
                ORDER BY {source_table}.timestamp DESC
            ) AS rn_desc
        FROM {source_table}
        WHERE {source_table}.timestamp >= %s
          AND {source_table}.timestamp < %s
          AND {source_table}.symbol = %s
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
    INSERT INTO {target_table} (
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    )
    SELECT
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    FROM aggregated
    ON CONFLICT ({", ".join(conflict_keys)})
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        datetime = EXCLUDED.datetime;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (start_ts_ms, end_ts_ms, symbol))
        inserted = cursor.rowcount
    conn.commit()
    return inserted


def _run_aggregate(
    db_cfg,
    lookback_days: int,
    batch_days: int,
    conflict_keys,
    bucket_minutes: int,
    label: str,
) -> Dict[str, int]:
    context = get_current_context()
    logical_date = _ensure_utc(context["logical_date"])

    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf or {}) if dag_run and dag_run.conf else {}

    conf_since = _parse_conf_dt(conf.get("since"))
    conf_until = _parse_conf_dt(conf.get("until") or conf.get("end"))

    start_dt_raw = conf_since or (logical_date - timedelta(days=lookback_days))
    end_dt_raw = conf_until or logical_date
    start_dt = _ensure_utc(start_dt_raw)
    end_dt = _ensure_utc(end_dt_raw)

    if end_dt <= start_dt:
        raise ValueError("End time must be greater than start time for aggregation window")

    bucket_ms = bucket_minutes * 60 * 1000
    start_ts_ms = _bucket_start_ts(int(start_dt.timestamp() * 1000), bucket_ms)
    end_ts_ms = _bucket_start_ts(int(end_dt.timestamp() * 1000), bucket_ms)
    if end_ts_ms <= start_ts_ms:
        end_ts_ms = start_ts_ms + bucket_ms

    batch_ms = max(batch_days, 1) * 24 * 60 * 60 * 1000
    total_inserted = 0

    hook = PostgresHook(postgres_conn_id=db_cfg["postgres_conn_id"])
    conn = hook.get_conn()
    try:
        window_start = start_ts_ms
        while window_start < end_ts_ms:
            window_end = min(window_start + batch_ms, end_ts_ms)
            inserted = aggregate_window(
                conn,
                window_start,
                window_end,
                db_cfg["source_table"],
                db_cfg["target_table"],
                conflict_keys,
                bucket_ms,
                SYMBOL,
            )
            total_inserted += inserted
            logger.info(
                "[%s] Aggregated %s rows for %s window %s - %s",
                label,
                inserted,
                SYMBOL,
                datetime.fromtimestamp(window_start / 1000, tz=timezone.utc),
                datetime.fromtimestamp(window_end / 1000, tz=timezone.utc),
            )
            window_start = window_end

        logger.info(
            "[%s] Total aggregated rows: %s for %s range %s - %s",
            label,
            total_inserted,
            SYMBOL,
            datetime.fromtimestamp(start_ts_ms / 1000, tz=timezone.utc),
            datetime.fromtimestamp(end_ts_ms / 1000, tz=timezone.utc),
        )
        return {"inserted": total_inserted}
    finally:
        conn.close()


with DAG(
    dag_id="aggregate_crypto_ohlcv_1d_7d_xmr_backdate_dag",
    description="Backdate aggregate 3m OHLCV candles into 1d/7d timeframe for XMR only",
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
    tags=["crypto", "ohlcv", "aggregation", "backdate", "xmr"],
) as dag:

    @task(task_id="aggregate_1d")
    def aggregate_1d() -> Dict[str, int]:
        return _run_aggregate(
            DB_CFG_1D,
            LOOKBACK_DAYS_1D,
            BATCH_DAYS_1D,
            CONFLICT_KEYS_1D,
            BUCKET_MINUTES_1D,
            "1d",
        )

    @task(task_id="aggregate_7d")
    def aggregate_7d() -> Dict[str, int]:
        return _run_aggregate(
            DB_CFG_7D,
            LOOKBACK_DAYS_7D,
            BATCH_DAYS_7D,
            CONFLICT_KEYS_7D,
            BUCKET_MINUTES_7D,
            "7d",
        )

    aggregate_1d()
    aggregate_7d()
