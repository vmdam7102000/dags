# dags/crypto/sync_crypto_ohlcv_3m_dag_dag.py
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("crypto_configs/ccxt_ohlcv_3m.yml")["ccxt_ohlcv_3m"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]

TIMEFRAME: str = API_CFG.get("timeframe", "3m")
BATCH_LIMIT: int = API_CFG.get("limit", 1000)
SLEEP_FLOOR: float = API_CFG.get("rate_limit_floor", 0.2)
QUOTE = DB_CFG.get("symbol_quote", "USDT")
POOL_NAME: str = API_CFG.get("pool_name", "ccxt_ohlcv_pool")
PAIR_TASK_CONCURRENCY: int = int(API_CFG.get("task_concurrency", 3))

logger = logging.getLogger("ccxt_ohlcv_3m_dag")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _since_from_checkpoint(last_ts_ms: Optional[int]) -> Optional[int]:
    if last_ts_ms is not None:
        return last_ts_ms + 1
    try:
        since_days = int(API_CFG.get("since_days") or 0)
    except (TypeError, ValueError):
        since_days = 0
    if since_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        return int(cutoff.timestamp() * 1000)
    return None


def load_pairs(conn) -> List[Tuple[str, str]]:
    sql = f"""
    SELECT symbol, available_exchange
    FROM {DB_CFG['metadata_table']}
    WHERE available_exchange IS NOT NULL AND available_exchange <> ''
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    pairs: List[Tuple[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for sym, exch_str in rows:
        exchanges = [e.strip() for e in exch_str.split(",") if e.strip()]
        for ex in exchanges:
            pair = (sym.upper(), ex)
            if pair in seen:
                continue
            seen.add(pair)
            pairs.append(pair)
    return pairs


def load_checkpoint(conn, symbol: str, exchange_id: str) -> Optional[int]:
    sql = f"""
    SELECT last_ts_ms FROM {DB_CFG['checkpoint_table']}
    WHERE symbol=%s AND exchange=%s AND timeframe=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, exchange_id, TIMEFRAME))
        row = cur.fetchone()
    return row[0] if row else None


def upsert_checkpoint(conn, symbol: str, exchange_id: str, last_ts_ms: int) -> None:
    sql = f"""
    INSERT INTO {DB_CFG['checkpoint_table']} (symbol, exchange, timeframe, last_ts_ms, updated_at)
    VALUES (%s, %s, %s, %s, now())
    ON CONFLICT (symbol, exchange, timeframe)
    DO UPDATE SET last_ts_ms = EXCLUDED.last_ts_ms, updated_at = now()
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, exchange_id, TIMEFRAME, last_ts_ms))
    conn.commit()


def upsert_ohlcv(conn, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0

    insert_sql = f"""
    INSERT INTO {DB_CFG['target_table']} (
        symbol, exchange, timestamp, open, high, low, close, volume, datetime
    )
    VALUES %s
    ON CONFLICT (symbol, exchange, timestamp) DO UPDATE SET
        open=EXCLUDED.open,
        high=EXCLUDED.high,
        low=EXCLUDED.low,
        close=EXCLUDED.close,
        volume=EXCLUDED.volume,
        datetime=EXCLUDED.datetime
    """
    values = [
        (
            r["symbol"],
            r["exchange"],
            r["timestamp"],
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
            r["datetime"],
        )
        for r in records
    ]
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, values, page_size=1000)
    conn.commit()
    return len(records)


async def _fetch_ohlcv(exchange: ccxt.Exchange, symbol: str, since_ms: Optional[int]) -> List[List[Any]]:
    all_rows: List[List[Any]] = []
    while True:
        rows = await exchange.fetch_ohlcv(
            f"{symbol}/{QUOTE}",
            timeframe=TIMEFRAME,
            since=since_ms,
            limit=BATCH_LIMIT,
        )
        if not rows:
            break
        all_rows.extend(rows)
        since_ms = rows[-1][0] + 1
        await asyncio.sleep(max(exchange.rateLimit / 1000, SLEEP_FLOOR))
        if len(rows) < BATCH_LIMIT:
            break
    return all_rows


async def fetch_for_pair(exchange_id: str, symbol: str, since_ms: Optional[int]) -> List[Dict[str, Any]]:
    exchange_class = getattr(ccxt, exchange_id, None)
    if not exchange_class:
        raise ValueError(f"Exchange {exchange_id} not found in ccxt")
    exchange = exchange_class({"enableRateLimit": True})
    await exchange.load_markets()
    try:
        rows = await _fetch_ohlcv(exchange, symbol, since_ms)
    finally:
        await exchange.close()

    records: List[Dict[str, Any]] = []
    for ts, o, h, l, c, v in rows:
        records.append(
            {
                "symbol": symbol,
                "exchange": exchange_id,
                "timestamp": int(ts),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
                "datetime": datetime.fromtimestamp(ts / 1000, tz=timezone.utc),
            }
        )
    return records


with DAG(
    dag_id="sync_crypto_ohlcv_3m_dag",
    description="Sync OHLCV 3m from CCXT into Postgres (incremental)",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="*/3 * * * *",  # every 3 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    concurrency=PAIR_TASK_CONCURRENCY,
    max_active_runs=1,
    tags=["crypto", "ccxt", "ohlcv", "3m"],
) as dag:

    @task
    def get_pairs() -> List[Dict[str, str]]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            pairs = load_pairs(conn)
            if not pairs:
                raise ValueError("No symbol/exchange pairs found in metadata table")
            logger.info("Loaded %s symbol/exchange pairs to sync", len(pairs))
            return [{"symbol": sym, "exchange": exch} for sym, exch in pairs]
        finally:
            conn.close()

    @task(pool=POOL_NAME)
    def sync_pair(pair: Dict[str, str]) -> None:
        symbol = pair["symbol"]
        exchange_id = pair["exchange"]

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            last_ts_ms = load_checkpoint(conn, symbol, exchange_id)
            since_ms = _since_from_checkpoint(last_ts_ms)
            logger.info("Fetching %s %s since %s", exchange_id, symbol, since_ms)

            records = asyncio.run(fetch_for_pair(exchange_id, symbol, since_ms))
            if not records:
                logger.info("No new data for %s %s", exchange_id, symbol)
                return

            inserted = upsert_ohlcv(conn, records)
            upsert_checkpoint(conn, symbol, exchange_id, records[-1]["timestamp"])
            logger.info("Upserted %s rows for %s %s", inserted, exchange_id, symbol)
        finally:
            conn.close()

    sync_pair.expand(pair=get_pairs())
