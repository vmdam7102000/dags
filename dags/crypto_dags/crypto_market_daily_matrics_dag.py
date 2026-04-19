from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("crypto_configs/crypto_market_daily_matrics.yml")[
    "sync_crypto_market_daily_matrics_dag"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
MARKETS_CFG = API_CFG["markets"]
TICKERS_CFG = API_CFG["tickers"]
API_KEY_VAR = API_CFG.get("api_key_var")
API_KEY = Variable.get(API_KEY_VAR, default_var="") if API_KEY_VAR else ""


def _normalize_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coalesce_timestamp(*values: Optional[datetime]) -> Optional[datetime]:
    timestamps = [value for value in values if value is not None]
    return max(timestamps) if timestamps else None


def _chunked(values: Sequence[Dict[str, Any]], size: int) -> Iterable[Sequence[Dict[str, Any]]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _build_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    api_key_header = API_CFG.get("api_key_header")
    if API_KEY and api_key_header:
        headers[api_key_header] = API_KEY
    return headers


def _get_db_identity(conn) -> Dict[str, Any]:
    sql = """
        SELECT
            current_database(),
            current_user,
            current_schema(),
            inet_server_addr()::text,
            inet_server_port()
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
        if not row:
            return {}
        return {
            "database": row[0],
            "user": row[1],
            "schema": row[2],
            "server_addr": row[3],
            "server_port": row[4],
        }


def _load_target_rows(conn, table_name: str) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT id, symbol, name, coingecko_id
        FROM {table_name}
        WHERE coingecko_id IS NOT NULL
          AND BTRIM(coingecko_id) <> ''
        ORDER BY id
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    return [
        {
            "id": int(row[0]),
            "symbol": row[1],
            "name": row[2],
            "coingecko_id": str(row[3]).strip(),
        }
        for row in rows
    ]


def _fetch_markets_batch(coingecko_ids: Sequence[str]) -> List[Dict[str, Any]]:
    if not coingecko_ids:
        return []

    params = dict(MARKETS_CFG.get("params") or {})
    params["ids"] = ",".join(coingecko_ids)
    payload = request_json(
        MARKETS_CFG["url"],
        headers=_build_headers(),
        params=params,
        timeout=MARKETS_CFG.get("timeout", 60),
        retries=MARKETS_CFG.get("retries", 3),
        backoff=MARKETS_CFG.get("backoff", 1.5),
        retry_statuses=MARKETS_CFG.get("retry_statuses"),
    )
    if not isinstance(payload, list):
        return []
    return [entry for entry in payload if isinstance(entry, dict)]


def _fetch_tickers_payload(coingecko_id: str) -> Optional[Dict[str, Any]]:
    if not TICKERS_CFG.get("enabled", True):
        return None

    payload = request_json(
        TICKERS_CFG["url"].format(coingecko_id=coingecko_id),
        headers=_build_headers(),
        params=TICKERS_CFG.get("params"),
        timeout=TICKERS_CFG.get("timeout", 60),
        retries=TICKERS_CFG.get("retries", 3),
        backoff=TICKERS_CFG.get("backoff", 1.5),
        retry_statuses=TICKERS_CFG.get("retry_statuses"),
    )
    return payload if isinstance(payload, dict) else None


def _ticker_volume_usd(ticker: Dict[str, Any]) -> Optional[float]:
    converted_volume = ticker.get("converted_volume")
    if isinstance(converted_volume, dict):
        volume_usd = _safe_float(converted_volume.get("usd"))
        if volume_usd is not None:
            return volume_usd

    raw_volume = _safe_float(ticker.get("volume"))
    if raw_volume is None:
        return None

    converted_last = ticker.get("converted_last")
    if isinstance(converted_last, dict):
        last_usd = _safe_float(converted_last.get("usd"))
        if last_usd is not None:
            return raw_volume * last_usd

    return raw_volume


def _summarize_tickers(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    tickers = payload.get("tickers") if isinstance(payload, dict) else None
    if not isinstance(tickers, list):
        tickers = []

    valid_tickers: List[Dict[str, Any]] = []
    exchange_identifiers = set()
    stale_count = 0
    anomaly_count = 0
    last_synced_at: Optional[datetime] = None

    for ticker in tickers:
        if not isinstance(ticker, dict):
            continue

        last_synced_at = _coalesce_timestamp(
            last_synced_at,
            _parse_timestamp(ticker.get("last_fetch_at")),
            _parse_timestamp(ticker.get("timestamp")),
            _parse_timestamp(ticker.get("last_traded_at")),
        )

        if ticker.get("is_stale") is True:
            stale_count += 1
            continue
        if ticker.get("is_anomaly") is True:
            anomaly_count += 1
            continue

        market = ticker.get("market")
        market_identifier = None
        market_name = None
        if isinstance(market, dict):
            market_identifier = _normalize_text(market.get("identifier"))
            market_name = _normalize_text(market.get("name"))

        if market_identifier:
            exchange_identifiers.add(market_identifier)
        elif market_name:
            exchange_identifiers.add(market_name.lower())

        valid_tickers.append(ticker)

    best_ticker = None
    if valid_tickers:
        best_ticker = max(
            valid_tickers,
            key=lambda ticker: (
                _ticker_volume_usd(ticker) or -1,
                -(_safe_float(ticker.get("bid_ask_spread_percentage")) or 999999),
            ),
        )

    best_market = best_ticker.get("market") if isinstance(best_ticker, dict) else None
    best_exchange_name = None
    best_exchange_identifier = None
    if isinstance(best_market, dict):
        best_exchange_name = _normalize_text(best_market.get("name"))
        best_exchange_identifier = _normalize_text(best_market.get("identifier"))

    return {
        "tickers_seen": len(tickers),
        "valid_ticker_count": len(valid_tickers),
        "exchange_count": len(exchange_identifiers),
        "stale_ticker_count": stale_count,
        "anomaly_ticker_count": anomaly_count,
        "best_exchange_name": best_exchange_name,
        "best_exchange_identifier": best_exchange_identifier,
        "best_volume_usd": _ticker_volume_usd(best_ticker) if best_ticker else None,
        "cost_to_move_up_usd": (
            _safe_float(best_ticker.get("cost_to_move_up_usd")) if best_ticker else None
        ),
        "cost_to_move_down_usd": (
            _safe_float(best_ticker.get("cost_to_move_down_usd")) if best_ticker else None
        ),
        "bid_ask_spread_pct": (
            _safe_float(best_ticker.get("bid_ask_spread_percentage")) if best_ticker else None
        ),
        "source_last_synced_at": last_synced_at,
    }


def _build_record(
    row: Dict[str, Any],
    market_payload: Dict[str, Any],
    ticker_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    market_last_updated = _parse_timestamp(market_payload.get("last_updated"))
    ticker_summary = _summarize_tickers(ticker_payload)

    snapshot_at = _coalesce_timestamp(
        market_last_updated,
        ticker_summary.get("source_last_synced_at"),
    ) or datetime.now(timezone.utc)
    metric_date = (
        market_last_updated.date()
        if market_last_updated is not None
        else snapshot_at.date()
    )

    market_price_usd = _safe_float(market_payload.get("current_price"))
    market_cap_usd = _safe_float(market_payload.get("market_cap"))
    tvl_usd = None
    fees_24h_usd = None

    sources = {
        "coingecko": {
            "coingecko_id": row["coingecko_id"],
            "markets": {
                "last_updated": _normalize_text(market_payload.get("last_updated")),
                "market_cap_rank": market_payload.get("market_cap_rank"),
            },
            "tickers": {
                "enabled": bool(TICKERS_CFG.get("enabled", True)),
                "tickers_seen": ticker_summary["tickers_seen"],
                "selected_exchange": ticker_summary["best_exchange_identifier"]
                or ticker_summary["best_exchange_name"],
                "selected_volume_usd": ticker_summary["best_volume_usd"],
                "selected_cost_to_move_up_usd": ticker_summary["cost_to_move_up_usd"],
                "selected_cost_to_move_down_usd": ticker_summary["cost_to_move_down_usd"],
                "selected_bid_ask_spread_pct": ticker_summary["bid_ask_spread_pct"],
            },
        }
    }

    quality = {
        "market_payload_found": True,
        "tickers_payload_found": ticker_payload is not None,
        "valid_ticker_count": ticker_summary["valid_ticker_count"],
        "stale_ticker_count": ticker_summary["stale_ticker_count"],
        "anomaly_ticker_count": ticker_summary["anomaly_ticker_count"],
    }

    return {
        "coin_id": int(row["id"]),
        "metric_date": metric_date,
        "snapshot_at": snapshot_at,
        "price_usd": market_price_usd,
        "market_cap_usd": market_cap_usd,
        "fdv_usd": _safe_float(market_payload.get("fully_diluted_valuation")),
        "volume_24h_usd": _safe_float(market_payload.get("total_volume")),
        "price_change_pct_24h": _safe_float(
            market_payload.get("price_change_percentage_24h")
            if market_payload.get("price_change_percentage_24h") is not None
            else market_payload.get("price_change_percentage_24h_in_currency")
        ),
        "circulating_supply": _safe_float(market_payload.get("circulating_supply")),
        "total_supply": _safe_float(market_payload.get("total_supply")),
        "max_supply": _safe_float(market_payload.get("max_supply")),
        "exchange_count": ticker_summary["exchange_count"],
        "best_liquidity_exchange": ticker_summary["best_exchange_name"],
        "cost_to_move_up_usd": ticker_summary["cost_to_move_up_usd"],
        "cost_to_move_down_usd": ticker_summary["cost_to_move_down_usd"],
        "bid_ask_spread_pct": ticker_summary["bid_ask_spread_pct"],
        "tvl_usd": tvl_usd,
        "fees_24h_usd": fees_24h_usd,
        "revenue_24h_usd": None,
        "dex_volume_24h_usd": None,
        "dex_liquidity_usd": None,
        "active_addresses_1d": None,
        "tx_count_1d": None,
        "tx_volume_usd_1d": None,
        "fees_usd_1d": None,
        "mc_to_tvl": (
            market_cap_usd / tvl_usd
            if market_cap_usd is not None and tvl_usd not in (None, 0)
            else None
        ),
        "mc_to_fees_annualized": (
            market_cap_usd / (fees_24h_usd * 365)
            if market_cap_usd is not None and fees_24h_usd not in (None, 0)
            else None
        ),
        "sources": Json(sources),
        "quality": Json(quality),
        "source_last_synced_at": snapshot_at,
    }


def _replace_daily_metric(conn, table_name: str, record: Dict[str, Any]) -> int:
    delete_sql = f"""
        DELETE FROM {table_name}
        WHERE coin_id = %s
          AND metric_date = %s
    """
    insert_sql = f"""
        INSERT INTO {table_name} (
            coin_id,
            metric_date,
            snapshot_at,
            price_usd,
            market_cap_usd,
            fdv_usd,
            volume_24h_usd,
            price_change_pct_24h,
            circulating_supply,
            total_supply,
            max_supply,
            exchange_count,
            best_liquidity_exchange,
            cost_to_move_up_usd,
            cost_to_move_down_usd,
            bid_ask_spread_pct,
            tvl_usd,
            fees_24h_usd,
            revenue_24h_usd,
            dex_volume_24h_usd,
            dex_liquidity_usd,
            active_addresses_1d,
            tx_count_1d,
            tx_volume_usd_1d,
            fees_usd_1d,
            mc_to_tvl,
            mc_to_fees_annualized,
            sources,
            quality,
            source_last_synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    with conn.cursor() as cursor:
        cursor.execute(delete_sql, (record["coin_id"], record["metric_date"]))
        cursor.execute(
            insert_sql,
            (
                record["coin_id"],
                record["metric_date"],
                record["snapshot_at"],
                record["price_usd"],
                record["market_cap_usd"],
                record["fdv_usd"],
                record["volume_24h_usd"],
                record["price_change_pct_24h"],
                record["circulating_supply"],
                record["total_supply"],
                record["max_supply"],
                record["exchange_count"],
                record["best_liquidity_exchange"],
                record["cost_to_move_up_usd"],
                record["cost_to_move_down_usd"],
                record["bid_ask_spread_pct"],
                record["tvl_usd"],
                record["fees_24h_usd"],
                record["revenue_24h_usd"],
                record["dex_volume_24h_usd"],
                record["dex_liquidity_usd"],
                record["active_addresses_1d"],
                record["tx_count_1d"],
                record["tx_volume_usd_1d"],
                record["fees_usd_1d"],
                record["mc_to_tvl"],
                record["mc_to_fees_annualized"],
                record["sources"],
                record["quality"],
                record["source_last_synced_at"],
            ),
        )
        return cursor.rowcount


with DAG(
    dag_id="sync_crypto_market_daily_matrics_dag",
    description="Sync CoinGecko market metrics into cryptocurrency_market_metrics_daily",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="10 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coingecko", "metrics"],
) as dag:

    @task
    def sync_market_metrics() -> None:
        if not API_KEY:
            logging.warning(
                "Airflow Variable %s is empty. CoinGecko requests may be rate-limited.",
                API_KEY_VAR or "coingecko_api_key",
            )

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()

        inserted_rows = 0
        market_missing_ids: List[str] = []
        ticker_failed_ids: List[str] = []
        processing_failed_ids: List[str] = []
        sample_updates: List[Dict[str, Any]] = []

        try:
            logging.info("Connected to Postgres target: %s", _get_db_identity(conn))
            target_rows = _load_target_rows(conn, DB_CFG["source_table"])
            if not target_rows:
                logging.warning("No rows with coingecko_id found in %s", DB_CFG["source_table"])
                return

            logging.info(
                "Starting CoinGecko market metric sync for %s rows from %s",
                len(target_rows),
                DB_CFG["source_table"],
            )

            market_payload_by_id: Dict[str, Dict[str, Any]] = {}
            batch_size = max(int(MARKETS_CFG.get("batch_size", 50) or 50), 1)
            for batch in _chunked(target_rows, batch_size):
                batch_ids = [str(row["coingecko_id"]) for row in batch]
                payload = _fetch_markets_batch(batch_ids)
                for entry in payload:
                    coin_gecko_id = _normalize_text(entry.get("id"))
                    if coin_gecko_id:
                        market_payload_by_id[coin_gecko_id] = entry
            if not market_payload_by_id:
                raise ValueError("CoinGecko /coins/markets returned no usable payloads")

            pause_seconds = float(TICKERS_CFG.get("request_pause_seconds", 0) or 0)
            failure_pause_seconds = float(
                TICKERS_CFG.get("failure_pause_seconds", pause_seconds) or 0
            )

            for index, row in enumerate(target_rows, start=1):
                coingecko_id = row["coingecko_id"]
                market_payload = market_payload_by_id.get(coingecko_id)
                if not market_payload:
                    market_missing_ids.append(coingecko_id)
                    logging.warning(
                        "[%s/%s] Missing /coins/markets payload for coingecko_id=%s",
                        index,
                        len(target_rows),
                        coingecko_id,
                    )
                    continue

                ticker_payload = None
                if TICKERS_CFG.get("enabled", True):
                    try:
                        ticker_payload = _fetch_tickers_payload(coingecko_id)
                        if ticker_payload is None:
                            ticker_failed_ids.append(coingecko_id)
                            logging.warning(
                                "[%s/%s] Missing /tickers payload for coingecko_id=%s",
                                index,
                                len(target_rows),
                                coingecko_id,
                            )
                            if failure_pause_seconds > 0:
                                time.sleep(failure_pause_seconds)
                    except Exception as exc:
                        ticker_failed_ids.append(coingecko_id)
                        logging.exception(
                            "[%s/%s] Failed to fetch /tickers for coingecko_id=%s error=%s",
                            index,
                            len(target_rows),
                            coingecko_id,
                            exc,
                        )
                        if failure_pause_seconds > 0:
                            time.sleep(failure_pause_seconds)
                    finally:
                        if pause_seconds > 0:
                            time.sleep(pause_seconds)

                try:
                    record = _build_record(row, market_payload, ticker_payload)
                    inserted_rows += _replace_daily_metric(
                        conn,
                        DB_CFG["target_table"],
                        record,
                    )
                    conn.commit()

                    if len(sample_updates) < 10:
                        sample_updates.append(
                            {
                                "coin_id": record["coin_id"],
                                "coingecko_id": coingecko_id,
                                "metric_date": str(record["metric_date"]),
                                "price_usd": record["price_usd"],
                                "best_liquidity_exchange": record["best_liquidity_exchange"],
                                "cost_to_move_up_usd": record["cost_to_move_up_usd"],
                                "cost_to_move_down_usd": record["cost_to_move_down_usd"],
                            }
                        )
                except Exception as exc:
                    conn.rollback()
                    processing_failed_ids.append(coingecko_id)
                    logging.exception(
                        "[%s/%s] Failed to transform/load coin_id=%s coingecko_id=%s error=%s",
                        index,
                        len(target_rows),
                        row["id"],
                        coingecko_id,
                        exc,
                    )

            logging.info(
                (
                    "CoinGecko market metrics sync completed for %s: target_rows=%s "
                    "loaded_rows=%s missing_market_payloads=%s missing_ticker_payloads=%s "
                    "processing_failures=%s "
                    "sample_updates=%s"
                ),
                DB_CFG["target_table"],
                len(target_rows),
                inserted_rows,
                len(market_missing_ids),
                len(ticker_failed_ids),
                len(processing_failed_ids),
                sample_updates,
            )
            if market_missing_ids:
                logging.warning(
                    "CoinGecko /coins/markets missing ids (%s): %s",
                    len(market_missing_ids),
                    market_missing_ids[:20],
                )
            if ticker_failed_ids:
                logging.warning(
                    "CoinGecko /tickers missing ids (%s): %s",
                    len(ticker_failed_ids),
                    ticker_failed_ids[:20],
                )
            if processing_failed_ids:
                logging.warning(
                    "CoinGecko market metric processing failed ids (%s): %s",
                    len(processing_failed_ids),
                    processing_failed_ids[:20],
                )
        finally:
            conn.close()

    sync_market_metrics()
