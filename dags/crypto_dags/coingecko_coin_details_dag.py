from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("crypto_configs/coingecko_coin_list.yml")[
    "sync_crypto_coingecko_details_dag"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
API_KEY_VAR = API_CFG.get("api_key_var")
API_KEY = Variable.get(API_KEY_VAR, default_var="") if API_KEY_VAR else ""


def _build_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    api_key_header = API_CFG.get("api_key_header")
    if API_KEY and api_key_header:
        headers[api_key_header] = API_KEY
    return headers


def _normalize_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _parse_date(value: Any) -> Optional[datetime.date]:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _first_nonempty(values: Sequence[Any]) -> Optional[str]:
    for value in values:
        text = _normalize_text(value)
        if text:
            return text
    return None


def _first_nonempty_key(mapping: Any) -> Optional[str]:
    if not isinstance(mapping, dict):
        return None
    for key in mapping:
        text = _normalize_text(key)
        if text:
            return text
    return None


def _extract_primary_chain(payload: Dict[str, Any]) -> Optional[str]:
    asset_platform_id = _normalize_text(payload.get("asset_platform_id"))
    if asset_platform_id:
        return asset_platform_id

    detail_platforms = payload.get("detail_platforms")
    primary_chain = _first_nonempty_key(detail_platforms)
    if primary_chain:
        return primary_chain

    platforms = payload.get("platforms")
    return _first_nonempty_key(platforms)


def _extract_primary_contract_address(
    payload: Dict[str, Any],
    primary_chain: Optional[str],
) -> Optional[str]:
    detail_platforms = payload.get("detail_platforms")
    if isinstance(detail_platforms, dict):
        if primary_chain and isinstance(detail_platforms.get(primary_chain), dict):
            contract_address = _normalize_text(
                detail_platforms[primary_chain].get("contract_address")
            )
            if contract_address:
                return contract_address

        for detail in detail_platforms.values():
            if isinstance(detail, dict):
                contract_address = _normalize_text(detail.get("contract_address"))
                if contract_address:
                    return contract_address

    platforms = payload.get("platforms")
    if isinstance(platforms, dict):
        if primary_chain:
            contract_address = _normalize_text(platforms.get(primary_chain))
            if contract_address:
                return contract_address

        for address in platforms.values():
            contract_address = _normalize_text(address)
            if contract_address:
                return contract_address

    return None


def _extract_categories(payload: Dict[str, Any]) -> Optional[List[str]]:
    categories = payload.get("categories")
    if not isinstance(categories, list):
        return None

    cleaned: List[str] = []
    seen = set()
    for item in categories:
        text = _normalize_text(item)
        if text and text not in seen:
            seen.add(text)
            cleaned.append(text)

    return cleaned or None


def _extract_category(payload: Dict[str, Any]) -> Optional[str]:
    categories = _extract_categories(payload)
    if not categories:
        return None
    return ", ".join(categories)


def _extract_homepage(payload: Dict[str, Any]) -> Optional[str]:
    links = payload.get("links")
    if not isinstance(links, dict):
        return None
    homepage = links.get("homepage")
    if not isinstance(homepage, list):
        return None
    return _first_nonempty(homepage)


def _extract_whitepaper_url(payload: Dict[str, Any]) -> Optional[str]:
    links = payload.get("links")
    if not isinstance(links, dict):
        return None
    return _normalize_text(links.get("whitepaper"))


def _extract_max_supply(payload: Dict[str, Any]) -> Optional[float]:
    market_data = payload.get("market_data")
    if not isinstance(market_data, dict):
        return None

    raw_value = market_data.get("max_supply")
    if raw_value is None:
        return None
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return None


def _get_cursor_id() -> int:
    cursor_var = API_CFG.get("cursor_var")
    if not cursor_var:
        return 0

    raw_value = Variable.get(cursor_var, default_var="0")
    try:
        return max(int(raw_value), 0)
    except (TypeError, ValueError):
        logging.warning("Invalid cursor value for %s: %s", cursor_var, raw_value)
        return 0


def _set_cursor_id(row_id: int) -> None:
    cursor_var = API_CFG.get("cursor_var")
    if not cursor_var:
        return
    Variable.set(cursor_var, str(max(int(row_id), 0)))


def _load_target_rows(
    conn,
    table_name: str,
    *,
    after_id: int,
    limit: int,
    only_missing_details: bool,
) -> List[Dict[str, Any]]:
    missing_filter_sql = ""
    if only_missing_details:
        missing_filter_sql = """
          AND (
                launch_date IS NULL
             OR primary_chain IS NULL
             OR primary_contract_address IS NULL
             OR max_supply IS NULL
             OR categories IS NULL
             OR homepage IS NULL
             OR whitepaper_url IS NULL
          )
        """

    sql = f"""
        SELECT id, symbol, name, coingecko_id
        FROM {table_name}
        WHERE coingecko_id IS NOT NULL
          AND BTRIM(coingecko_id) <> ''
          AND id > %s
          {missing_filter_sql}
        ORDER BY id
        LIMIT %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (after_id, limit))
        rows = cursor.fetchall()

    return [
        {
            "id": row[0],
            "symbol": row[1],
            "name": row[2],
            "coingecko_id": row[3],
        }
        for row in rows
    ]


def _build_detail_url(coingecko_id: str) -> str:
    return API_CFG["url"].format(coingecko_id=coingecko_id)


def _fetch_coin_details(coingecko_id: str) -> Optional[Dict[str, Any]]:
    payload = request_json(
        _build_detail_url(coingecko_id),
        headers=_build_headers(),
        params=API_CFG.get("params"),
        timeout=API_CFG.get("timeout", 60),
        retries=API_CFG.get("retries", 3),
        backoff=API_CFG.get("backoff", 1.5),
        retry_statuses=API_CFG.get("retry_statuses"),
    )
    return payload if isinstance(payload, dict) else None


def _transform_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    primary_chain = _extract_primary_chain(payload)
    return {
        "name": _normalize_text(payload.get("name")),
        "category": _extract_category(payload),
        "launch_date": _parse_date(payload.get("genesis_date")),
        "primary_chain": primary_chain,
        "primary_contract_address": _extract_primary_contract_address(payload, primary_chain),
        "max_supply": _extract_max_supply(payload),
        "categories": _extract_categories(payload),
        "homepage": _extract_homepage(payload),
        "whitepaper_url": _extract_whitepaper_url(payload),
    }


def _update_coin_details(conn, table_name: str, row_id: int, record: Dict[str, Any]) -> int:
    sql = f"""
        UPDATE {table_name}
        SET name = COALESCE(%s, name),
            category = COALESCE(%s, category),
            launch_date = %s,
            primary_chain = %s,
            primary_contract_address = %s,
            max_supply = %s,
            categories = %s,
            homepage = %s,
            whitepaper_url = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                record.get("name"),
                record.get("category"),
                record.get("launch_date"),
                record.get("primary_chain"),
                record.get("primary_contract_address"),
                record.get("max_supply"),
                record.get("categories"),
                record.get("homepage"),
                record.get("whitepaper_url"),
                row_id,
            ),
        )
        return cursor.rowcount


with DAG(
    dag_id="sync_crypto_coingecko_details_dag",
    description="Sync CoinGecko coin details into cryptocurrency_top100 using coingecko_id",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coingecko", "metadata"],
) as dag:

    @task
    def sync_coingecko_details() -> None:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()

        updated_rows = 0
        success_count = 0
        failed_ids: List[str] = []
        sample_updates: List[Tuple[str, str, Optional[str], Optional[str]]] = []
        last_processed_id = 0

        try:
            batch_size = max(int(API_CFG.get("batch_size", 5) or 5), 1)
            only_missing_details = bool(API_CFG.get("only_missing_details", True))
            cursor_id = _get_cursor_id()
            target_rows = _load_target_rows(
                conn,
                DB_CFG["target_table"],
                after_id=cursor_id,
                limit=batch_size,
                only_missing_details=only_missing_details,
            )
            if not target_rows and cursor_id > 0:
                target_rows = _load_target_rows(
                    conn,
                    DB_CFG["target_table"],
                    after_id=0,
                    limit=batch_size,
                    only_missing_details=only_missing_details,
                )
            if not target_rows:
                logging.warning("No rows with coingecko_id found in %s", DB_CFG["target_table"])
                return

            pause_seconds = float(API_CFG.get("request_pause_seconds", 0) or 0)
            failure_pause_seconds = float(API_CFG.get("failure_pause_seconds", pause_seconds) or 0)

            for row in target_rows:
                last_processed_id = int(row["id"])
                coingecko_id = str(row["coingecko_id"]).strip()
                try:
                    payload = _fetch_coin_details(coingecko_id)
                    if not payload:
                        failed_ids.append(coingecko_id)
                        logging.warning("No detail payload returned for coingecko_id=%s", coingecko_id)
                        if failure_pause_seconds > 0:
                            time.sleep(failure_pause_seconds)
                        continue

                    record = _transform_payload(payload)
                    updated_rows += _update_coin_details(
                        conn,
                        DB_CFG["target_table"],
                        int(row["id"]),
                        record,
                    )
                    success_count += 1

                    if len(sample_updates) < 10:
                        sample_updates.append(
                            (
                                str(row["symbol"]),
                                coingecko_id,
                                record.get("primary_chain"),
                                record.get("homepage"),
                            )
                        )
                except Exception as exc:
                    failed_ids.append(coingecko_id)
                    logging.exception(
                        "Failed to sync CoinGecko details for row_id=%s symbol=%s coingecko_id=%s error=%s",
                        row["id"],
                        row["symbol"],
                        coingecko_id,
                        exc,
                    )
                    if failure_pause_seconds > 0:
                        time.sleep(failure_pause_seconds)
                finally:
                    if pause_seconds > 0:
                        time.sleep(pause_seconds)

            conn.commit()
            if last_processed_id > 0:
                _set_cursor_id(last_processed_id)
            logging.info(
                (
                    "CoinGecko detail sync completed for %s: batch_size=%s cursor_start=%s cursor_end=%s target_rows=%s "
                    "successful_payloads=%s failed_payloads=%s updated_rows=%s "
                    "sample_updates=%s"
                ),
                DB_CFG["target_table"],
                batch_size,
                cursor_id,
                last_processed_id,
                len(target_rows),
                success_count,
                len(failed_ids),
                updated_rows,
                sample_updates,
            )

            if failed_ids:
                logging.warning(
                    "CoinGecko detail sync failed for %s ids: %s",
                    len(failed_ids),
                    failed_ids[:20],
                )
        finally:
            conn.close()

    sync_coingecko_details()
