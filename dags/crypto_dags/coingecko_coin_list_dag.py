from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Set, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("crypto_configs/coingecko_coin_list.yml")[
    "sync_crypto_coingecko_ids_dag"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
API_KEY_VAR = API_CFG.get("api_key_var")
API_KEY = Variable.get(API_KEY_VAR, default_var="") if API_KEY_VAR else ""


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _build_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    api_key_header = API_CFG.get("api_key_header")
    if API_KEY and api_key_header:
        headers[api_key_header] = API_KEY
    return headers


def _load_target_keys(conn, table_name: str) -> Set[Tuple[str, str]]:
    sql = f"""
        SELECT DISTINCT
            LOWER(BTRIM(symbol)) AS symbol_lower,
            LOWER(BTRIM(name)) AS name_lower
        FROM {table_name}
        WHERE symbol IS NOT NULL
          AND BTRIM(symbol) <> ''
          AND name IS NOT NULL
          AND BTRIM(name) <> ''
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return {
            (row[0], row[1])
            for row in cursor.fetchall()
            if row[0] and row[1]
        }


def _count_populated_coingecko_ids(conn, table_name: str) -> int:
    sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE coingecko_id IS NOT NULL
          AND BTRIM(coingecko_id) <> ''
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


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


def _extract_unique_matches(
    payload: List[Dict[str, Any]],
    target_keys: Set[Tuple[str, str]],
) -> Tuple[Dict[Tuple[str, str], str], Dict[Tuple[str, str], List[str]], int]:
    matches: Dict[Tuple[str, str], Set[str]] = defaultdict(set)
    skipped_records = 0

    for entry in payload:
        if not isinstance(entry, dict):
            skipped_records += 1
            continue

        symbol = _normalize_text(entry.get("symbol"))
        name = _normalize_text(entry.get("name"))
        coin_id = str(entry.get("id") or "").strip()
        target_key = (symbol, name)
        if (
            not symbol
            or not name
            or not coin_id
            or target_key not in target_keys
        ):
            continue

        matches[target_key].add(coin_id)

    unique_matches = {
        target_key: next(iter(coin_ids))
        for target_key, coin_ids in matches.items()
        if len(coin_ids) == 1
    }
    ambiguous_matches = {
        target_key: sorted(coin_ids)
        for target_key, coin_ids in matches.items()
        if len(coin_ids) > 1
    }
    return unique_matches, ambiguous_matches, skipped_records


def _update_coingecko_ids(
    conn,
    table_name: str,
    target_key_to_id: Dict[Tuple[str, str], str],
) -> int:
    if not target_key_to_id:
        return 0

    update_sql = f"""
        UPDATE {table_name}
        SET coingecko_id = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE LOWER(BTRIM(symbol)) = %s
          AND LOWER(BTRIM(name)) = %s
          AND coingecko_id IS DISTINCT FROM %s
    """

    updated_rows = 0
    with conn.cursor() as cursor:
        for (symbol_lower, name_lower), coin_id in target_key_to_id.items():
            cursor.execute(
                update_sql,
                (coin_id, symbol_lower, name_lower, coin_id),
            )
            updated_rows += cursor.rowcount

    return updated_rows


def _load_updated_samples(conn, table_name: str, limit: int = 10) -> List[Tuple[str, str]]:
    sql = f"""
        SELECT symbol, coingecko_id
        FROM {table_name}
        WHERE coingecko_id IS NOT NULL
          AND BTRIM(coingecko_id) <> ''
        ORDER BY updated_at DESC NULLS LAST, symbol
        LIMIT %s
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (limit,))
        return [(str(row[0]), str(row[1])) for row in cursor.fetchall()]


def _count_expected_matches(
    conn,
    table_name: str,
    target_key_to_id: Dict[Tuple[str, str], str],
) -> int:
    if not target_key_to_id:
        return 0

    match_sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE LOWER(BTRIM(symbol)) = %s
          AND LOWER(BTRIM(name)) = %s
          AND coingecko_id = %s
    """
    matched_rows = 0
    with conn.cursor() as cursor:
        for (symbol_lower, name_lower), coin_id in target_key_to_id.items():
            cursor.execute(match_sql, (symbol_lower, name_lower, coin_id))
            row = cursor.fetchone()
            matched_rows += int(row[0]) if row else 0
    return matched_rows


with DAG(
    dag_id="sync_crypto_coingecko_ids_dag",
    description="Sync CoinGecko ids into cryptocurrency_top100 by exact lower(symbol) and lower(name) match",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="30 5 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "coingecko", "metadata"],
) as dag:

    @task
    def sync_coingecko_ids() -> None:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()

        try:
            db_identity = _get_db_identity(conn)
            logging.info("Connected to Postgres target: %s", db_identity)

            target_keys = _load_target_keys(conn, DB_CFG["target_table"])
            if not target_keys:
                logging.warning("No symbol/name pairs found in %s", DB_CFG["target_table"])
                return
            populated_before = _count_populated_coingecko_ids(conn, DB_CFG["target_table"])

            payload = request_json(
                API_CFG["url"],
                headers=_build_headers(),
                timeout=API_CFG.get("timeout", 60),
                retries=API_CFG.get("retries", 3),
                backoff=API_CFG.get("backoff", 1.5),
                retry_statuses=API_CFG.get("retry_statuses"),
            )
            if not isinstance(payload, list):
                raise ValueError("CoinGecko /coins/list did not return a JSON array")

            unique_matches, ambiguous_matches, skipped_records = _extract_unique_matches(
                payload,
                target_keys,
            )
            matched_keys = set(unique_matches) | set(ambiguous_matches)
            unmatched_keys = target_keys - matched_keys

            updated_rows = _update_coingecko_ids(
                conn,
                DB_CFG["target_table"],
                unique_matches,
            )
            conn.commit()
            populated_after = _count_populated_coingecko_ids(conn, DB_CFG["target_table"])
            expected_matches_after = _count_expected_matches(
                conn,
                DB_CFG["target_table"],
                unique_matches,
            )
            updated_samples = _load_updated_samples(conn, DB_CFG["target_table"])

            if unique_matches and expected_matches_after == 0:
                raise ValueError(
                    "Post-commit verification failed: none of the expected CoinGecko ids "
                    f"were found in {DB_CFG['target_table']} after update"
                )

            if ambiguous_matches:
                sample = {
                    f"{symbol}|{name}": coin_ids[:3]
                    for (symbol, name), coin_ids in list(ambiguous_matches.items())[:10]
                }
                logging.warning(
                    "Skipped %s ambiguous symbol/name matches from CoinGecko: %s",
                    len(ambiguous_matches),
                    sample,
                )

            logging.info(
                (
                    "CoinGecko sync completed for %s: target_keys=%s "
                    "unique_matches=%s ambiguous_matches=%s unmatched_keys=%s "
                    "updated_rows=%s skipped_payload_records=%s "
                    "populated_before=%s populated_after=%s "
                    "expected_matches_after=%s updated_samples=%s"
                ),
                DB_CFG["target_table"],
                len(target_keys),
                len(unique_matches),
                len(ambiguous_matches),
                len(unmatched_keys),
                updated_rows,
                skipped_records,
                populated_before,
                populated_after,
                expected_matches_after,
                updated_samples,
            )
        finally:
            conn.close()

    sync_coingecko_ids()
