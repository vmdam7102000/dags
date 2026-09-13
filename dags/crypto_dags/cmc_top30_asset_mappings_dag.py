from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional


DAGS_ROOT = str(Path(__file__).resolve().parent.parent)
if DAGS_ROOT not in sys.path:
    sys.path.insert(0, DAGS_ROOT)

import ccxt
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from crypto_dags.cmc_top30_universe import TOP_N, parse_bool
from plugins.utils.cmc_top30_assets import (
    DEFAULT_DATA_START_AT,
    load_canonical_symbol_targets,
    load_existing_symbol_targets,
    replace_canonical_symbol_targets,
    select_primary_usdt_venue,
)
from plugins.utils.config_loader import load_yaml_config


CONFIG = load_yaml_config("crypto_configs/cmc_top30_universe.yml")[
    "sync_cmc_top30_asset_mappings_dag"
]
MARKET_CFG = CONFIG["market"]
CLASSIFICATION_CFG = CONFIG.get("classification") or {}
DB_CFG = CONFIG["db"]
DOWNSTREAM_CFG = CONFIG.get("downstream") or {}
QUOTE = str(MARKET_CFG.get("quote", "USDT")).strip().upper()

if QUOTE != "USDT":
    raise ValueError(
        "CMC Top 50 (legacy cmc_top30 contract) mapping only supports the USDT quote"
    )


def _should_trigger_backfill(**context) -> bool:
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf or {}) if dag_run else {}
    enabled = parse_bool(
        conf.get("trigger_backfill"),
        default=bool(DOWNSTREAM_CFG.get("trigger_backfill", True)),
    )
    logging.info("CMC Top 50 downstream backfill trigger enabled=%s", enabled)
    return enabled


def _configured_data_start() -> datetime:
    value = CONFIG.get("default_data_start_at")
    if not value:
        return DEFAULT_DATA_START_AT
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_markets() -> tuple[
    Dict[str, Mapping[str, Mapping[str, Any]]],
    Dict[str, str],
]:
    markets_by_venue: Dict[str, Mapping[str, Mapping[str, Any]]] = {}
    errors: Dict[str, str] = {}
    for raw_venue in MARKET_CFG["venue_priority"]:
        venue = str(raw_venue).strip().lower()
        exchange_class = getattr(ccxt, venue, None)
        if exchange_class is None:
            errors[venue] = "ccxt exchange class is unavailable"
            logging.warning("Configured CCXT venue %s is unavailable", venue)
            continue

        exchange = exchange_class(
            {
                "enableRateLimit": True,
                "timeout": int(MARKET_CFG.get("timeout_ms", 60000)),
            }
        )
        try:
            markets_by_venue[venue] = exchange.load_markets()
        except Exception as exc:
            errors[venue] = str(exc).strip() or exc.__class__.__name__
            logging.exception("Unable to load CCXT markets for %s", venue)
        finally:
            close = getattr(exchange, "close", None)
            if callable(close):
                close()
    return markets_by_venue, errors


def _exchange_override(symbol: str) -> Optional[str]:
    overrides = MARKET_CFG.get("exchange_overrides") or {}
    if not isinstance(overrides, Mapping):
        raise ValueError(
            "market.exchange_overrides must be a symbol-to-venue mapping"
        )
    normalized = {
        str(key).strip().upper(): str(value).strip().lower()
        for key, value in overrides.items()
        if str(key).strip() and str(value).strip()
    }
    return normalized.get(str(symbol).strip().upper())


def _resolve_target(
    target: Mapping[str, Any],
    *,
    existing: Optional[Mapping[str, Any]],
    markets_by_venue: Mapping[str, Mapping[str, Mapping[str, Any]]],
    venue_errors: Mapping[str, str],
    exchange_override: Optional[str] = None,
) -> Dict[str, Any]:
    symbol = str(target["symbol"])
    selected_exchange = (
        str(existing.get("selected_exchange")).strip().lower()
        if existing and existing.get("selected_exchange")
        else None
    )
    base = {
        "symbol": symbol,
        "name": target["name"],
        "data_start_at": target["data_start_at"],
        "is_stablecoin": bool(target.get("is_stablecoin")),
        "is_wrapped": bool(target.get("is_wrapped")),
        "selected_exchange": selected_exchange,
    }

    if base["is_stablecoin"] or base["is_wrapped"]:
        return {
            **base,
            "mapping_status": "excluded_by_policy",
            "backfill_status": "excluded_by_policy",
            "last_error": "stablecoin or wrapped asset excluded by research policy",
        }
    if target.get("identity_ambiguous"):
        return {
            **base,
            "mapping_status": "ambiguous",
            "backfill_status": "pending",
            "last_error": target.get("identity_error"),
        }

    selection = select_primary_usdt_venue(
        symbol=symbol,
        venue_priority=MARKET_CFG["venue_priority"],
        markets_by_venue=markets_by_venue,
        venue_errors=venue_errors,
        existing_exchange=selected_exchange,
        exchange_override=exchange_override,
    )
    mapping_status = str(selection["mapping_status"])
    if selection.get("selection_changed"):
        backfill_status = "pending"
    elif existing and existing.get("backfill_status"):
        backfill_status = str(existing["backfill_status"])
        if backfill_status == "excluded_by_policy":
            backfill_status = "pending"
    elif mapping_status == "unavailable":
        backfill_status = "unavailable"
    else:
        backfill_status = "pending"
    return {
        **base,
        "selected_exchange": selection.get("selected_exchange"),
        "mapping_status": mapping_status,
        "backfill_status": backfill_status,
        "force_selected_exchange": bool(selection.get("selection_changed")),
        "last_error": selection.get("last_error"),
    }


with DAG(
    dag_id="sync_cmc_top30_asset_mappings_dag",
    description=(
        "Resolve canonical CMC Top 50 symbols to one sticky USDT venue "
        "using legacy cmc_top30 names"
    ),
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(
            hours=int(CONFIG.get("execution_timeout_hours", 2))
        ),
    },
    schedule_interval=CONFIG.get("schedule", "30 7 * * *"),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=[
        "crypto",
        "cmc",
        "top30",
        "top50",
        "mapping",
        "point-in-time",
    ],
) as dag:

    @task
    def sync_symbol_targets() -> Dict[str, Any]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            targets = load_canonical_symbol_targets(
                conn,
                run_table=DB_CFG["run_table"],
                snapshot_table=DB_CFG["snapshot_table"],
                expected_row_count=TOP_N,
                default_data_start_at=_configured_data_start(),
                stablecoin_overrides=(
                    CLASSIFICATION_CFG.get("stablecoin_overrides") or {}
                ),
                wrapped_overrides=(
                    CLASSIFICATION_CFG.get("wrapped_overrides") or {}
                ),
                data_start_overrides=(
                    CLASSIFICATION_CFG.get("data_start_overrides") or {}
                ),
            )
            existing = load_existing_symbol_targets(
                conn,
                table=DB_CFG["symbol_target_table"],
            )
        finally:
            conn.close()

        if not targets:
            raise RuntimeError("No complete CMC Top 50 snapshots are available")

        needs_market_data = [
            target
            for target in targets
            if not target.get("is_stablecoin")
            and not target.get("is_wrapped")
            and not target.get("identity_ambiguous")
            and (
                not (existing.get(target["symbol"]) or {}).get("selected_exchange")
                or _exchange_override(target["symbol"]) is not None
            )
        ]
        markets_by_venue: Dict[str, Mapping[str, Mapping[str, Any]]] = {}
        venue_errors: Dict[str, str] = {}
        if needs_market_data:
            markets_by_venue, venue_errors = _load_markets()

        records = [
            _resolve_target(
                target,
                existing=existing.get(target["symbol"]),
                markets_by_venue=markets_by_venue,
                venue_errors=venue_errors,
                exchange_override=_exchange_override(target["symbol"]),
            )
            for target in targets
        ]

        conn = hook.get_conn()
        try:
            replace_canonical_symbol_targets(
                conn,
                records,
                table=DB_CFG["symbol_target_table"],
            )
        finally:
            conn.close()

        statuses: Dict[str, int] = {}
        for record in records:
            status = str(record["mapping_status"])
            statuses[status] = statuses.get(status, 0) + 1
        logging.info(
            "CMC Top 50 symbol mapping summary: targets=%s statuses=%s",
            len(records),
            statuses,
        )
        return {
            "targets": len(records),
            "statuses": statuses,
            "symbols": [record["symbol"] for record in records],
        }

    mapping_summary = sync_symbol_targets()

    backfill_gate = ShortCircuitOperator(
        task_id="check_trigger_delta_backfill",
        python_callable=_should_trigger_backfill,
    )
    trigger_backfill = TriggerDagRunOperator(
        task_id="trigger_delta_backfill",
        trigger_dag_id=DOWNSTREAM_CFG.get(
            "backfill_dag_id", "backfill_cmc_top30_historical_data_dag"
        ),
        conf={"mode": "missing_only", "triggered_by_mapping": True},
        wait_for_completion=False,
        reset_dag_run=False,
    )
    mapping_summary >> backfill_gate >> trigger_backfill
