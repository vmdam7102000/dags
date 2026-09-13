from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

# DagBag may import a nested DAG file without placing the repository's nested
# DAG root on sys.path. Add that one stable root before package imports so
# `crypto_dags.*` works both for direct DagBag loading and normal Python imports.
DAGS_ROOT = str(Path(__file__).resolve().parent.parent)
if DAGS_ROOT not in sys.path:
    sys.path.insert(0, DAGS_ROOT)

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from crypto_dags.cmc_top30_universe import (
    historical_listing_params,
    normalize_historical_listing,
    parse_bool,
    requested_snapshot_dates,
)
from crypto_dags.cmc_top30_universe_store import (
    mark_snapshot_failed,
    mark_snapshot_pending,
    missing_snapshot_dates,
    replace_snapshot,
)
from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config


CONFIG = load_yaml_config("crypto_configs/cmc_top30_universe.yml")[
    "sync_cmc_top30_universe_dag"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
DOWNSTREAM_CFG = CONFIG.get("downstream") or {}


def _should_trigger_mapping(**context) -> bool:
    dag_run = context.get("dag_run")
    conf = dict(dag_run.conf or {}) if dag_run else {}
    enabled = parse_bool(
        conf.get("trigger_mapping"),
        default=bool(DOWNSTREAM_CFG.get("trigger_mapping", True)),
    )
    logging.info("CMC Top 50 downstream mapping trigger enabled=%s", enabled)
    return enabled


with DAG(
    dag_id="sync_cmc_top30_point_in_time_universe_dag",
    description=(
        "Sync month-end point-in-time CMC Top 50 ranking snapshots "
        "using legacy cmc_top30 names"
    ),
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 0,
        "execution_timeout": timedelta(
            hours=CONFIG.get("execution_timeout_hours", 2)
        ),
    },
    schedule_interval=CONFIG.get("schedule", "15 1 1 * *"),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=[
        "crypto",
        "cmc",
        "universe",
        "top30",
        "top50",
        "point-in-time",
        "monthly",
    ],
) as dag:

    @task
    def select_dates(dag_run=None) -> List[str]:
        conf: Dict[str, Any] = dict(dag_run.conf or {}) if dag_run else {}
        requested = requested_snapshot_dates(
            conf,
            now=datetime.now(timezone.utc),
            history_years=int(CONFIG.get("history_years", 3)),
        )
        refresh_existing = parse_bool(conf.get("refresh_existing"), default=False)

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            targets = missing_snapshot_dates(
                conn,
                requested,
                refresh_existing=refresh_existing,
                run_table=DB_CFG["run_table"],
            )
        finally:
            conn.close()

        logging.info(
            "CMC Top 50 month-end targets: requested=%s fetch=%s skipped=%s refresh=%s",
            len(requested),
            len(targets),
            len(requested) - len(targets),
            refresh_existing,
        )
        return [value.isoformat() for value in targets]

    @task(retries=0)
    def fetch_and_load(snapshot_dates: List[str]) -> Dict[str, Any]:
        if not snapshot_dates:
            return {"fetched": 0, "credits": 0, "dates": []}

        cmc_api_key = Variable.get(API_CFG["api_key_var"], default_var="")
        if not cmc_api_key:
            raise ValueError(
                f"Missing Airflow Variable {API_CFG['api_key_var']}"
            )

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        fetched_dates: List[str] = []
        credits = 0
        try:
            for index, date_text in enumerate(snapshot_dates, start=1):
                snapshot_date = datetime.strptime(date_text, "%Y-%m-%d").date()
                mark_snapshot_pending(
                    conn,
                    snapshot_date,
                    run_table=DB_CFG["run_table"],
                )
                try:
                    payload = request_json(
                        API_CFG["url"],
                        params=historical_listing_params(snapshot_date),
                        headers={
                            "Accept": "application/json",
                            "X-CMC_PRO_API_KEY": cmc_api_key,
                        },
                        timeout=API_CFG.get("timeout", 60),
                        retries=API_CFG.get("retries", 4),
                        backoff=API_CFG.get("backoff", 2),
                        retry_statuses=[429, 500, 502, 503, 504],
                        fatal_statuses=[400, 401, 403],
                    )
                    if not isinstance(payload, dict):
                        raise RuntimeError(
                            f"No valid CMC payload for {snapshot_date.isoformat()}"
                        )

                    normalized = normalize_historical_listing(
                        payload,
                        snapshot_date=snapshot_date,
                        collected_at=datetime.now(timezone.utc),
                    )
                    replace_snapshot(
                        conn,
                        normalized,
                        run_table=DB_CFG["run_table"],
                        snapshot_table=DB_CFG["snapshot_table"],
                    )
                except Exception as exc:
                    conn.rollback()
                    mark_snapshot_failed(
                        conn,
                        snapshot_date,
                        exc,
                        run_table=DB_CFG["run_table"],
                    )
                    logging.exception(
                        "Failed CMC Top 50 snapshot %s (%s/%s)",
                        snapshot_date,
                        index,
                        len(snapshot_dates),
                    )
                    raise

                fetched_dates.append(snapshot_date.isoformat())
                credits += int(normalized.get("api_credit_count") or 0)
                logging.info(
                    "Loaded CMC Top 50 month-end %s (%s/%s, hash=%s)",
                    snapshot_date,
                    index,
                    len(snapshot_dates),
                    normalized["payload_sha256"],
                )

                if index < len(snapshot_dates):
                    time.sleep(float(API_CFG.get("throttle_seconds", 0.25)))
        finally:
            conn.close()

        return {
            "fetched": len(fetched_dates),
            "credits": credits,
            "dates": fetched_dates,
        }

    load_summary = fetch_and_load(select_dates())

    mapping_gate = ShortCircuitOperator(
        task_id="check_trigger_asset_mapping_sync",
        python_callable=_should_trigger_mapping,
    )
    trigger_mapping = TriggerDagRunOperator(
        task_id="trigger_asset_mapping_sync",
        trigger_dag_id=DOWNSTREAM_CFG.get(
            "mapping_dag_id", "sync_cmc_top30_asset_mappings_dag"
        ),
        conf={"mode": "delta"},
        wait_for_completion=False,
        reset_dag_run=False,
    )
    load_summary >> mapping_gate >> trigger_mapping
