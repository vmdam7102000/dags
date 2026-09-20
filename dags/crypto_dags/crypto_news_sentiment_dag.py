"""Daily CMC Top 50 crypto news ingestion; paused until migration and secrets exist."""
from __future__ import annotations

import sys
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

DAGS_ROOT = str(Path(__file__).resolve().parent.parent)
if DAGS_ROOT not in sys.path:
    sys.path.insert(0, DAGS_ROOT)

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.crypto_news.pipeline import (
    aggregate_coin, classify_pending, fetch_cse, load_universe, lock_coin,
    register_candidates, report_coin, restrict_universe, unlock_coin,
    validate_versions,
)
from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("crypto_configs/news_sentiment.yml")["crypto_news"]


def _connection():
    return PostgresHook(postgres_conn_id=CONFIG["postgres_conn_id"]).get_conn()


def _end() -> datetime:
    return get_current_context()["data_interval_end"].in_timezone("UTC")


with DAG(
    dag_id="crypto_news_sentiment_daily",
    schedule_interval=CONFIG["schedule"],
    start_date=datetime(2026, 9, 19, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"owner": "crypto-data", "retries": 2,
                  "retry_delay": timedelta(minutes=10)},
    tags=["crypto", "news", "sentiment"],
) as dag:

    @task
    def roster(dag_run=None) -> list[dict]:
        conn = _connection()
        try:
            validate_versions(conn, CONFIG)
            all_coins = load_universe(conn, _end())
            conf = dict(dag_run.conf or {}) if dag_run else {}
            return restrict_universe(all_coins, conf.get("symbols"))
        finally:
            conn.close()

    @task
    def ingest(coin: dict) -> dict:
        conn = _connection()
        try:
            items = fetch_cse(
                coin["symbol"], coin["name"], CONFIG,
                Variable.get(CONFIG["google_api_key_var"], default_var=""),
                Variable.get(CONFIG["google_cx_var"], default_var=""),
            )
            registered = register_candidates(conn, coin["symbol"], coin["name"], items, CONFIG)
            logging.info("Crypto news extracted coin=%s candidates=%s", coin["symbol"], registered)
            return coin
        finally:
            conn.close()

    @task
    def classify(coin: dict) -> dict:
        conn = _connection()
        key = None
        try:
            key = lock_coin(conn, coin["symbol"], CONFIG["classifier_version"])
            classified = classify_pending(
                conn, coin["symbol"], CONFIG,
                Variable.get(CONFIG["openrouter_api_key_var"], default_var=""),
                datetime.now(timezone.utc))
            logging.info("Crypto news classified coin=%s count=%s",
                         coin["symbol"], classified)
            return coin
        finally:
            if key is not None:
                unlock_coin(conn, key)
            conn.close()

    @task
    def aggregate(coin: dict) -> dict:
        conn = _connection()
        try:
            aggregate_coin(conn, coin["symbol"], CONFIG, _end())
            return coin
        finally:
            conn.close()

    @task
    def report(coin: dict) -> None:
        conn = _connection()
        try:
            report_coin(conn, coin["symbol"], CONFIG, _end(),
                        Variable.get(CONFIG["openrouter_api_key_var"], default_var=""))
        finally:
            conn.close()

    report.expand(coin=aggregate.expand(coin=classify.expand(coin=ingest.expand(coin=roster()))))
