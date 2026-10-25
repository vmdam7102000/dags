from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.timetables.interval import CronDataIntervalTimetable

from plugins.utils.config_loader import load_yaml_config
from plugins.utils.eod_price_sync import get_adjusted_refresh_date, sync_global_eod_one

CONFIG = load_yaml_config("global_stock_configs/global_eod_prices.yml")["global_eod_prices"]
API_CFG = CONFIG["api"]
REFRESH_CFG = CONFIG["adjusted_refresh"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = 400
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")
EODHD_POOL = Variable.get("eodhd_airflow_pool", default_var="default_pool")
ADJUSTED_UPDATE_COLUMNS = ("adjusted_close",)


with DAG(
    dag_id="refresh_global_eod_stock_adjusted_prices_monthly_dag",
    description="Month-end refresh of 10,000 days of global-stock adjusted close prices from EODHD",
    default_args={
        "owner": "global-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=CronDataIntervalTimetable(REFRESH_CFG["schedule_interval"], timezone="UTC"),
    # The first complete interval ends on 2026-10-31 at 03:30 UTC.
    start_date=datetime(2026, 9, 30, 3, 30, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["stock", "global", "eod", "eodhd", "adjusted-refresh", "monthly"],
) as dag:

    @task
    def get_companies() -> List[Dict[str, Any]]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        cursor = conn.cursor()
        id_col = DB_CFG.get("company_id_column", "id")
        ticker_col = DB_CFG.get("company_ticker_column", "ticker")
        table = DB_CFG["company_table"]
        where_clause = " WHERE is_active = TRUE" if DB_CFG.get("only_active", True) else ""
        try:
            cursor.execute(f"SELECT {id_col}, {ticker_col} FROM {table}{where_clause}")
            companies = [
                {"company_id": row[0], "ticker": row[1]} for row in cursor.fetchall()
            ]
        finally:
            cursor.close()
            conn.close()
        logging.info("Fetched %s companies from %s", len(companies), table)
        return companies

    @task
    def chunk_companies(companies: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        return [companies[index : index + CHUNK_SIZE] for index in range(0, len(companies), CHUNK_SIZE)]

    @task(pool=EODHD_POOL)
    def refresh_company_batch(companies: List[Dict[str, Any]]) -> None:
        refresh_date = get_adjusted_refresh_date(get_current_context())
        logging.info("Refreshing %s companies as of %s", len(companies), refresh_date)
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for company in companies:
                ticker = company.get("ticker")
                company_id = company.get("company_id")
                if not ticker or company_id is None:
                    continue
                sync_global_eod_one(
                    ticker=ticker,
                    company_id=company_id,
                    logical_date=refresh_date,
                    lookback_days=REFRESH_CFG["lookback_days"],
                    api_cfg=API_CFG,
                    db_cfg=DB_CFG,
                    api_key=API_KEY,
                    update_columns=ADJUSTED_UPDATE_COLUMNS,
                    conn=conn,
                )
        finally:
            conn.close()

    companies = get_companies()
    company_batches = chunk_companies(companies)
    refresh_company_batch.expand(companies=company_batches)
