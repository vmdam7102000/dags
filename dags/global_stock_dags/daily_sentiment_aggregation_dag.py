# dags/global_stock_dags/daily_sentiment_aggregation_dag.py
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, List

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("global_stock_configs/sentiment_aggregation.yml")["sentiment_aggregation"]


def _aggregate_daily_sentiment(logical_date: datetime) -> int:
    postgres_conn_id = CONFIG["postgres_conn_id"]
    news_table = CONFIG["news_table"]
    evaluation_table = CONFIG["evaluation_table"]
    daily_table = CONFIG["daily_table"]

    lookback_days = int(CONFIG.get("lookback_days", 1))
    end_date = logical_date.date()
    start_date = (logical_date - timedelta(days=lookback_days)).date()

    model_filter: List[str] = list(CONFIG.get("model_filter") or [])
    model_clause = ""
    params: List[Any] = [start_date, end_date]
    if model_filter:
        model_clause = "AND se.model_name = ANY(%s)"
        params.append(model_filter)

    sql = f"""
        INSERT INTO {daily_table} (
            company_id,
            date,
            model_name,
            article_count,
            relevant_count,
            avg_sentiment_score,
            max_sentiment_score,
            min_sentiment_score,
            positive_count,
            negative_count,
            neutral_count,
            calculated_at
        )
        SELECT
            n.company_id,
            DATE(n.published_at) AS date,
            se.model_name,
            COUNT(*) AS article_count,
            COUNT(*) FILTER (WHERE se.is_relevant IS TRUE) AS relevant_count,
            AVG(se.sentiment_score) AS avg_sentiment_score,
            MAX(se.sentiment_score) AS max_sentiment_score,
            MIN(se.sentiment_score) AS min_sentiment_score,
            COUNT(*) FILTER (WHERE se.sentiment_label = 'positive') AS positive_count,
            COUNT(*) FILTER (WHERE se.sentiment_label = 'negative') AS negative_count,
            COUNT(*) FILTER (WHERE se.sentiment_label = 'neutral') AS neutral_count,
            NOW() AS calculated_at
        FROM {news_table} n
        JOIN {evaluation_table} se ON se.article_id = n.id
        WHERE n.published_at >= %s
          AND n.published_at < %s
          {model_clause}
        GROUP BY n.company_id, DATE(n.published_at), se.model_name
        ON CONFLICT (company_id, date, model_name)
        DO UPDATE SET
            article_count = EXCLUDED.article_count,
            relevant_count = EXCLUDED.relevant_count,
            avg_sentiment_score = EXCLUDED.avg_sentiment_score,
            max_sentiment_score = EXCLUDED.max_sentiment_score,
            min_sentiment_score = EXCLUDED.min_sentiment_score,
            positive_count = EXCLUDED.positive_count,
            negative_count = EXCLUDED.negative_count,
            neutral_count = EXCLUDED.neutral_count,
            calculated_at = EXCLUDED.calculated_at
    """

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        conn.commit()
        return cursor.rowcount or 0
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="daily_sentiment_aggregation_dag",
    description="Aggregate daily sentiment after evaluations",
    default_args={
        "owner": "news-pipeline",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=CONFIG.get("schedule_interval"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["news", "sentiment", "aggregation"],
) as dag:

    @task
    def aggregate() -> dict:
        context = get_current_context()
        logical_date = context["logical_date"]
        affected = _aggregate_daily_sentiment(logical_date)
        logging.info("Aggregated sentiment rows affected: %s", affected)
        return {"rows": affected}

    aggregate()
