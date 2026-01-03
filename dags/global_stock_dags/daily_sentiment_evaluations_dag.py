# dags/global_stock_dags/daily_sentiment_evaluations_dag.py
from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("global_stock_configs/sentiment_evaluations.yml")["sentiment_evaluations"]
OPENROUTER_CFG = CONFIG["openrouter"]
POSTGRES_CFG = CONFIG["postgres"]
EVAL_CFG = CONFIG["evaluation"]


def _determine_sentiment(score: Optional[float]) -> str:
    if score is None:
        return "neutral"
    if score >= EVAL_CFG.get("positive_threshold", 60):
        return "positive"
    if score <= EVAL_CFG.get("negative_threshold", 40):
        return "negative"
    return "neutral"


def _fetch_candidates(conn, logical_date: datetime) -> List[Dict[str, Any]]:
    lookback_days = int(EVAL_CFG.get("lookback_days", 1))
    start_date = (logical_date - timedelta(days=lookback_days)).date()
    end_date = logical_date.date()

    company_table = POSTGRES_CFG["company_table"]
    news_table = POSTGRES_CFG["news_table"]
    evaluation_table = POSTGRES_CFG["evaluation_table"]

    sql = f"""
        SELECT
            n.id AS article_id,
            n.mongo_uuid AS mongo_uuid,
            n.company_id AS company_id,
            c.name AS company_name,
            n.title AS title,
            n.description AS description,
            n.snippet AS snippet
        FROM {news_table} n
        JOIN {company_table} c ON c.id = n.company_id
        LEFT JOIN {evaluation_table} se
            ON se.article_id = n.id
        WHERE n.published_at >= %s
          AND n.published_at < %s
          AND se.id IS NULL
        ORDER BY n.published_at DESC
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql, (start_date, end_date))
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return [
        {
            "article_id": row[0],
            "mongo_uuid": row[1],
            "company_id": row[2],
            "company_name": row[3],
            "title": row[4],
            "description": row[5],
            "snippet": row[6],
        }
        for row in rows
    ]


def _build_prompt(company_name: str, articles: List[Dict[str, str]]) -> str:
    payload = json.dumps(articles, indent=2, ensure_ascii=True)
    return f"""
        You are an expert in financial news analysis.

        Company: {company_name}

        Analyze each article below and return a JSON array describing each one.

        Rules:
        1. Decide if the article is relevant to {company_name}.
        2. If relevant, assign a sentiment score 0-100 for the company's stock impact.
        If not relevant, sentiment_score = null.
        3. Always return the same UUID exactly.

        Return JSON matching this schema:
        {{
        "results": [
            {{
            "uuid": "exact-uuid-from-input",
            "relevant": true,
            "sentiment_score": 73.5
            }}
        ]
        }}

        Articles to analyze:
        {payload}
    """


def _parse_results(content: str) -> List[Dict[str, Any]]:
    if not content:
        return []
    text = content.strip()
    try:
        parsed = json.loads(text)
        return parsed.get("results", []) if isinstance(parsed, dict) else []
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return []
        try:
            parsed = json.loads(text[start : end + 1])
            return parsed.get("results", []) if isinstance(parsed, dict) else []
        except json.JSONDecodeError:
            return []


def _call_openrouter(
    session: requests.Session,
    company_name: str,
    articles: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    api_key = Variable.get(OPENROUTER_CFG.get("api_key_var"))
    model_name = OPENROUTER_CFG.get("model_name")

    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    payload: Dict[str, Any] = {
        "model": model_name,
        "messages": [{"role": "user", "content": _build_prompt(company_name, articles)}],
    }
    response_format = OPENROUTER_CFG.get("response_format")
    if response_format == "json_object":
        payload["response_format"] = {"type": "json_object"}
    elif response_format == "json_schema":
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "BatchEvaluation",
                "schema": {
                    "type": "object",
                    "properties": {
                        "results": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "uuid": {"type": "string"},
                                    "relevant": {"type": "boolean"},
                                    "sentiment_score": {"type": ["number", "null"]},
                                },
                                "required": ["uuid", "relevant", "sentiment_score"],
                                "additionalProperties": False,
                            },
                        }
                    },
                    "required": ["results"],
                    "additionalProperties": False,
                },
            },
        }

    response = request_json(
        OPENROUTER_CFG["api_url"],
        method="POST",
        headers=headers,
        json=payload,
        timeout=OPENROUTER_CFG.get("timeout", 60),
        retries=OPENROUTER_CFG.get("retries", 3),
        backoff=OPENROUTER_CFG.get("backoff", 2),
        retry_statuses=OPENROUTER_CFG.get("retry_statuses"),
        fatal_statuses=OPENROUTER_CFG.get("fatal_statuses"),
        session=session,
    )
    if not response:
        return []
    try:
        content = response["choices"][0]["message"]["content"]
    except (KeyError, TypeError, IndexError):
        return []
    return _parse_results(content)


def _upsert_evaluations(conn, records: List[Dict[str, Any]]) -> int:
    if not records:
        return 0
    evaluation_table = POSTGRES_CFG["evaluation_table"]
    sql = f"""
        INSERT INTO {evaluation_table} (
            article_id,
            model_name,
            is_relevant,
            sentiment_score,
            sentiment_label,
            evaluated_at
        )
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (article_id, model_name)
        DO UPDATE SET
            is_relevant = EXCLUDED.is_relevant,
            sentiment_score = EXCLUDED.sentiment_score,
            sentiment_label = EXCLUDED.sentiment_label,
            evaluated_at = EXCLUDED.evaluated_at
    """
    cursor = conn.cursor()
    try:
        cursor.executemany(
            sql,
            [
                (
                    rec["article_id"],
                    rec["model_name"],
                    rec["is_relevant"],
                    rec["sentiment_score"],
                    rec["sentiment_label"],
                )
                for rec in records
            ],
        )
        conn.commit()
        return cursor.rowcount or 0
    finally:
        cursor.close()


def _iter_company_batches(
    candidates: List[Dict[str, Any]],
) -> Iterable[tuple[str, List[Dict[str, Any]]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    max_per_company = int(EVAL_CFG.get("max_per_company", 50))

    for candidate in candidates:
        uuid = candidate.get("mongo_uuid")
        if not uuid:
            continue
        text = " ".join(
            [
                candidate.get("title") or "",
                candidate.get("description") or candidate.get("snippet") or "",
            ]
        ).strip()
        if not text:
            continue
        company_name = candidate.get("company_name") or "Unknown"
        if len(grouped[company_name]) >= max_per_company:
            continue
        grouped[company_name].append(
            {
                "article_id": candidate["article_id"],
                "uuid": uuid,
                "text": text,
            }
        )

    batch_size = int(EVAL_CFG.get("batch_size", 10))
    for company_name, articles in grouped.items():
        for i in range(0, len(articles), batch_size):
            yield company_name, articles[i : i + batch_size]


def _evaluate_and_store(logical_date: datetime) -> int:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CFG["postgres_conn_id"])
    conn = hook.get_conn()
    session = requests.Session()

    processed = 0
    try:
        candidates = _fetch_candidates(conn, logical_date)
        if not candidates:
            logging.info("No unevaluated articles found")
            return 0

        model_name = OPENROUTER_CFG.get("model_name")
        sleep_seconds = float(EVAL_CFG.get("sleep_seconds", 1))

        for company_name, batch in _iter_company_batches(candidates):
            payload = [{"uuid": item["uuid"], "text": item["text"]} for item in batch]
            results = _call_openrouter(session, company_name, payload)
            if not results:
                logging.info("No results for %s batch size %s", company_name, len(batch))
                time.sleep(sleep_seconds)
                continue

            uuid_map = {item["uuid"]: item["article_id"] for item in batch}
            records: List[Dict[str, Any]] = []
            for result in results:
                uuid = result.get("uuid")
                if uuid not in uuid_map:
                    continue
                is_relevant = bool(result.get("relevant"))
                score = result.get("sentiment_score")
                sentiment_score = float(score) if score is not None else None
                records.append(
                    {
                        "article_id": uuid_map[uuid],
                        "model_name": model_name,
                        "is_relevant": is_relevant,
                        "sentiment_score": sentiment_score,
                        "sentiment_label": _determine_sentiment(sentiment_score),
                    }
                )

            processed += _upsert_evaluations(conn, records)
            time.sleep(sleep_seconds)
    finally:
        session.close()
        conn.close()

    return processed


with DAG(
    dag_id="daily_sentiment_evaluations_dag",
    description="Evaluate news sentiment with AI",
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
    tags=["news", "sentiment", "evaluation"],
) as dag:

    @task
    def evaluate() -> Dict[str, Any]:
        context = get_current_context()
        logical_date = context["logical_date"]
        processed = _evaluate_and_store(logical_date)
        return {"processed": processed}

    evaluate_task = evaluate()
    trigger_aggregation = TriggerDagRunOperator(
        task_id="trigger_daily_aggregation",
        trigger_dag_id="daily_sentiment_aggregation_dag",
        execution_date="{{ logical_date }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    evaluate_task >> trigger_aggregation
