# dags/global_stock_dags/sync_marketaux_news_dag.py
from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.db_utils import insert_dynamic_records

CONFIG = load_yaml_config("global_stock_configs/marketaux_news.yml")["marketaux_news"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
MONGO_CFG = CONFIG["mongo"]

API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")
CHUNK_SIZE = API_CFG.get("chunk_size", 50)

GENERIC_BLOCKLIST = {
    "arthur",
    "admiral",
    "advanced",
    "auto",
    "automatic",
    "barry",
    "bentley",
    "block",
    "booking",
    "ge",
    "gartner",
    "brown",
    "cadence",
    "car",
    "carnival",
    "check",
    "coinbase",
    "compass",
    "descartes",
    "deutsche",
    "dominos",
    "east",
    "edwards",
    "evolution",
    "fair",
    "first",
    "fisher",
    "gallagher",
    "hannover",
    "intact",
    "heico",
    "japan",
    "mitsubishi",
    "new",
    "nice",
    "recruit",
    "ross",
    "royal",
    "s&p",
    "sartorius",
    "merck",
    "lotus",
    "otsuka",
    "old",
    "nippon",
    "schindler",
    "schneider",
    "screen",
    "siemens",
    "singapore",
    "smith",
    "start",
    "republic",
    "swedish",
    "tokyo",
    "trade",
    "tyler",
    "united",
    "universal",
    "vinci",
    "visa",
    "waste",
    "west",
    "on",
    "northern",
    "adobe",
    "be",
    "cloudfare",
    "costar",
    "capital",
    "element",
    "fast",
    "goodman",
    "grab",
    "ww",
    "american",
    "trend",
    "hilton",
    "hydro",
    "m",
    "investment",
    "live",
    "lundin",
    "z",
    "mastercard",
    "microsoft",
    "monday",
    "monster",
    "the",
    "o",
    "pan",
    "pro",
    "progressive",
    "pure",
    "rational",
    "restaurant",
    "charles",
    "sigma",
    "george",
}


class MarketauxLimitReached(RuntimeError):
    pass


class MarketauxPaymentRequired(RuntimeError):
    pass


def _normalize_company_name(name: str) -> str:
    cleaned = name.lower().replace("&", "and")
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned)
    tokens = [token for token in cleaned.split() if token and token not in GENERIC_BLOCKLIST]
    return " ".join(tokens)


def _build_search_terms(name: str) -> List[str]:
    normalized = _normalize_company_name(name)
    if not normalized:
        return [name.strip()] if name.strip() else []
    tokens = normalized.split()
    terms = [normalized]
    if len(tokens) >= 2:
        terms.append(" ".join(tokens[:2]))
    max_terms = API_CFG.get("max_search_terms", 2)
    deduped = list(dict.fromkeys(terms))
    return deduped[:max_terms]


def _parse_published_at(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _ensure_uuid(article: Dict[str, Any]) -> str:
    existing = article.get("uuid")
    if existing:
        return str(existing)
    url = article.get("url") or ""
    published_at = article.get("published_at") or ""
    base = f"{url}|{published_at}"
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()
    article["uuid"] = digest
    return digest


def _extract_source(article: Dict[str, Any]) -> Optional[str]:
    source = article.get("source")
    if isinstance(source, dict):
        return source.get("name") or source.get("id")
    return source


def _get_mongo_collection() -> tuple[MongoClient, Any]:
    uri_var = MONGO_CFG.get("mongo_uri_var")
    uri = Variable.get(uri_var, default_var="") if uri_var else ""
    if not uri:
        uri = MONGO_CFG.get("mongo_uri", "")
    if not uri:
        raise ValueError("Missing MongoDB URI configuration")
    client = MongoClient(uri)
    db = client[MONGO_CFG["database"]]
    collection = db[MONGO_CFG["collection"]]
    collection.create_index("uuid", unique=True)
    collection.create_index([("company_ticker", 1), ("published_at", -1)])
    collection.create_index([("company_id", 1), ("published_at", -1)])
    return client, collection


def _get_api_usage(conn, api_name: str, usage_date: datetime.date) -> int:
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT request_count FROM raw_global_stock_data.api_usage WHERE api_name = %s AND date = %s",
            (api_name, usage_date),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0
    except Exception as exc:
        logging.warning("Failed to read api_usage: %s", exc)
        return 0
    finally:
        if cursor:
            cursor.close()


def _increment_api_usage(conn, api_name: str, usage_date: datetime.date, count: int) -> None:
    if count <= 0:
        return
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO raw_global_stock_data.api_usage (api_name, date, request_count, last_request_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (api_name, date)
            DO UPDATE SET
                request_count = api_usage.request_count + EXCLUDED.request_count,
                last_request_at = EXCLUDED.last_request_at
            """,
            (api_name, usage_date, count),
        )
        conn.commit()
    except Exception as exc:
        logging.warning("Failed to update api_usage: %s", exc)
        conn.rollback()
    finally:
        if cursor:
            cursor.close()


def _init_request_budget(conn, logical_date: datetime) -> Dict[str, Optional[int]]:
    limit = API_CFG.get("request_limit")
    if not limit:
        return {"limit": None, "remaining": None, "count": 0}
    api_name = API_CFG.get("api_name", "marketaux")
    used = _get_api_usage(conn, api_name, logical_date.date())
    remaining = max(0, int(limit) - used)
    return {"limit": int(limit), "remaining": remaining, "count": 0}


def _request_marketaux_json(
    url: str,
    params: Dict[str, Any],
    session: requests.Session,
    budget: Dict[str, Optional[int]],
) -> Optional[Any]:
    remaining = budget.get("remaining")
    if remaining is not None and remaining <= 0:
        raise MarketauxLimitReached("Marketaux daily limit reached")
    try:
        payload = request_json(
            url,
            params=params,
            timeout=API_CFG.get("timeout", 30),
            retries=API_CFG.get("retries", 3),
            backoff=API_CFG.get("backoff", 1.5),
            retry_statuses=API_CFG.get("retry_statuses"),
            fatal_statuses=API_CFG.get("fatal_statuses"),
            session=session,
        )
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response else None
        if status_code == 402:
            raise MarketauxPaymentRequired("Marketaux payment required") from exc
        raise
    budget["count"] = int(budget.get("count", 0)) + 1
    if remaining is not None:
        budget["remaining"] = max(0, remaining - 1)
    return payload


def _extract_articles(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        data = payload.get("data", [])
    elif isinstance(payload, list):
        data = payload
    else:
        return []
    return [item for item in data if isinstance(item, dict)]


def _paginate_news(
    url: str,
    params: Dict[str, Any],
    session: requests.Session,
    budget: Dict[str, Optional[int]],
) -> Iterable[List[Dict[str, Any]]]:
    page_param = API_CFG.get("page_param", "page")
    limit_param = API_CFG.get("limit_param", "limit")
    limit = API_CFG.get("limit", 50)
    max_pages = API_CFG.get("max_pages", 10)

    page = 1
    while page <= max_pages:
        page_params = dict(params)
        page_params[page_param] = page
        page_params[limit_param] = limit
        payload = _request_marketaux_json(url, page_params, session, budget)
        data = _extract_articles(payload)
        if not data:
            break
        yield data
        next_page = None
        if isinstance(payload, dict):
            meta = payload.get("meta") or payload.get("pagination") or {}
            next_page = meta.get("next_page") or meta.get("next")
        if next_page:
            page = int(next_page)
            continue
        if len(data) < limit:
            break
        page += 1


def _get_entity_symbols(
    name: str,
    session: requests.Session,
    budget: Dict[str, Optional[int]],
) -> List[str]:
    if not API_CFG.get("enable_entity_search", True):
        return []
    if not API_KEY:
        return []
    params = {
        API_CFG.get("api_key_param", "api_token"): API_KEY,
        API_CFG.get("search_param", "search"): name,
        API_CFG.get("limit_param", "limit"): API_CFG.get("entity_search_limit", 10),
    }
    payload = _request_marketaux_json(
        API_CFG["entity_search_url"],
        params,
        session,
        budget,
    )
    symbols: set[str] = set()
    for item in _extract_articles(payload):
        for key in ("symbol", "ticker", "stock_symbol", "exchange_symbol"):
            value = item.get(key)
            if value:
                symbols.add(str(value))
        entity = item.get("entity")
        if isinstance(entity, dict):
            for key in ("symbol", "ticker", "stock_symbol", "exchange_symbol"):
                value = entity.get(key)
                if value:
                    symbols.add(str(value))
    return sorted(symbols)


def _build_symbols(
    ticker: Optional[str],
    company_name: str,
    session: requests.Session,
    budget: Dict[str, Optional[int]],
) -> List[str]:
    symbols: List[str] = []
    if ticker:
        symbols.append(ticker)
    symbols.extend(_get_entity_symbols(company_name, session, budget))
    seen = set()
    deduped: List[str] = []
    for symbol in symbols:
        if symbol and symbol not in seen:
            seen.add(symbol)
            deduped.append(symbol)
    return deduped


def _build_date_filters(logical_date: datetime, days_back: int) -> Dict[str, str]:
    end_date = logical_date.date()
    start_date = end_date - timedelta(days=days_back)
    return {
        API_CFG.get("published_after_param", "published_after"): start_date.strftime("%Y-%m-%d"),
        API_CFG.get("published_before_param", "published_before"): end_date.strftime("%Y-%m-%d"),
    }


def _fetch_news_for_company(
    company_name: str,
    symbols: List[str],
    days_back: int,
    logical_date: datetime,
    session: requests.Session,
    budget: Dict[str, Optional[int]],
) -> List[Dict[str, Any]]:
    if not API_KEY:
        logging.warning("Marketaux API key is missing")
        return []
    base_params = {
        API_CFG.get("api_key_param", "api_token"): API_KEY,
        **_build_date_filters(logical_date, days_back),
    }
    language = API_CFG.get("language")
    if language:
        base_params["language"] = language

    articles: Dict[str, Dict[str, Any]] = {}

    if symbols:
        symbols_param = API_CFG.get("symbols_param", "symbols")
        params = dict(base_params)
        params[symbols_param] = ",".join(symbols)
        for page in _paginate_news(API_CFG["news_url"], params, session, budget):
            for article in page:
                uuid = _ensure_uuid(article)
                articles[uuid] = article

    if not symbols or API_CFG.get("enable_search_fallback", True):
        search_terms = _build_search_terms(company_name)
        search_param = API_CFG.get("search_param", "search")
        for term in search_terms:
            params = dict(base_params)
            params[search_param] = term
            for page in _paginate_news(API_CFG["news_url"], params, session, budget):
                for article in page:
                    uuid = _ensure_uuid(article)
                    articles[uuid] = article

    return list(articles.values())


def _upsert_raw_articles(
    collection,
    articles: Sequence[Dict[str, Any]],
    company_id: int,
    ticker: Optional[str],
    company_name: str,
    fetched_at: datetime,
) -> int:
    operations: List[UpdateOne] = []
    for article in articles:
        uuid = _ensure_uuid(article)
        published_at = article.get("published_at")
        if not published_at:
            continue
        doc = dict(article)
        doc.update(
            {
                "uuid": uuid,
                "company_id": company_id,
                "company_ticker": ticker,
                "company_name": company_name,
                "fetched_at": fetched_at,
            }
        )
        operations.append(
            UpdateOne({"uuid": uuid}, {"$setOnInsert": doc}, upsert=True)
        )

    if not operations:
        return 0

    try:
        result = collection.bulk_write(operations, ordered=False)
        return int(result.upserted_count or 0)
    except BulkWriteError as exc:
        upserted = exc.details.get("nUpserted", 0) if exc.details else 0
        logging.warning("Bulk write had duplicate keys: %s", exc)
        return int(upserted)


def _prepare_postgres_records(
    articles: Sequence[Dict[str, Any]],
    company_id: int,
    fetched_at: datetime,
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for article in articles:
        published_at = _parse_published_at(article.get("published_at"))
        if not published_at:
            continue
        records.append(
            {
                "mongo_uuid": _ensure_uuid(article),
                "company_id": company_id,
                "published_at": published_at,
                "source": _extract_source(article),
                "language": article.get("language"),
                "title": article.get("title"),
                "description": article.get("description"),
                "snippet": article.get("snippet"),
                "url": article.get("url"),
                "fetched_at": fetched_at,
            }
        )
    return records


def _calculate_fetch_days(
    last_published_at: Optional[datetime],
    logical_date: datetime,
) -> Optional[int]:
    if not last_published_at:
        return API_CFG.get("lookback_days", 30)
    latest = last_published_at.replace(tzinfo=None)
    logical = logical_date.replace(tzinfo=None)
    days_diff = (logical.date() - latest.date()).days
    if days_diff <= 0:
        return None
    return max(1, days_diff + 1)


def _sync_one(
    company: Dict[str, Any],
    logical_date: datetime,
    conn,
    collection,
    session: requests.Session,
    budget: Dict[str, Optional[int]],
) -> None:
    company_id = company.get("company_id")
    company_name = company.get("name")
    ticker = company.get("ticker")
    last_published_at = company.get("last_published_at")

    if not company_id or not company_name:
        return

    fetch_days = _calculate_fetch_days(last_published_at, logical_date)
    if fetch_days is None:
        logging.info("%s is up to date, skipping", company_name)
        return

    symbols = _build_symbols(ticker, company_name, session, budget)
    articles = _fetch_news_for_company(
        company_name,
        symbols,
        fetch_days,
        logical_date,
        session,
        budget,
    )

    if not articles:
        logging.info("No articles for %s", company_name)
        return

    fetched_at = datetime.utcnow()
    inserted_raw = _upsert_raw_articles(
        collection,
        articles,
        company_id,
        ticker,
        company_name,
        fetched_at,
    )

    records = _prepare_postgres_records(articles, company_id, fetched_at)
    if records:
        insert_dynamic_records(
            postgres_conn_id=DB_CFG["postgres_conn_id"],
            table=DB_CFG["news_table"],
            records=records,
            columns_map=DB_CFG["columns"],
            conflict_keys=DB_CFG["conflict_keys"],
            on_conflict_do_update=False,
            conn=conn,
        )

    logging.info(
        "Fetched %s articles for %s (raw inserted: %s, pg staged: %s)",
        len(articles),
        company_name,
        inserted_raw,
        len(records),
    )

    time.sleep(API_CFG.get("throttle_seconds", 1))


with DAG(
    dag_id="sync_marketaux_news_dag",
    description="Fetch Marketaux news into MongoDB + Postgres references",
    default_args={
        "owner": "news-pipeline",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["news", "marketaux", "mongo", "postgres"],
) as dag:

    @task
    def get_companies() -> List[Dict[str, Any]]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        cursor = conn.cursor()

        company_table = DB_CFG["company_table"]
        news_table = DB_CFG["news_table"]
        id_col = DB_CFG.get("company_id_column", "id")
        ticker_col = DB_CFG.get("company_ticker_column", "ticker")
        name_col = DB_CFG.get("company_name_column", "name")
        universe_col = DB_CFG.get("company_universe_column", "universe")

        where_parts: List[str] = []
        params: List[Any] = []
        if DB_CFG.get("only_active", True):
            where_parts.append("c.is_active = TRUE")
        universe = DB_CFG.get("universe")
        if universe:
            where_parts.append(f"c.{universe_col} = %s")
            params.append(universe)
        where_clause = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

        limit_clause = ""
        company_limit = DB_CFG.get("company_limit")
        if company_limit:
            limit_clause = "LIMIT %s"
            params.append(int(company_limit))

        try:
            cursor.execute(
                f"""
                SELECT
                    c.{id_col} AS company_id,
                    c.{ticker_col} AS ticker,
                    c.{name_col} AS name,
                    MAX(n.published_at) AS last_published_at
                FROM {company_table} c
                LEFT JOIN {news_table} n ON n.company_id = c.{id_col}
                {where_clause}
                GROUP BY c.{id_col}, c.{ticker_col}, c.{name_col}
                ORDER BY c.{id_col}
                {limit_clause}
                """,
                params,
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

        companies = [
            {
                "company_id": row[0],
                "ticker": row[1],
                "name": row[2],
                "last_published_at": row[3],
            }
            for row in rows
        ]
        logging.info("Fetched %s companies for news sync", len(companies))
        return companies

    @task
    def chunk_companies(companies: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        return [companies[i : i + CHUNK_SIZE] for i in range(0, len(companies), CHUNK_SIZE)]

    @task
    def sync_company_batch(companies: List[Dict[str, Any]]) -> None:
        if not companies:
            return
        context = get_current_context()
        logical_date = context["logical_date"]

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        mongo_client, collection = _get_mongo_collection()
        session = requests.Session()

        budget = _init_request_budget(conn, logical_date)
        api_name = API_CFG.get("api_name", "marketaux")

        try:
            for company in companies:
                try:
                    _sync_one(company, logical_date, conn, collection, session, budget)
                except MarketauxLimitReached:
                    logging.warning("%s API limit reached; stopping batch", api_name)
                    break
                except MarketauxPaymentRequired as exc:
                    logging.error(
                        "Marketaux payment required; stopping batch",
                    )
                    break
        finally:
            _increment_api_usage(conn, api_name, logical_date.date(), budget.get("count", 0))
            session.close()
            mongo_client.close()
            conn.close()

    companies = get_companies()
    company_batches = chunk_companies(companies)
    sync_company_batch.expand(companies=company_batches)
