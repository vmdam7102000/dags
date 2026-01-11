# dags/global_stock_dags/global_stock_fundamentals_ingest_dag.py
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config
from plugins.utils.db_utils import insert_dynamic_records

CONFIG = load_yaml_config("global_stock_configs/global_fundamentals.yml")["global_fundamentals"]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
CHUNK_SIZE = API_CFG.get("chunk_size", 200)
API_KEY = Variable.get(API_CFG["api_key_var"], default_var="")

HIGHLIGHTS_COLUMNS = [
    {"json_key": "snapshot_id", "column": "snapshot_id"},
    {"json_key": "date_of_data", "column": "date_of_data"},
    {"json_key": "market_capitalization", "column": "market_capitalization"},
    {"json_key": "market_capitalization_mln", "column": "market_capitalization_mln"},
    {"json_key": "ebitda", "column": "ebitda"},
    {"json_key": "pe_ratio", "column": "pe_ratio"},
    {"json_key": "peg_ratio", "column": "peg_ratio"},
    {"json_key": "wall_street_target_price", "column": "wall_street_target_price"},
    {"json_key": "book_value", "column": "book_value"},
    {"json_key": "dividend_share", "column": "dividend_share"},
    {"json_key": "dividend_yield", "column": "dividend_yield"},
    {"json_key": "earnings_share", "column": "earnings_share"},
    {"json_key": "eps_estimate_current_year", "column": "eps_estimate_current_year"},
    {"json_key": "eps_estimate_next_year", "column": "eps_estimate_next_year"},
    {"json_key": "eps_estimate_next_quarter", "column": "eps_estimate_next_quarter"},
    {"json_key": "eps_estimate_current_quarter", "column": "eps_estimate_current_quarter"},
    {"json_key": "most_recent_quarter", "column": "most_recent_quarter"},
    {"json_key": "profit_margin", "column": "profit_margin"},
    {"json_key": "operating_margin_ttm", "column": "operating_margin_ttm"},
    {"json_key": "return_on_assets_ttm", "column": "return_on_assets_ttm"},
    {"json_key": "return_on_equity_ttm", "column": "return_on_equity_ttm"},
    {"json_key": "revenue_ttm", "column": "revenue_ttm"},
    {"json_key": "revenue_per_share_ttm", "column": "revenue_per_share_ttm"},
    {"json_key": "quarterly_revenue_growth_yoy", "column": "quarterly_revenue_growth_yoy"},
    {"json_key": "gross_profit_ttm", "column": "gross_profit_ttm"},
    {"json_key": "diluted_eps_ttm", "column": "diluted_eps_ttm"},
    {"json_key": "quarterly_earnings_growth_yoy", "column": "quarterly_earnings_growth_yoy"},
]

VALUATION_COLUMNS = [
    {"json_key": "snapshot_id", "column": "snapshot_id"},
    {"json_key": "date_of_data", "column": "date_of_data"},
    {"json_key": "trailing_pe", "column": "trailing_pe"},
    {"json_key": "forward_pe", "column": "forward_pe"},
    {"json_key": "price_sales_ttm", "column": "price_sales_ttm"},
    {"json_key": "price_book_mrq", "column": "price_book_mrq"},
    {"json_key": "enterprise_value", "column": "enterprise_value"},
    {"json_key": "enterprise_value_revenue", "column": "enterprise_value_revenue"},
    {"json_key": "enterprise_value_ebitda", "column": "enterprise_value_ebitda"},
]

SHARES_STATS_COLUMNS = [
    {"json_key": "snapshot_id", "column": "snapshot_id"},
    {"json_key": "date_of_data", "column": "date_of_data"},
    {"json_key": "shares_outstanding", "column": "shares_outstanding"},
    {"json_key": "shares_float", "column": "shares_float"},
    {"json_key": "percent_insiders", "column": "percent_insiders"},
    {"json_key": "percent_institutions", "column": "percent_institutions"},
    {"json_key": "shares_short", "column": "shares_short"},
    {"json_key": "shares_short_prior_month", "column": "shares_short_prior_month"},
    {"json_key": "short_ratio", "column": "short_ratio"},
    {"json_key": "short_percent_outstanding", "column": "short_percent_outstanding"},
    {"json_key": "short_percent_float", "column": "short_percent_float"},
]

TECHNICALS_COLUMNS = [
    {"json_key": "snapshot_id", "column": "snapshot_id"},
    {"json_key": "date_of_data", "column": "date_of_data"},
    {"json_key": "beta", "column": "beta"},
    {"json_key": "week_52_high", "column": "week_52_high"},
    {"json_key": "week_52_low", "column": "week_52_low"},
    {"json_key": "ma_50_day", "column": "ma_50_day"},
    {"json_key": "ma_200_day", "column": "ma_200_day"},
    {"json_key": "shares_short", "column": "shares_short"},
    {"json_key": "shares_short_prior_month", "column": "shares_short_prior_month"},
    {"json_key": "short_ratio", "column": "short_ratio"},
    {"json_key": "short_percent", "column": "short_percent"},
]


def _parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        try:
            return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).date()
        except ValueError:
            return None
    return None


def _coerce_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _get_snapshot_date(payload: Dict[str, Any], logical_date: datetime) -> date:
    general = payload.get("General") if isinstance(payload, dict) else None
    updated_at = general.get("UpdatedAt") if isinstance(general, dict) else None
    parsed = _parse_date(updated_at)
    return parsed or logical_date.date()


def _fetch_fundamentals(ticker: str) -> Optional[Dict[str, Any]]:
    params = {"fmt": API_CFG.get("fmt", "json")}
    if API_KEY:
        params["api_token"] = API_KEY

    payload = request_json(
        API_CFG["url"].format(ticker=ticker),
        params=params,
        timeout=API_CFG.get("timeout", 30),
        retries=API_CFG.get("retries", 3),
        backoff=API_CFG.get("backoff", 1.5),
        retry_statuses=API_CFG.get("retry_statuses"),
        fatal_statuses=API_CFG.get("fatal_statuses"),
    )
    if payload is None:
        logging.warning("No fundamentals payload for %s", ticker)
        return None
    if isinstance(payload, dict):
        if payload.get("message") and payload.get("code"):
            logging.warning("EODHD error for %s: %s", ticker, payload.get("message"))
            return None
        return payload

    logging.warning("Unexpected fundamentals payload type for %s: %s", ticker, type(payload))
    return None


def _upsert_snapshot(conn, company_id: int, snapshot_date: date, payload: Dict[str, Any]) -> Optional[int]:
    conflict_keys = DB_CFG.get("snapshot_conflict_keys", ["company_id", "date_of_data"])
    conflict_sql = ", ".join(conflict_keys)
    insert_sql = f"""
        INSERT INTO {DB_CFG["snapshot_table"]} (company_id, date_of_data, raw_json)
        VALUES (%s, %s, %s)
        ON CONFLICT ({conflict_sql}) DO UPDATE
        SET raw_json = EXCLUDED.raw_json
        RETURNING id
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            insert_sql,
            (company_id, snapshot_date, json.dumps(payload, ensure_ascii=True)),
        )
        row = cursor.fetchone()
        conn.commit()
        return row[0] if row else None
    except Exception:
        conn.rollback()
        logging.exception("Failed to upsert fundamentals snapshot for company_id=%s", company_id)
        return None
    finally:
        cursor.close()


def _has_metrics(record: Dict[str, Any]) -> bool:
    for key, value in record.items():
        if key in {"snapshot_id", "date_of_data"}:
            continue
        if value is not None:
            return True
    return False


def _insert_section(
    conn,
    table: str,
    columns_map: List[Dict[str, str]],
    conflict_keys: List[str],
    record: Optional[Dict[str, Any]],
) -> None:
    if not record or not _has_metrics(record):
        return
    insert_dynamic_records(
        postgres_conn_id=DB_CFG["postgres_conn_id"],
        table=table,
        records=[record],
        columns_map=columns_map,
        conflict_keys=conflict_keys,
        on_conflict_do_update=True,
        conn=conn,
    )


def _build_highlights_record(
    highlights: Any,
    snapshot_id: int,
    snapshot_date: date,
) -> Optional[Dict[str, Any]]:
    if not isinstance(highlights, dict):
        return None
    return {
        "snapshot_id": snapshot_id,
        "date_of_data": snapshot_date,
        "market_capitalization": _coerce_int(highlights.get("MarketCapitalization")),
        "market_capitalization_mln": _coerce_decimal(highlights.get("MarketCapitalizationMln")),
        "ebitda": _coerce_int(highlights.get("EBITDA")),
        "pe_ratio": _coerce_decimal(highlights.get("PERatio")),
        "peg_ratio": _coerce_decimal(highlights.get("PEGRatio")),
        "wall_street_target_price": _coerce_decimal(highlights.get("WallStreetTargetPrice")),
        "book_value": _coerce_decimal(highlights.get("BookValue")),
        "dividend_share": _coerce_decimal(highlights.get("DividendShare")),
        "dividend_yield": _coerce_decimal(highlights.get("DividendYield")),
        "earnings_share": _coerce_decimal(highlights.get("EarningsShare")),
        "eps_estimate_current_year": _coerce_decimal(highlights.get("EPSEstimateCurrentYear")),
        "eps_estimate_next_year": _coerce_decimal(highlights.get("EPSEstimateNextYear")),
        "eps_estimate_next_quarter": _coerce_decimal(highlights.get("EPSEstimateNextQuarter")),
        "eps_estimate_current_quarter": _coerce_decimal(highlights.get("EPSEstimateCurrentQuarter")),
        "most_recent_quarter": _parse_date(highlights.get("MostRecentQuarter")),
        "profit_margin": _coerce_decimal(highlights.get("ProfitMargin")),
        "operating_margin_ttm": _coerce_decimal(highlights.get("OperatingMarginTTM")),
        "return_on_assets_ttm": _coerce_decimal(highlights.get("ReturnOnAssetsTTM")),
        "return_on_equity_ttm": _coerce_decimal(highlights.get("ReturnOnEquityTTM")),
        "revenue_ttm": _coerce_int(highlights.get("RevenueTTM")),
        "revenue_per_share_ttm": _coerce_decimal(highlights.get("RevenuePerShareTTM")),
        "quarterly_revenue_growth_yoy": _coerce_decimal(
            highlights.get("QuarterlyRevenueGrowthYOY")
        ),
        "gross_profit_ttm": _coerce_int(highlights.get("GrossProfitTTM")),
        "diluted_eps_ttm": _coerce_decimal(highlights.get("DilutedEpsTTM")),
        "quarterly_earnings_growth_yoy": _coerce_decimal(
            highlights.get("QuarterlyEarningsGrowthYOY")
        ),
    }


def _build_valuation_record(
    valuation: Any,
    snapshot_id: int,
    snapshot_date: date,
) -> Optional[Dict[str, Any]]:
    if not isinstance(valuation, dict):
        return None
    return {
        "snapshot_id": snapshot_id,
        "date_of_data": snapshot_date,
        "trailing_pe": _coerce_decimal(valuation.get("TrailingPE")),
        "forward_pe": _coerce_decimal(valuation.get("ForwardPE")),
        "price_sales_ttm": _coerce_decimal(valuation.get("PriceSalesTTM")),
        "price_book_mrq": _coerce_decimal(valuation.get("PriceBookMRQ")),
        "enterprise_value": _coerce_int(valuation.get("EnterpriseValue")),
        "enterprise_value_revenue": _coerce_decimal(valuation.get("EnterpriseValueRevenue")),
        "enterprise_value_ebitda": _coerce_decimal(valuation.get("EnterpriseValueEbitda")),
    }


def _build_shares_stats_record(
    shares_stats: Any,
    snapshot_id: int,
    snapshot_date: date,
) -> Optional[Dict[str, Any]]:
    if not isinstance(shares_stats, dict):
        return None
    return {
        "snapshot_id": snapshot_id,
        "date_of_data": snapshot_date,
        "shares_outstanding": _coerce_int(shares_stats.get("SharesOutstanding")),
        "shares_float": _coerce_int(shares_stats.get("SharesFloat")),
        "percent_insiders": _coerce_decimal(shares_stats.get("PercentInsiders")),
        "percent_institutions": _coerce_decimal(shares_stats.get("PercentInstitutions")),
        "shares_short": _coerce_int(shares_stats.get("SharesShort")),
        "shares_short_prior_month": _coerce_int(shares_stats.get("SharesShortPriorMonth")),
        "short_ratio": _coerce_decimal(shares_stats.get("ShortRatio")),
        "short_percent_outstanding": _coerce_decimal(shares_stats.get("ShortPercentOutstanding")),
        "short_percent_float": _coerce_decimal(shares_stats.get("ShortPercentFloat")),
    }


def _build_technicals_record(
    technicals: Any,
    snapshot_id: int,
    snapshot_date: date,
) -> Optional[Dict[str, Any]]:
    if not isinstance(technicals, dict):
        return None
    return {
        "snapshot_id": snapshot_id,
        "date_of_data": snapshot_date,
        "beta": _coerce_decimal(technicals.get("Beta")),
        "week_52_high": _coerce_decimal(technicals.get("52WeekHigh")),
        "week_52_low": _coerce_decimal(technicals.get("52WeekLow")),
        "ma_50_day": _coerce_decimal(technicals.get("50DayMA")),
        "ma_200_day": _coerce_decimal(technicals.get("200DayMA")),
        "shares_short": _coerce_int(technicals.get("SharesShort")),
        "shares_short_prior_month": _coerce_int(technicals.get("SharesShortPriorMonth")),
        "short_ratio": _coerce_decimal(technicals.get("ShortRatio")),
        "short_percent": _coerce_decimal(technicals.get("ShortPercent")),
    }


def _sync_one(ticker: str, company_id: int, logical_date: datetime, conn) -> None:
    payload = _fetch_fundamentals(ticker)
    if not payload:
        return

    snapshot_date = _get_snapshot_date(payload, logical_date)
    snapshot_id = _upsert_snapshot(conn, company_id, snapshot_date, payload)
    if snapshot_id is None:
        return

    highlights_record = _build_highlights_record(
        payload.get("Highlights"),
        snapshot_id,
        snapshot_date,
    )
    valuation_record = _build_valuation_record(
        payload.get("Valuation"),
        snapshot_id,
        snapshot_date,
    )
    shares_stats_record = _build_shares_stats_record(
        payload.get("SharesStats"),
        snapshot_id,
        snapshot_date,
    )
    technicals_record = _build_technicals_record(
        payload.get("Technicals"),
        snapshot_id,
        snapshot_date,
    )

    _insert_section(
        conn,
        DB_CFG["highlights_table"],
        HIGHLIGHTS_COLUMNS,
        DB_CFG.get("highlights_conflict_keys", ["snapshot_id"]),
        highlights_record,
    )
    _insert_section(
        conn,
        DB_CFG["valuation_table"],
        VALUATION_COLUMNS,
        DB_CFG.get("valuation_conflict_keys", ["snapshot_id"]),
        valuation_record,
    )
    _insert_section(
        conn,
        DB_CFG["shares_stats_table"],
        SHARES_STATS_COLUMNS,
        DB_CFG.get("shares_stats_conflict_keys", ["snapshot_id"]),
        shares_stats_record,
    )
    _insert_section(
        conn,
        DB_CFG["technicals_table"],
        TECHNICALS_COLUMNS,
        DB_CFG.get("technicals_conflict_keys", ["snapshot_id"]),
        technicals_record,
    )

    time.sleep(API_CFG.get("throttle_seconds", 1))


with DAG(
    dag_id="sync_global_stock_fundamentals",
    description="Sync global stock fundamentals from EODHD API to Postgres",
    default_args={
        "owner": "global-stock-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval=CONFIG.get("schedule", "0 4 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock", "global", "fundamentals", "eodhd"],
) as dag:

    @task
    def get_companies() -> List[Dict[str, Any]]:
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        cursor = conn.cursor()
        id_col = DB_CFG.get("company_id_column", "id")
        ticker_col = DB_CFG.get("company_ticker_column", "ticker")
        table = DB_CFG["company_table"]
        where_clause = ""
        if DB_CFG.get("only_active", True):
            where_clause = " WHERE is_active = TRUE"
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
        return [companies[i : i + CHUNK_SIZE] for i in range(0, len(companies), CHUNK_SIZE)]

    @task
    def sync_company_batch(companies: List[Dict[str, Any]]) -> None:
        context = get_current_context()
        logical_date = context["logical_date"]
        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        conn = hook.get_conn()
        try:
            for company in companies:
                ticker = company.get("ticker")
                company_id = company.get("company_id")
                if not ticker or company_id is None:
                    continue
                _sync_one(ticker, company_id, logical_date, conn)
        finally:
            conn.close()

    companies = get_companies()
    company_batches = chunk_companies(companies)
    sync_company_batch.expand(companies=company_batches)
