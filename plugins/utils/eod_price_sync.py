from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PGConnection

from plugins.utils.api_utils import request_json
from plugins.utils.db_utils import insert_dynamic_records


def get_adjusted_refresh_date(context: Mapping[str, Any]) -> datetime:
    """Keep scheduled/manual refresh cutoffs stable across retries, in UTC."""
    if context["dag_run"].run_type == "manual":
        reference_date = context["logical_date"]
    else:
        reference_date = context["data_interval_end"]
    return reference_date.astimezone(timezone.utc)


def build_date_range(
    logical_date: datetime,
    lookback_days: int,
    from_param: str,
    to_param: str,
) -> Dict[str, str]:
    """Build an inclusive source date range ending on the DAG logical date."""
    to_date = logical_date.date()
    from_date = (logical_date - timedelta(days=lookback_days)).date()
    return {
        from_param: from_date.strftime("%Y-%m-%d"),
        to_param: to_date.strftime("%Y-%m-%d"),
    }


def _parse_eodhd_records(payload: Any, ticker: str) -> List[Dict[str, Any]]:
    if payload is None:
        logging.warning("No EOD payload for %s", ticker)
        return []

    if isinstance(payload, dict):
        if payload.get("message") and payload.get("code"):
            logging.warning("EODHD error for %s: %s", ticker, payload.get("message"))
            return []
        records = payload.get("data", payload)
    else:
        records = payload

    if not records:
        logging.info("No EOD records for %s", ticker)
        return []
    if not isinstance(records, list):
        logging.warning("Unexpected EOD payload format for %s: %s", ticker, type(records))
        return []

    return [record for record in records if isinstance(record, dict)]


def sync_global_eod_one(
    ticker: str,
    company_id: int,
    logical_date: datetime,
    lookback_days: int,
    api_cfg: Dict[str, Any],
    db_cfg: Dict[str, Any],
    api_key: str,
    update_columns: Sequence[str],
    conn: PGConnection,
    provider_ticker: str | None = None,
) -> None:
    """Fetch EODHD history using the provider symbol and store the source ticker."""
    date_range = build_date_range(
        logical_date=logical_date,
        lookback_days=lookback_days,
        from_param=api_cfg.get("from_param", "from"),
        to_param=api_cfg.get("to_param", "to"),
    )
    params = {"fmt": api_cfg.get("fmt", "json"), **date_range}
    if api_key:
        params["api_token"] = api_key

    payload = request_json(
        api_cfg["url"].format(ticker=provider_ticker or ticker),
        params=params,
        timeout=api_cfg.get("timeout", 30),
    )
    records = _parse_eodhd_records(payload, ticker)
    if not records:
        return

    enriched = []
    for record in records:
        row = dict(record)
        row["company_id"] = company_id
        row["ticker"] = ticker
        enriched.append(row)

    insert_dynamic_records(
        postgres_conn_id=db_cfg["postgres_conn_id"],
        table=db_cfg["price_table"],
        records=enriched,
        columns_map=db_cfg["columns"],
        conflict_keys=db_cfg["conflict_keys"],
        on_conflict_do_update=True,
        update_columns=update_columns,
        conn=conn,
    )
    logging.info(
        "Processed %s EOD records for %s, from %s to %s",
        len(enriched), ticker, *date_range.values(),
    )
    time.sleep(api_cfg.get("throttle_seconds", 1))


def sync_vn_eod_one(
    code: str,
    logical_date: datetime,
    lookback_days: int,
    api_cfg: Dict[str, Any],
    db_cfg: Dict[str, Any],
    api_key: str,
    update_columns: Sequence[str],
    conn: PGConnection,
) -> None:
    """Fetch and upsert one Vietnam-stock EOD history range."""
    date_range = build_date_range(
        logical_date=logical_date,
        lookback_days=lookback_days,
        from_param="from-date",
        to_param="to-date",
    )
    params = {"code": code, **date_range}
    if api_key:
        params["apikey"] = api_key

    payload = request_json(
        api_cfg["url"],
        params=params,
        timeout=api_cfg.get("timeout", 30),
    )
    if payload is None:
        logging.warning("No EOD payload for %s", code)
        return

    records = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not records:
        logging.info("No EOD records for %s", code)
        return
    if not isinstance(records, list):
        logging.warning("Unexpected EOD payload format for %s: %s", code, type(records))
        return

    valid_records = [record for record in records if isinstance(record, dict)]
    if not valid_records:
        logging.info("No valid EOD records for %s", code)
        return

    insert_dynamic_records(
        postgres_conn_id=db_cfg["postgres_conn_id"],
        table=db_cfg["price_table"],
        records=valid_records,
        columns_map=db_cfg["columns"],
        conflict_keys=db_cfg["conflict_keys"],
        on_conflict_do_update=True,
        update_columns=update_columns,
        conn=conn,
    )
    logging.info("Processed %s EOD records for %s", len(valid_records), code)
    time.sleep(api_cfg.get("throttle_seconds", 1))
