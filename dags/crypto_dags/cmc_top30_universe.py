from __future__ import annotations

import calendar
import hashlib
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Mapping, Optional


UTC = timezone.utc
TOP_N = 50
SOURCE_PUBLICATION_DELAY = timedelta(minutes=30)


def _parse_date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an ISO date (YYYY-MM-DD)") from exc


def _subtract_years(value: date, years: int) -> date:
    try:
        return value.replace(year=value.year - years)
    except ValueError:
        return value.replace(year=value.year - years, day=28)


def is_month_end(value: date) -> bool:
    return value.day == calendar.monthrange(value.year, value.month)[1]


def month_end(value: date) -> date:
    return value.replace(day=calendar.monthrange(value.year, value.month)[1])


def previous_month_end(value: date) -> date:
    return value.replace(day=1) - timedelta(days=1)


def month_ends_between(start: date, end: date) -> List[date]:
    """Return every calendar month-end in the inclusive date range."""
    if start > end:
        return []

    current = month_end(start)
    values: List[date] = []
    while current <= end:
        if current >= start:
            values.append(current)
        current = month_end(current + timedelta(days=1))
    return values


def latest_available_daily_snapshot_date(now: datetime) -> date:
    """Return the newest CMC EOD snapshot expected to be published."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    publication_clock = now.astimezone(UTC) - SOURCE_PUBLICATION_DELAY
    return publication_clock.date() - timedelta(days=1)


def latest_available_month_end(now: datetime) -> date:
    """Return the newest published CMC snapshot that is a month-end."""
    newest_daily = latest_available_daily_snapshot_date(now)
    candidate = month_end(newest_daily)
    if candidate > newest_daily:
        return previous_month_end(newest_daily)
    return candidate


def source_available_at(snapshot_date: date) -> datetime:
    """CMC publishes an EOD snapshot 30 minutes into the following UTC day."""
    return datetime.combine(
        snapshot_date + timedelta(days=1),
        time(0, 30),
        tzinfo=UTC,
    )


def effective_from(snapshot_date: date) -> date:
    """Return the DATE-level universe effective date selected for this project."""
    return snapshot_date + timedelta(days=1)


def requested_snapshot_dates(
    run_conf: Optional[Mapping[str, Any]],
    *,
    now: datetime,
    history_years: int = 3,
) -> List[date]:
    """Resolve month-end targets within the rolling CMC Builder window.

    A scheduled run (no explicit range) returns every accessible month-end so
    the storage layer can bootstrap an empty table and repair gaps. Manual
    ranges must name month-end dates exactly and are inclusive.
    """
    if history_years <= 0:
        raise ValueError("history_years must be positive")

    conf = dict(run_conf or {})
    newest_daily = latest_available_daily_snapshot_date(now)
    newest_month_end = latest_available_month_end(now)
    earliest_supported = _subtract_years(newest_daily, history_years)
    start_raw = conf.get("start_date")
    end_raw = conf.get("end_date")

    if start_raw is None and end_raw is None:
        return month_ends_between(earliest_supported, newest_month_end)

    start = _parse_date(start_raw or end_raw, "start_date")
    end = _parse_date(end_raw or start_raw, "end_date")
    if start > end:
        raise ValueError("start_date must be on or before end_date")
    if not is_month_end(start):
        raise ValueError(f"start_date {start.isoformat()} must be a calendar month-end")
    if not is_month_end(end):
        raise ValueError(f"end_date {end.isoformat()} must be a calendar month-end")
    if end > newest_month_end:
        raise ValueError(
            f"end_date {end.isoformat()} is not available as a completed month-end; "
            f"newest expected CMC month-end is {newest_month_end.isoformat()}"
        )
    if start < earliest_supported:
        raise ValueError(
            f"start_date {start.isoformat()} is outside the CMC Builder "
            f"{history_years}-year history window ({earliest_supported.isoformat()} onward)"
        )
    return month_ends_between(start, end)


def parse_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def historical_listing_params(snapshot_date: date) -> Dict[str, Any]:
    if not is_month_end(snapshot_date):
        raise ValueError(
            f"snapshot_date {snapshot_date.isoformat()} must be a calendar month-end"
        )
    return {
        "date": snapshot_date.isoformat(),
        "start": 1,
        "limit": TOP_N,
        "convert": "USD",
        "sort": "cmc_rank",
        "sort_dir": "asc",
        "cryptocurrency_type": "all",
        "aux": (
            "platform,tags,date_added,circulating_supply,total_supply,"
            "max_supply,cmc_rank,num_market_pairs"
        ),
    }


def _decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _integer(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _timestamp(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _quote_usd(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    quote = entry.get("quote")
    if not isinstance(quote, Mapping):
        return {}
    usd = quote.get("USD")
    return usd if isinstance(usd, Mapping) else {}


def normalize_historical_listing(
    payload: Mapping[str, Any],
    *,
    snapshot_date: date,
    collected_at: datetime,
) -> Dict[str, Any]:
    if not is_month_end(snapshot_date):
        raise ValueError(
            f"snapshot_date {snapshot_date.isoformat()} must be a calendar month-end"
        )
    if collected_at.tzinfo is None:
        raise ValueError("collected_at must be timezone-aware")

    status = payload.get("status")
    if not isinstance(status, Mapping):
        raise ValueError("CMC payload is missing a status object")

    error_code = _integer(status.get("error_code")) or 0
    error_message = str(status.get("error_message") or "").strip()
    if error_code or error_message:
        raise ValueError(
            f"CMC API error for {snapshot_date.isoformat()}: "
            f"code={error_code} message={error_message or 'unknown'}"
        )

    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("CMC historical listing payload data must be an array")
    if len(data) != TOP_N:
        raise ValueError(
            f"CMC snapshot {snapshot_date.isoformat()} returned {len(data)} rows; "
            f"expected exactly {TOP_N}"
        )

    rows: List[Dict[str, Any]] = []
    seen_ids = set()
    seen_ranks = set()
    for entry in data:
        if not isinstance(entry, Mapping):
            raise ValueError("CMC historical listing contains a non-object row")

        cmc_id = _integer(entry.get("id"))
        cmc_rank = _integer(entry.get("cmc_rank"))
        symbol = str(entry.get("symbol") or "").strip()
        name = str(entry.get("name") or "").strip()
        slug = str(entry.get("slug") or "").strip()
        if not cmc_id or not cmc_rank or not symbol or not name or not slug:
            raise ValueError("CMC historical listing row is missing identity or rank fields")
        if cmc_id in seen_ids:
            raise ValueError(f"Duplicate CMC id {cmc_id} in snapshot")
        if cmc_rank in seen_ranks:
            raise ValueError(f"Duplicate CMC rank {cmc_rank} in snapshot")
        seen_ids.add(cmc_id)
        seen_ranks.add(cmc_rank)

        quote = _quote_usd(entry)
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "cmc_id": cmc_id,
                "cmc_rank": cmc_rank,
                "symbol": symbol.upper(),
                "name": name,
                "slug": slug,
                "price_usd": _decimal(quote.get("price")),
                "market_cap_usd": _decimal(quote.get("market_cap")),
                "volume_24h_usd": _decimal(quote.get("volume_24h")),
                "circulating_supply": _decimal(entry.get("circulating_supply")),
                "total_supply": _decimal(entry.get("total_supply")),
                "max_supply": _decimal(entry.get("max_supply")),
                "num_market_pairs": _integer(entry.get("num_market_pairs")),
                "asset_last_updated_at": _timestamp(entry.get("last_updated")),
                "quote_last_updated_at": _timestamp(quote.get("last_updated")),
                "platform": entry.get("platform"),
                "tags": entry.get("tags") or [],
                "raw_payload": dict(entry),
            }
        )

    expected_ranks = set(range(1, TOP_N + 1))
    if seen_ranks != expected_ranks:
        missing = sorted(expected_ranks - seen_ranks)
        unexpected = sorted(seen_ranks - expected_ranks)
        raise ValueError(
            f"CMC snapshot ranks must be 1..{TOP_N}; "
            f"missing={missing} unexpected={unexpected}"
        )

    canonical_payload = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return {
        "snapshot_date": snapshot_date,
        "source_available_at": source_available_at(snapshot_date),
        "effective_from": effective_from(snapshot_date),
        "source_status_at": _timestamp(status.get("timestamp")),
        "collected_at": collected_at.astimezone(UTC),
        "api_credit_count": _integer(status.get("credit_count")),
        "payload_sha256": hashlib.sha256(canonical_payload).hexdigest(),
        "rows": sorted(rows, key=lambda row: row["cmc_rank"]),
    }
