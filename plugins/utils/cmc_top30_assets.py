from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


UTC = timezone.utc
DEFAULT_DATA_START_AT = datetime(2013, 1, 1, tzinfo=UTC)
STABLECOIN_TAG_FRAGMENTS = ("stablecoin", "stablecoins")
WRAPPED_TAG_FRAGMENTS = ("wrapped",)


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalized_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def normalize_tags(value: Any) -> List[str]:
    if not isinstance(value, (list, tuple, set)):
        return []
    return sorted(
        {
            _normalized_text(item)
            for item in value
            if _normalized_text(item)
        }
    )


def classify_asset(
    tags: Any,
    *,
    stablecoin_override: Optional[bool] = None,
    wrapped_override: Optional[bool] = None,
) -> Tuple[bool, bool]:
    """Classify only from CMC tags plus explicit, reviewed overrides."""
    normalized = normalize_tags(tags)
    stablecoin = any(
        fragment in tag
        for tag in normalized
        for fragment in STABLECOIN_TAG_FRAGMENTS
    )
    wrapped = any(
        fragment in tag
        for tag in normalized
        for fragment in WRAPPED_TAG_FRAGMENTS
    )
    if stablecoin_override is not None:
        stablecoin = bool(stablecoin_override)
    if wrapped_override is not None:
        wrapped = bool(wrapped_override)
    return stablecoin, wrapped


def _as_utc(value: Any) -> datetime:
    if isinstance(value, date) and not isinstance(value, datetime):
        value = datetime.combine(value, datetime.min.time())
    if not isinstance(value, datetime):
        raise ValueError(f"Expected a date/time value, got {value!r}")
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _override(mapping: Mapping[Any, Any], symbol: str) -> Any:
    if symbol in mapping:
        return mapping[symbol]
    normalized = {
        normalize_symbol(key): value
        for key, value in mapping.items()
        if normalize_symbol(key)
    }
    return normalized.get(symbol)


def build_canonical_symbol_targets(
    snapshot_rows: Sequence[Mapping[str, Any]],
    *,
    default_data_start_at: datetime = DEFAULT_DATA_START_AT,
    stablecoin_overrides: Optional[Mapping[Any, Any]] = None,
    wrapped_overrides: Optional[Mapping[Any, Any]] = None,
    data_start_overrides: Optional[Mapping[Any, Any]] = None,
) -> List[Dict[str, Any]]:
    """Reduce raw CMC history to one current symbol per transient ``cmc_id``.

    If an asset changed symbol, its new symbol starts at the first snapshot in
    the latest contiguous run of that symbol. The former symbol is deliberately
    not carried into the Phase-1 target set.
    """
    stablecoin_overrides = stablecoin_overrides or {}
    wrapped_overrides = wrapped_overrides or {}
    data_start_overrides = data_start_overrides or {}
    default_start = _as_utc(default_data_start_at)

    by_cmc_id: Dict[int, List[Dict[str, Any]]] = {}
    for raw in snapshot_rows:
        cmc_id = int(raw["cmc_id"])
        symbol = normalize_symbol(raw.get("symbol"))
        if not symbol:
            raise ValueError(f"CMC asset {cmc_id} has an empty symbol")
        available_at = _as_utc(raw["source_available_at"])
        by_cmc_id.setdefault(cmc_id, []).append(
            {
                "cmc_id": cmc_id,
                "symbol": symbol,
                "name": str(raw.get("name") or symbol).strip(),
                "tags": raw.get("tags") or [],
                "source_available_at": available_at,
                "snapshot_date": raw.get("snapshot_date"),
            }
        )

    candidates: List[Dict[str, Any]] = []
    for cmc_id, history in sorted(by_cmc_id.items()):
        history.sort(
            key=lambda row: (
                row["source_available_at"],
                row.get("snapshot_date") or date.min,
            )
        )
        latest = history[-1]
        symbol = latest["symbol"]
        distinct_symbols = {row["symbol"] for row in history}

        if len(distinct_symbols) == 1:
            data_start_at = default_start
        else:
            current_run = len(history) - 1
            while current_run > 0 and history[current_run - 1]["symbol"] == symbol:
                current_run -= 1
            data_start_at = history[current_run]["source_available_at"]

        configured_start = _override(data_start_overrides, symbol)
        if configured_start is not None:
            if isinstance(configured_start, str):
                configured_start = datetime.fromisoformat(
                    configured_start.replace("Z", "+00:00")
                )
            data_start_at = _as_utc(configured_start)

        stablecoin, wrapped = classify_asset(
            latest["tags"],
            stablecoin_override=_override(stablecoin_overrides, symbol),
            wrapped_override=_override(wrapped_overrides, symbol),
        )
        candidates.append(
            {
                # Kept only in memory so symbol reuse can be detected. It is
                # intentionally not written to cmc_top30_symbol_targets.
                "cmc_id": cmc_id,
                "symbol": symbol,
                "name": latest["name"],
                "data_start_at": data_start_at,
                "is_stablecoin": stablecoin,
                "is_wrapped": wrapped,
            }
        )

    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for candidate in candidates:
        by_symbol.setdefault(candidate["symbol"], []).append(candidate)

    targets: List[Dict[str, Any]] = []
    for symbol, matches in sorted(by_symbol.items()):
        target = dict(matches[-1])
        source_ids = sorted(int(match["cmc_id"]) for match in matches)
        target["source_cmc_ids"] = source_ids
        target["identity_ambiguous"] = len(source_ids) > 1
        if target["identity_ambiguous"]:
            target["identity_error"] = (
                f"current symbol {symbol} is shared by CMC ids {source_ids}"
            )
            target["data_start_at"] = max(
                match["data_start_at"] for match in matches
            )
        else:
            target["identity_error"] = None
        targets.append(target)
    return targets


def load_canonical_symbol_targets(
    conn,
    *,
    run_table: str,
    snapshot_table: str,
    expected_row_count: int = 50,
    default_data_start_at: datetime = DEFAULT_DATA_START_AT,
    stablecoin_overrides: Optional[Mapping[Any, Any]] = None,
    wrapped_overrides: Optional[Mapping[Any, Any]] = None,
    data_start_overrides: Optional[Mapping[Any, Any]] = None,
) -> List[Dict[str, Any]]:
    """Read only complete raw snapshots; ``cmc_id`` never leaves this helper."""
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                snapshot.cmc_id,
                snapshot.symbol,
                snapshot.name,
                snapshot.tags,
                run.source_available_at,
                snapshot.snapshot_date
            FROM {snapshot_table} AS snapshot
            JOIN {run_table} AS run USING (snapshot_date)
            WHERE run.row_count = %s
              AND run.payload_sha256 IS NOT NULL
            ORDER BY snapshot.cmc_id, run.source_available_at, snapshot.snapshot_date
            """,
            (expected_row_count,),
        )
        rows = [
            {
                "cmc_id": row[0],
                "symbol": row[1],
                "name": row[2],
                "tags": row[3] or [],
                "source_available_at": row[4],
                "snapshot_date": row[5],
            }
            for row in cursor.fetchall()
        ]
    return build_canonical_symbol_targets(
        rows,
        default_data_start_at=default_data_start_at,
        stablecoin_overrides=stablecoin_overrides,
        wrapped_overrides=wrapped_overrides,
        data_start_overrides=data_start_overrides,
    )


def resolve_spot_market(
    markets: Mapping[str, Mapping[str, Any]],
    *,
    asset_symbol: str,
    quote_priority: Sequence[str] = ("USDT",),
    override: Optional[str] = None,
) -> Dict[str, Any]:
    """Resolve an exact Phase-1 spot market; no quote fallback is allowed."""
    quotes = [normalize_symbol(quote) for quote in quote_priority]
    if quotes != ["USDT"]:
        raise ValueError(
            "CMC Top 50 (legacy cmc_top30 contract) supports only the USDT quote"
        )
    symbol = normalize_symbol(asset_symbol)
    expected_pair = f"{symbol}/USDT"
    requested_pair = str(override).strip() if override else expected_pair
    if requested_pair != expected_pair:
        return {"market": None, "quote": None, "reason": "override_not_expected_pair"}

    market = markets.get(expected_pair)
    if market is None:
        reason = "market_not_found"
    elif market.get("spot") is not True:
        market = None
        reason = "market_not_spot"
    elif normalize_symbol(market.get("base")) != symbol:
        market = None
        reason = "market_not_expected_pair"
    elif normalize_symbol(market.get("quote")) != "USDT":
        market = None
        reason = "market_not_expected_pair"
    else:
        reason = None
    return {
        "market": market,
        "quote": "USDT" if market is not None else None,
        "reason": reason,
    }


def select_primary_usdt_venue(
    *,
    symbol: str,
    venue_priority: Sequence[str],
    markets_by_venue: Mapping[str, Mapping[str, Mapping[str, Any]]],
    venue_errors: Optional[Mapping[str, str]] = None,
    existing_exchange: Optional[str] = None,
    exchange_override: Optional[str] = None,
) -> Dict[str, Any]:
    """Select one venue while making sticky and override behavior testable."""
    normalized_symbol = normalize_symbol(symbol)
    priorities = [str(venue).strip().lower() for venue in venue_priority]
    existing = str(existing_exchange or "").strip().lower() or None
    override = str(exchange_override or "").strip().lower() or None
    errors = {
        str(venue).strip().lower(): str(error)
        for venue, error in (venue_errors or {}).items()
    }

    if override is None and existing is not None:
        return {
            "selected_exchange": existing,
            "mapping_status": "resolved",
            "selection_changed": False,
            "last_error": None,
        }

    if override is not None:
        if override not in priorities:
            return {
                "selected_exchange": existing,
                "mapping_status": "resolved" if existing else "pending",
                "selection_changed": False,
                "last_error": (
                    f"override venue {override!r} is not in venue_priority"
                ),
            }
        venues = [override]
    else:
        venues = priorities

    reasons: List[str] = []
    for venue in venues:
        if venue in errors:
            reasons.append(f"{venue}:markets_unavailable:{errors[venue]}")
            return {
                "selected_exchange": existing,
                "mapping_status": "resolved" if existing else "pending",
                "selection_changed": False,
                "last_error": "; ".join(reasons)[:4000],
            }
        if venue not in markets_by_venue:
            reasons.append(f"{venue}:markets_unavailable")
            return {
                "selected_exchange": existing,
                "mapping_status": "resolved" if existing else "pending",
                "selection_changed": False,
                "last_error": "; ".join(reasons)[:4000],
            }

        resolved = resolve_spot_market(
            markets_by_venue[venue],
            asset_symbol=normalized_symbol,
            quote_priority=("USDT",),
        )
        if resolved["market"] is not None:
            return {
                "selected_exchange": venue,
                "mapping_status": "resolved",
                "selection_changed": existing != venue,
                "last_error": None,
            }

        reason = str(resolved.get("reason") or "market_not_found")
        reasons.append(f"{venue}:{reason}")
        if override is not None:
            return {
                "selected_exchange": existing,
                "mapping_status": "resolved" if existing else "pending",
                "selection_changed": False,
                "last_error": (
                    f"override was not applied; {'; '.join(reasons)}"
                )[:4000],
            }
        if reason == "ambiguous_market":
            return {
                "selected_exchange": None,
                "mapping_status": "ambiguous",
                "selection_changed": False,
                "last_error": "; ".join(reasons)[:4000],
            }

    return {
        "selected_exchange": None,
        "mapping_status": "unavailable",
        "selection_changed": False,
        "last_error": "; ".join(reasons)[:4000],
    }


def load_existing_symbol_targets(conn, *, table: str) -> Dict[str, Dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT
                symbol, selected_exchange, mapping_status, backfill_status,
                actual_first_candle_at, actual_last_candle_at, last_error
            FROM {table}
            """
        )
        return {
            normalize_symbol(row[0]): {
                "selected_exchange": row[1],
                "mapping_status": row[2],
                "backfill_status": row[3],
                "actual_first_candle_at": row[4],
                "actual_last_candle_at": row[5],
                "last_error": row[6],
            }
            for row in cursor.fetchall()
        }


def replace_canonical_symbol_targets(
    conn,
    records: Sequence[Mapping[str, Any]],
    *,
    table: str,
) -> int:
    """Atomically retire former aliases and upsert the current symbol set."""
    if not records:
        raise ValueError("Refusing to retire canonical targets from an empty source")

    from psycopg2.extras import execute_values

    values = [
        (
            normalize_symbol(record["symbol"]),
            str(record["name"]),
            _as_utc(record["data_start_at"]),
            bool(record.get("is_stablecoin")),
            bool(record.get("is_wrapped")),
            record.get("selected_exchange"),
            record["mapping_status"],
            record.get("backfill_status", "pending"),
            record.get("last_error"),
        )
        for record in records
    ]
    try:
        with conn.cursor() as cursor:
            for record in records:
                if not record.get("force_selected_exchange"):
                    continue
                cursor.execute(
                    f"""
                    UPDATE {table}
                    SET selected_exchange = %s,
                        mapping_status = 'resolved',
                        backfill_status = 'pending',
                        actual_first_candle_at = NULL,
                        actual_last_candle_at = NULL,
                        last_error = NULL,
                        updated_at = now()
                    WHERE symbol = %s
                    """,
                    (
                        record.get("selected_exchange"),
                        normalize_symbol(record["symbol"]),
                    ),
                )
            cursor.execute(
                f"""
                UPDATE {table}
                SET is_canonical = FALSE,
                    updated_at = now()
                WHERE is_canonical IS TRUE
                """
            )
            execute_values(
                cursor,
                f"""
                INSERT INTO {table} AS target (
                    symbol, name, data_start_at, is_canonical,
                    is_stablecoin, is_wrapped, selected_exchange,
                    mapping_status, backfill_status, last_verified_at,
                    last_error
                ) VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                    name = EXCLUDED.name,
                    data_start_at = EXCLUDED.data_start_at,
                    is_canonical = TRUE,
                    is_stablecoin = EXCLUDED.is_stablecoin,
                    is_wrapped = EXCLUDED.is_wrapped,
                    selected_exchange = COALESCE(
                        target.selected_exchange,
                        EXCLUDED.selected_exchange
                    ),
                    mapping_status = EXCLUDED.mapping_status,
                    backfill_status = CASE
                        WHEN EXCLUDED.mapping_status = 'excluded_by_policy'
                        THEN 'excluded_by_policy'
                        WHEN EXCLUDED.mapping_status = 'ambiguous'
                        THEN 'pending'
                        WHEN target.data_start_at IS DISTINCT FROM EXCLUDED.data_start_at
                        THEN 'pending'
                        WHEN target.selected_exchange IS NOT NULL
                         AND target.backfill_status = 'excluded_by_policy'
                        THEN 'pending'
                        WHEN target.selected_exchange IS NOT NULL
                        THEN target.backfill_status
                        WHEN EXCLUDED.selected_exchange IS NOT NULL
                        THEN 'pending'
                        ELSE EXCLUDED.backfill_status
                    END,
                    actual_first_candle_at = CASE
                        WHEN target.data_start_at IS DISTINCT FROM EXCLUDED.data_start_at
                        THEN NULL
                        ELSE target.actual_first_candle_at
                    END,
                    actual_last_candle_at = CASE
                        WHEN target.data_start_at IS DISTINCT FROM EXCLUDED.data_start_at
                        THEN NULL
                        ELSE target.actual_last_candle_at
                    END,
                    last_verified_at = now(),
                    last_error = CASE
                        WHEN EXCLUDED.mapping_status IN (
                            'excluded_by_policy', 'ambiguous', 'pending',
                            'unavailable'
                        )
                        THEN EXCLUDED.last_error
                        WHEN EXCLUDED.last_error IS NOT NULL
                        THEN EXCLUDED.last_error
                        WHEN target.data_start_at IS DISTINCT FROM EXCLUDED.data_start_at
                        THEN NULL
                        WHEN target.selected_exchange IS NOT NULL
                        THEN target.last_error
                        ELSE EXCLUDED.last_error
                    END,
                    updated_at = now()
                """,
                values,
                template="(%s, %s, %s, TRUE, %s, %s, %s, %s, %s, now(), %s)",
                page_size=100,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(values)
