"""Four-table news pipeline. Airflow owns run status and scheduling.

Every public function accepts a psycopg2 connection so the DAGs use the same
PostgresHook connection contract as other crypto jobs. No network call is made
inside a transaction.
"""
from __future__ import annotations

import hashlib
import json
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from psycopg2.extras import Json

LOG = logging.getLogger(__name__)
SCHEMA = "raw_crypto_data"
SYSTEM_PROMPT = (
    "Classify cryptocurrency news for the named coin only. Respond with JSON: "
    "relevant (boolean), sentiment (positive|negative|neutral), "
    "impact (high|medium|low), confidence (0..1), reason (short text). "
    "Neutral means genuinely neutral or unclear, not an API or parsing failure."
)
REPORT_PROMPT = (
    "Write a concise Markdown market analysis for the specified cryptocurrency. "
    "Use only the supplied news, cite supplied URLs, and mention uncertainty."
)
_UTM = re.compile(r"^utm_", re.I)


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_url(raw: str) -> str:
    """Preserve Coinfox's URL identity rule during initial migration."""
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "http").lower()
    host = parsed.netloc.lower()
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    query = urlencode(sorted((k, v) for k, v in parse_qsl(parsed.query)
                             if v and not _UTM.match(k)), doseq=True)
    return urlunparse((scheme, host, path, "", query, ""))


def score_counts(pos: int, neg: int, neutral: int) -> float | None:
    if pos + neg + neutral == 0:
        return None
    return (max(-100, min(100, pos - neg)) + 100) / 2


def _request_json(method: str, url: str, *, timeout: int, **kwargs: Any) -> dict:
    """Retry transient provider errors without including a URL/key in exceptions."""
    for attempt in range(3):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)
            if response.status_code in (408, 429, 500, 502, 503, 504):
                if attempt == 2:
                    raise RuntimeError(f"Provider returned HTTP {response.status_code}")
                retry_after = response.headers.get("Retry-After", "")
                delay = float(retry_after) if retry_after.isdigit() else 2 ** attempt + random.random()
                time.sleep(min(delay, 30))
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"Provider returned HTTP {response.status_code}")
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError("Provider returned a non-object JSON response")
            return value
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 2:
                raise RuntimeError("Provider transport failed after retries") from None
            time.sleep(2 ** attempt + random.random())
        except requests.exceptions.JSONDecodeError:
            raise ValueError("Provider returned invalid JSON") from None
    raise RuntimeError("Provider request failed")


def _published(item: dict, seen_at: datetime) -> tuple[datetime, bool]:
    tags = ((item.get("pagemap") or {}).get("metatags") or [{}])[0]
    if not isinstance(tags, dict):
        tags = {}
    for key in ("article:published_time", "og:updated_time", "datePublished", "date"):
        value = tags.get(key)
        if not value:
            continue
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc), key not in (
                "article:published_time", "datePublished")
        except ValueError:
            pass
    return seen_at, True


def fetch_cse(coin: str, name: str, cfg: dict, api_key: str, cx: str) -> list[dict]:
    if not api_key or not cx:
        raise ValueError("Missing Google CSE Airflow Variables")
    sites = ("news.google.com", "reuters.com", "coindesk.com", "newsbtc.com",
             "ambcrypto.com", "cointelegraph.com")
    query = f'"{name}" (' + " OR ".join(f"site:{s}" for s in sites) + ")"
    limit = min(int(cfg["max_results_per_coin"]), 100)
    seen_at = datetime.now(timezone.utc)
    results = []
    for start in range(1, limit + 1, 10):
        payload = _request_json(
            "GET", "https://www.googleapis.com/customsearch/v1",
            timeout=int(cfg["request_timeout_seconds"]),
            params={"key": api_key, "cx": cx, "q": query, "num": min(10, limit - len(results)),
                    "start": start, "sort": "date", "lr": "lang_en", "dateRestrict": "d1"},
        )
        page = payload.get("items") or []
        if not page:
            break
        for item in page:
            if item.get("link"):
                pub, inferred = _published(item, seen_at)
                results.append({"url": item["link"], "title": item.get("title"),
                                "snippet": item.get("snippet"), "source": item.get("displayLink"),
                                "language": "en", "published_at": pub,
                                "published_at_inferred": inferred, "meta": item})
    return results


def _snapshot(item: dict, symbol: str, name: str, version: str) -> dict:
    return {"coin_symbol": symbol, "coin_name": name, "title": item.get("title") or "",
            "snippet": item.get("snippet") or "", "source": item.get("source") or "",
            "classifier_version": version, "prompt_hash": sha256(SYSTEM_PROMPT)}


def register_candidates(conn, symbol: str, name: str, items: list[dict], cfg: dict) -> int:
    """Commit each article and its coin-specific pending row atomically."""
    inserted = 0
    for item in items:
        url = normalize_url(item["url"])
        snapshot = _snapshot(item, symbol, name, cfg["classifier_version"])
        input_hash = sha256(json.dumps(snapshot, sort_keys=True, ensure_ascii=False))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.news_articles
                        (url,url_hash,title,snippet,source,language,published_at,
                         published_at_inferred,meta)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (url_hash) DO UPDATE SET
                          title=COALESCE(EXCLUDED.title,news_articles.title),
                          snippet=COALESCE(EXCLUDED.snippet,news_articles.snippet),
                          source=COALESCE(EXCLUDED.source,news_articles.source),
                          last_seen_at=now(),
                          published_at=CASE WHEN news_articles.published_at_inferred
                              AND NOT EXCLUDED.published_at_inferred THEN EXCLUDED.published_at
                              ELSE news_articles.published_at END,
                          published_at_inferred=news_articles.published_at_inferred
                              AND EXCLUDED.published_at_inferred
                        RETURNING id""",
                    (url, sha256(url), item.get("title"), item.get("snippet"),
                     item.get("source"), item.get("language"), item["published_at"],
                     item["published_at_inferred"], Json(item.get("meta"))),
                )
                news_id = cur.fetchone()[0]
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.news_sentiments
                        (news_id,coin_symbol,coin_name,classifier_version,model,prompt_hash,
                         input_hash,input_snapshot)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (news_id,coin_symbol,classifier_version,input_hash) DO NOTHING""",
                    (news_id, symbol, name, cfg["classifier_version"], cfg["model"],
                     sha256(SYSTEM_PROMPT),
                     input_hash, Json(snapshot)),
                )
            conn.commit()
            inserted += 1
        except Exception:
            conn.rollback()
            raise
    return inserted


def queue_reclassification(conn, symbol: str, name: str, cfg: dict) -> int:
    """Queue retained article/coin pairs without querying Google for old news."""
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT DISTINCT a.id,a.title,a.snippet,a.source
                FROM {SCHEMA}.news_articles a
                JOIN {SCHEMA}.news_sentiments s ON s.news_id=a.id
                WHERE s.coin_symbol=%s AND a.last_seen_at >= now() - interval '90 days'""",
            (symbol,),
        )
        articles = cur.fetchall()
    conn.commit()
    count = 0
    for news_id, title, snippet, source in articles:
        snapshot = _snapshot({"title": title, "snippet": snippet, "source": source},
                             symbol, name, cfg["classifier_version"])
        input_hash = sha256(json.dumps(snapshot, sort_keys=True, ensure_ascii=False))
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.news_sentiments
                        (news_id,coin_symbol,coin_name,classifier_version,model,prompt_hash,
                         input_hash,input_snapshot)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (news_id,coin_symbol,classifier_version,input_hash) DO NOTHING""",
                    (news_id, symbol, name, cfg["classifier_version"], cfg["model"],
                     sha256(SYSTEM_PROMPT),
                     input_hash, Json(snapshot)),
                )
                count += cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return count


def _classify(snapshot: dict, cfg: dict, api_key: str) -> dict:
    if not api_key:
        raise ValueError("Missing OpenRouter Airflow Variable")
    payload = {"model": cfg["model"], "temperature": 0.2,
               "response_format": {"type": "json_object"},
               "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                            {"role": "user", "content": json.dumps(snapshot, ensure_ascii=False)}]}
    data = _request_json(
        "POST", "https://openrouter.ai/api/v1/chat/completions",
        timeout=int(cfg["request_timeout_seconds"]),
        headers={"Authorization": f"Bearer {api_key}"}, json=payload,
    )
    try:
        result = json.loads(data["choices"][0]["message"]["content"])
        relevant = result["relevant"]
        sentiment = result["sentiment"]
        impact = result["impact"]
        confidence = result.get("confidence")
        if type(relevant) is not bool or sentiment not in ("positive", "negative", "neutral"):
            raise ValueError("Invalid relevance or sentiment")
        if impact not in ("high", "medium", "low"):
            raise ValueError("Invalid impact")
        if confidence is not None and not 0 <= float(confidence) <= 1:
            raise ValueError("Invalid confidence")
        return {"relevant": relevant, "sentiment": sentiment, "impact": impact,
                "confidence": confidence, "reason": str(result.get("reason") or "")}
    except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Invalid classification response") from exc


def classify_pending(conn, symbol: str, cfg: dict, api_key: str, cutoff: datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT id,input_snapshot FROM {SCHEMA}.news_sentiments
                WHERE coin_symbol=%s AND classifier_version=%s
                  AND status IN ('pending','failed') AND created_at <= %s
                  AND created_at >= now() - interval '90 days' AND attempts < 5
                ORDER BY created_at,id""",
            (symbol, cfg["classifier_version"], cutoff),
        )
        work = cur.fetchall()
    conn.commit()
    failures = 0
    for sentiment_id, snapshot in work:
        try:
            result = _classify(snapshot, cfg, api_key)
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {SCHEMA}.news_sentiments SET status='succeeded',
                        is_relevant=%s,sentiment=%s,impact=%s,confidence=%s,reason=%s,
                        classified_at=now(),attempts=attempts+1,last_error=NULL,updated_at=now()
                        WHERE id=%s AND status IN ('pending','failed')""",
                    (result["relevant"], result["sentiment"], result["impact"],
                     result["confidence"], result["reason"], sentiment_id),
                )
            conn.commit()
        except Exception as exc:
            conn.rollback()
            failures += 1
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {SCHEMA}.news_sentiments SET status='failed',
                        attempts=attempts+1,last_error=%s,updated_at=now()
                        WHERE id=%s AND status IN ('pending','failed')""",
                    (str(exc)[:500], sentiment_id),
                )
            conn.commit()
    if failures:
        raise RuntimeError(f"{failures} classification(s) failed for {symbol}")
    return len(work)


def _latest_success_rows(conn, symbol: str, version: str, start: datetime,
                         end: datetime, cutoff: datetime) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT id,news_id,url,title,source,published_at,
                       published_at_inferred,sentiment,impact
                FROM (
                    SELECT DISTINCT ON (s.news_id) s.id,a.id AS news_id,a.url,
                        a.title,a.source,a.published_at,a.published_at_inferred,
                        s.sentiment,s.impact,s.is_relevant,s.relevance_verified
                    FROM {SCHEMA}.news_sentiments s
                    JOIN {SCHEMA}.news_articles a ON a.id=s.news_id
                    WHERE s.coin_symbol=%s AND s.classifier_version=%s
                      AND s.status='succeeded' AND s.classified_at <= %s
                      AND a.published_at >= %s AND a.published_at < %s
                    ORDER BY s.news_id,s.created_at DESC,s.id DESC
                ) latest WHERE is_relevant AND relevance_verified""",
            (symbol, version, cutoff, start, end),
        )
        rows = cur.fetchall()
    conn.commit()
    names = ("sentiment_id", "id", "url", "title", "source", "published_at",
             "published_at_inferred", "sentiment", "impact")
    return [dict(zip(names, row)) for row in rows]


def aggregate_coin(conn, symbol: str, cfg: dict, end: datetime) -> None:
    """Publish each fixed window only after all candidate work is resolved."""
    cutoff = datetime.now(timezone.utc)
    for hours in (24, 168):
        start = end - timedelta(hours=hours)
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT COUNT(*) FROM {SCHEMA}.news_sentiments s
                    JOIN {SCHEMA}.news_articles a ON a.id=s.news_id
                    WHERE s.coin_symbol=%s AND s.classifier_version=%s
                      AND s.status <> 'succeeded' AND a.published_at >= %s
                      AND a.published_at < %s AND s.created_at <= %s""",
                (symbol, cfg["classifier_version"], start, end, cutoff),
            )
            unresolved = cur.fetchone()[0]
        conn.commit()
        if unresolved:
            raise RuntimeError(f"{symbol}: {unresolved} unresolved classification(s)")
        rows = _latest_success_rows(conn, symbol, cfg["classifier_version"], start, end, cutoff)
        pos = sum(r["sentiment"] == "positive" for r in rows)
        neg = sum(r["sentiment"] == "negative" for r in rows)
        neu = sum(r["sentiment"] == "neutral" for r in rows)
        inferred = sum(r["published_at_inferred"] for r in rows)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.news_sentiment_aggregates
                        (coin_symbol,window_start,window_end,classifier_version,score_version,
                         model,pos,neg,neutral,n_total,inferred_count,score,data_cutoff_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (coin_symbol,window_start,window_end,classifier_version,score_version)
                        DO UPDATE SET pos=EXCLUDED.pos,neg=EXCLUDED.neg,
                            neutral=EXCLUDED.neutral,n_total=EXCLUDED.n_total,
                            inferred_count=EXCLUDED.inferred_count,score=EXCLUDED.score,
                            data_cutoff_at=EXCLUDED.data_cutoff_at,computed_at=now()""",
                    (symbol, start, end, cfg["classifier_version"], cfg["score_version"],
                     cfg["model"], pos, neg, neu, len(rows), inferred,
                     score_counts(pos, neg, neu), cutoff),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise



def report_coin(conn, symbol: str, cfg: dict, end: datetime, api_key: str) -> None:
    if not api_key:
        raise ValueError("Missing OpenRouter Airflow Variable")
    for hours in (24, 168):
        start = end - timedelta(hours=hours)
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT data_cutoff_at FROM {SCHEMA}.news_sentiment_aggregates
                    WHERE coin_symbol=%s AND window_start=%s AND window_end=%s
                      AND classifier_version=%s AND score_version=%s""",
                (symbol, start, end, cfg["classifier_version"], cfg["score_version"]),
            )
            aggregate_row = cur.fetchone()
            if not aggregate_row:
                raise RuntimeError("Aggregate is not ready for report")
        conn.commit()
        rows = _latest_success_rows(conn, symbol, cfg["classifier_version"],
                                    start, end, aggregate_row[0])
        rows.sort(key=lambda r: ({"high": 3, "medium": 2, "low": 1}[r["impact"]],
                                 r["published_at"]), reverse=True)
        snapshot = [{**r, "published_at": r["published_at"].isoformat()} for r in rows[:40]]
        if not snapshot:
            continue
        input_hash = sha256(json.dumps({"symbol": symbol, "start": start.isoformat(),
                                       "end": end.isoformat(), "version": cfg["report_version"],
                                       "model": cfg["model"], "prompt_hash": sha256(REPORT_PROMPT),
                                       "sources": snapshot}, sort_keys=True))
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT 1 FROM {SCHEMA}.ai_longform_reports
                    WHERE coin_symbol=%s AND window_start=%s AND window_end=%s
                      AND report_version=%s AND input_hash=%s""",
                (symbol, start, end, cfg["report_version"], input_hash),
            )
            exists = cur.fetchone()
        conn.commit()
        if exists:
            continue
        data = _request_json(
            "POST", "https://openrouter.ai/api/v1/chat/completions",
            timeout=int(cfg["request_timeout_seconds"]),
            headers={"Authorization": f"Bearer {api_key}"},
            json={"model": cfg["model"], "temperature": 0.3,
                  "messages": [{"role": "system", "content": REPORT_PROMPT},
                               {"role": "user", "content": json.dumps(
                                   {"coin": symbol, "window_start": start.isoformat(),
                                    "window_end": end.isoformat(), "news": snapshot},
                                   ensure_ascii=False)}]},
        )
        try:
            markdown = data["choices"][0]["message"]["content"].strip()
            tokens = (data.get("usage") or {}).get("total_tokens")
            if not markdown:
                raise ValueError("Empty report")
        except (KeyError, IndexError, TypeError, AttributeError) as exc:
            raise ValueError("Invalid report response") from exc
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.ai_longform_reports
                        (coin_symbol,window_start,window_end,classifier_version,
                         report_version,input_hash,model,prompt_hash,summary_md,source_snapshot,
                         source_count,tokens_used)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (coin_symbol,window_start,window_end,report_version,input_hash)
                        DO NOTHING""",
                    (symbol, start, end, cfg["classifier_version"], cfg["report_version"],
                     input_hash, cfg["model"], sha256(REPORT_PROMPT),
                     markdown.replace("```", "``"),
                     Json(snapshot), len(snapshot), tokens if isinstance(tokens, int) else None),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def load_universe(conn, at: datetime) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT symbol,name,cmc_id,snapshot_date FROM "
            "raw_crypto_data.cmc_top30_universe_as_of(%s::timestamptz)", (at,),
        )
        rows = cur.fetchall()
    conn.commit()
    symbols = [str(row[0]).upper() for row in rows]
    if len(rows) != 50 or len(set(symbols)) != 50:
        raise RuntimeError("CMC Top 50 universe is missing or has ambiguous symbols")
    return [{"symbol": symbol, "name": str(row[1]), "cmc_id": row[2],
             "snapshot_date": row[3].isoformat()} for symbol, row in zip(symbols, rows)]


def validate_versions(conn, cfg: dict) -> None:
    """Require a new version when model or prompt changes."""
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT EXISTS (SELECT 1 FROM {SCHEMA}.news_sentiments
                WHERE classifier_version=%s AND
                      (model<>%s OR prompt_hash<>%s))""",
            (cfg["classifier_version"], cfg["model"], sha256(SYSTEM_PROMPT)),
        )
        classifier_mismatch = cur.fetchone()[0]
        cur.execute(
            f"""SELECT EXISTS (SELECT 1 FROM {SCHEMA}.ai_longform_reports
                WHERE report_version=%s AND
                      (model<>%s OR prompt_hash<>%s))""",
            (cfg["report_version"], cfg["model"], sha256(REPORT_PROMPT)),
        )
        report_mismatch = cur.fetchone()[0]
    conn.commit()
    if classifier_mismatch or report_mismatch:
        raise RuntimeError("Model or prompt changed without a news version bump")


def restrict_universe(roster: list[dict], symbols: object) -> list[dict]:
    """Manual shadow runs may select a verified subset of the CMC Top 50."""
    if symbols is None:
        return roster
    if not isinstance(symbols, list) or not symbols or any(
            not isinstance(value, str) or not value.strip() for value in symbols):
        raise ValueError("symbols must be a nonempty list of Top 50 symbols")
    requested = {value.strip().upper() for value in symbols}
    selected = [coin for coin in roster if coin["symbol"] in requested]
    if len(requested) != len(selected):
        raise ValueError("One or more symbols are absent from the CMC Top 50 universe")
    return selected


def advisory_key(symbol: str, version: str) -> int:
    number = int.from_bytes(hashlib.sha256(f"crypto_news:{symbol}:{version}".encode()).digest()[:8], "big")
    return number - (1 << 64) if number >= (1 << 63) else number


def lock_coin(conn, symbol: str, version: str) -> int:
    key = advisory_key(symbol, version)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
        acquired = cur.fetchone()[0]
    conn.commit()
    if not acquired:
        raise RuntimeError(f"News job already running for {symbol}/{version}")
    return key


def unlock_coin(conn, key: int) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
    conn.commit()


def apply_retention(conn, days: int = 90) -> dict:
    if days < 90:
        raise ValueError("News retention cannot be shorter than 90 days")
    with conn.cursor() as cur:
        cur.execute(f"""SELECT COUNT(*) FROM {SCHEMA}.news_sentiments
            WHERE status IN ('pending','failed')
              AND created_at < now() - (%s * interval '1 day')""", (days - 7,))
        backlog = cur.fetchone()[0]
        if backlog:
            raise RuntimeError(f"{backlog} unresolved news items are near retention")
        cur.execute(f"""DELETE FROM {SCHEMA}.ai_longform_reports
            WHERE window_end < now() - (%s * interval '1 day')""", (days,))
        reports = cur.rowcount
        cur.execute(f"""DELETE FROM {SCHEMA}.news_sentiment_aggregates
            WHERE window_end < now() - (%s * interval '1 day')""", (days,))
        aggregates = cur.rowcount
        cur.execute(f"""DELETE FROM {SCHEMA}.news_sentiments
            WHERE (status='succeeded' AND classified_at < now() - (%s * interval '1 day'))
               OR (status IN ('pending','failed') AND created_at < now() - (%s * interval '1 day'))""",
            (days, days))
        sentiments = cur.rowcount
        cur.execute(f"""DELETE FROM {SCHEMA}.news_articles a
            WHERE last_seen_at < now() - (%s * interval '1 day')
              AND NOT EXISTS (SELECT 1 FROM {SCHEMA}.news_sentiments s WHERE s.news_id=a.id)""",
            (days,))
        articles = cur.rowcount
    conn.commit()
    return {"reports": reports, "aggregates": aggregates,
            "sentiments": sentiments, "articles": articles}
