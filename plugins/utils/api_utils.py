# plugins/utils/api_utils.py
"""
Utility helpers for making HTTP requests to external APIs.
Centralizes retry/backoff logic so DAGs & operators reuse a single entry point.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Sequence, Union

import requests

JsonType = Union[Dict[str, Any], List[Any]]


def request_json(
    url: str,
    *,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    json: Any = None,
    timeout: int | float = 30,
    retries: int = 3,
    backoff: float = 1.5,
    retry_statuses: Optional[Sequence[int]] = None,
    fatal_statuses: Optional[Sequence[int]] = None,
    session: Optional[requests.Session] = None,
) -> Optional[JsonType]:
    """
    Make an HTTP request and parse JSON with simple retries/backoff.

    Returns the decoded JSON on success, otherwise None after exhausting retries.
    """
    http = session or requests.Session()
    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            resp = http.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                json=json,
                timeout=timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            last_exc = exc
            resp = exc.response
            status_code = resp.status_code if resp else None

            if fatal_statuses and status_code in fatal_statuses:
                logging.error(
                    "Fatal API response (%s): %s %s params=%s",
                    status_code,
                    method,
                    url,
                    params,
                )
                raise

            retry_after = None
            retry_on = set(retry_statuses or ())
            if status_code == 429 or status_code in retry_on:
                retry_after = _parse_retry_after(
                    resp.headers.get("Retry-After") if resp else None
                )

            logging.warning(
                "API request failed (%s/%s): %s %s params=%s error=%s",
                attempt,
                retries,
                method,
                url,
                params,
                exc,
            )
            if attempt < retries:
                sleep_time = backoff ** (attempt - 1)
                if retry_after:
                    sleep_time = max(sleep_time, retry_after)
                time.sleep(sleep_time)
        except Exception as exc:
            last_exc = exc
            logging.warning(
                "API request failed (%s/%s): %s %s params=%s error=%s",
                attempt,
                retries,
                method,
                url,
                params,
                exc,
            )
            if attempt < retries:
                sleep_time = backoff ** (attempt - 1)
                time.sleep(sleep_time)

    if last_exc:
        logging.error("All retries failed for %s %s: %s", method, url, last_exc)

    return None
