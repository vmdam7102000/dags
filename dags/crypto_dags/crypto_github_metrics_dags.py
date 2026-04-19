from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from plugins.utils.api_utils import request_json
from plugins.utils.config_loader import load_yaml_config

CONFIG = load_yaml_config("crypto_configs/crypto_github_metrics.yml")[
    "sync_crypto_github_metrics_dag"
]
API_CFG = CONFIG["api"]
DB_CFG = CONFIG["db"]
API_KEY_VAR = API_CFG.get("api_key_var")
API_KEY = Variable.get(API_KEY_VAR, default_var="") if API_KEY_VAR else ""
GITHUB_API_CFG = CONFIG.get("github_api") or {}
GITHUB_API_KEY_VAR = GITHUB_API_CFG.get("api_key_var")
GITHUB_API_KEY = (
    Variable.get(GITHUB_API_KEY_VAR, default_var="") if GITHUB_API_KEY_VAR else ""
)


def _emit_log(message: str, level: int = logging.INFO) -> None:
    logging.log(level, message)
    print(message, flush=True)


def _build_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    api_key_header = API_CFG.get("api_key_header")
    if API_KEY and api_key_header:
        headers[api_key_header] = API_KEY
    return headers


def _build_github_headers() -> Dict[str, str]:
    headers = {
        "Accept": GITHUB_API_CFG.get("accept", "application/vnd.github+json"),
    }
    if GITHUB_API_KEY:
        auth_prefix = GITHUB_API_CFG.get("api_key_prefix", "Bearer")
        headers["Authorization"] = f"{auth_prefix} {GITHUB_API_KEY}"

    api_version = GITHUB_API_CFG.get("api_version")
    if api_version:
        headers["X-GitHub-Api-Version"] = api_version
    return headers


def _normalize_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _load_target_rows(conn, table_name: str) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT id, symbol, name
        FROM {table_name}
        WHERE id IS NOT NULL
        ORDER BY id
    """
    _emit_log(f"Loading source rows from {table_name}")
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    _emit_log(f"Loaded {len(rows)} source rows from {table_name}")

    return [
        {
            "id": int(row[0]),
            "symbol": row[1],
            "name": row[2],
        }
        for row in rows
    ]


def _load_github_source_rows(
    conn,
    table_name: str,
    metric_date,
) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT coin_id, metric_date, github_repo_url
        FROM {table_name}
        WHERE metric_date = %s
          AND github_repo_url IS NOT NULL
          AND BTRIM(github_repo_url) <> ''
        ORDER BY coin_id
    """
    _emit_log(
        f"Loading GitHub source rows from {table_name} for metric_date={metric_date}"
    )
    with conn.cursor() as cursor:
        cursor.execute(sql, (metric_date,))
        rows = cursor.fetchall()
    _emit_log(
        f"Loaded {len(rows)} GitHub source rows from {table_name} for metric_date={metric_date}"
    )
    return [
        {
            "coin_id": int(row[0]),
            "metric_date": row[1],
            "github_repo_url": row[2],
        }
        for row in rows
    ]


def _fetch_coin_info(coin_id: int) -> Optional[Dict[str, Any]]:
    timeout = API_CFG.get("timeout", 60)
    _emit_log(
        f"Fetching CoinMarketCap info for coin_id={coin_id} url={API_CFG['url']} timeout={timeout}s"
    )
    request_started_at = time.monotonic()
    payload = request_json(
        API_CFG["url"],
        headers=_build_headers(),
        params={"id": coin_id},
        timeout=timeout,
        retries=API_CFG.get("retries", 3),
        backoff=API_CFG.get("backoff", 1.5),
        retry_statuses=API_CFG.get("retry_statuses"),
    )
    elapsed = time.monotonic() - request_started_at
    _emit_log(f"Finished CoinMarketCap request for coin_id={coin_id} in {elapsed:.2f}s")
    if not isinstance(payload, dict):
        _emit_log(
            f"CoinMarketCap payload is empty or not a dict for coin_id={coin_id}",
            logging.WARNING,
        )
        return None

    status = payload.get("status")
    if isinstance(status, dict) and status.get("error_code") not in (None, 0):
        _emit_log(
            f"CMC info API returned error for coin_id={coin_id}: "
            f"{status.get('error_message') or status.get('error_code')}",
            logging.WARNING,
        )
        return None

    _emit_log(
        f"CoinMarketCap payload OK for coin_id={coin_id} keys={sorted(payload.keys())}"
    )
    return payload


def _parse_github_repo_url(repo_url: str) -> Optional[Dict[str, str]]:
    normalized_url = _normalize_text(repo_url)
    if not normalized_url:
        return None

    parsed = urlparse(normalized_url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) < 2:
        return None

    owner = path_parts[0].strip()
    repo = path_parts[1].strip()
    if repo.endswith(".git"):
        repo = repo[:-4]

    if not owner or not repo:
        return None

    return {
        "owner": owner,
        "repo": repo,
    }


def _build_github_repo_api_url(owner: str, repo: str) -> str:
    url_template = GITHUB_API_CFG["url_template"]
    return url_template.format(
        owner=quote(owner, safe=""),
        repo=quote(repo, safe=""),
    )


def _fetch_github_repo_metrics(github_repo_url: str) -> Optional[Dict[str, Any]]:
    repo_ref = _parse_github_repo_url(github_repo_url)
    if not repo_ref:
        _emit_log(
            f"Cannot parse GitHub repo URL: {github_repo_url}",
            logging.WARNING,
        )
        return None

    api_url = _build_github_repo_api_url(repo_ref["owner"], repo_ref["repo"])
    timeout = GITHUB_API_CFG.get("timeout", 60)
    _emit_log(
        f"Fetching GitHub repo metrics for repo={repo_ref['owner']}/{repo_ref['repo']} "
        f"url={api_url} timeout={timeout}s"
    )
    request_started_at = time.monotonic()
    payload = request_json(
        api_url,
        headers=_build_github_headers(),
        timeout=timeout,
        retries=GITHUB_API_CFG.get("retries", 3),
        backoff=GITHUB_API_CFG.get("backoff", 1.5),
        retry_statuses=GITHUB_API_CFG.get("retry_statuses"),
        fatal_statuses=GITHUB_API_CFG.get("fatal_statuses"),
    )
    elapsed = time.monotonic() - request_started_at
    _emit_log(
        f"Finished GitHub repo request for repo={repo_ref['owner']}/{repo_ref['repo']} "
        f"in {elapsed:.2f}s"
    )
    if not isinstance(payload, dict):
        _emit_log(
            f"GitHub payload is empty or not a dict for repo={repo_ref['owner']}/{repo_ref['repo']}",
            logging.WARNING,
        )
        return None

    return payload


def _extract_github_repo_url(coin_payload: Dict[str, Any]) -> Optional[str]:
    urls = coin_payload.get("urls")
    if not isinstance(urls, dict):
        return None

    source_code_urls = urls.get("source_code")
    if not isinstance(source_code_urls, list):
        return None

    for value in source_code_urls:
        url = _normalize_text(value)
        if url and "github.com/" in url.lower():
            return url
    return None


def _transform_payload(coin_id: int, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, dict):
        _emit_log(f"Payload missing data object for coin_id={coin_id}", logging.WARNING)
        return None

    coin_payload = data.get(str(coin_id))
    if not isinstance(coin_payload, dict):
        _emit_log(
            f"Payload data missing coin node for coin_id={coin_id}",
            logging.WARNING,
        )
        return None

    github_repo_url = _extract_github_repo_url(coin_payload)
    if not github_repo_url:
        _emit_log(
            f"No GitHub repo found in urls.source_code for coin_id={coin_id}",
            logging.INFO,
        )
        return None

    status = payload.get("status")
    source_last_synced_at = None
    if isinstance(status, dict):
        source_last_synced_at = _parse_timestamp(status.get("timestamp"))

    metric_date = (
        source_last_synced_at.astimezone(timezone.utc).date()
        if source_last_synced_at
        else datetime.now(timezone.utc).date()
    )

    return {
        "coin_id": coin_id,
        "github_repo_url": github_repo_url,
        "metric_date": metric_date,
        "stars": None,
        "forks": None,
        "subscribers": None,
        "open_issues": None,
        "closed_issues": None,
        "total_issues": None,
        "pull_requests_merged": None,
        "pull_request_contributors": None,
        "commits_4_weeks": None,
        "code_additions_4_weeks": None,
        "code_deletions_4_weeks": None,
        "active_developers_4_weeks": None,
        "last_commit_at": None,
        "source_last_synced_at": source_last_synced_at,
    }


def _transform_github_metrics(
    coin_id: int,
    metric_date,
    github_repo_url: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "coin_id": coin_id,
        "metric_date": metric_date,
        "github_repo_url": github_repo_url,
        "stars": _safe_int(payload.get("stargazers_count")),
        "forks": _safe_int(payload.get("forks_count")),
        "open_issues": _safe_int(payload.get("open_issues_count")),
        "subscribers": _safe_int(payload.get("subscribers_count")),
    }


def _replace_daily_metric(conn, table_name: str, record: Dict[str, Any]) -> int:
    delete_sql = f"""
        DELETE FROM {table_name}
        WHERE coin_id = %s
          AND metric_date = %s
    """
    insert_sql = f"""
        INSERT INTO {table_name} (
            coin_id,
            github_repo_url,
            metric_date,
            stars,
            forks,
            subscribers,
            open_issues,
            closed_issues,
            total_issues,
            pull_requests_merged,
            pull_request_contributors,
            commits_4_weeks,
            code_additions_4_weeks,
            code_deletions_4_weeks,
            active_developers_4_weeks,
            last_commit_at,
            source_last_synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    _emit_log(
        f"Upserting github metric row into {table_name} for coin_id={record['coin_id']} "
        f"metric_date={record['metric_date']} github_repo_url={record['github_repo_url']}"
    )
    write_started_at = time.monotonic()
    with conn.cursor() as cursor:
        cursor.execute(delete_sql, (record["coin_id"], record["metric_date"]))
        cursor.execute(
            insert_sql,
            (
                record["coin_id"],
                record["github_repo_url"],
                record["metric_date"],
                record["stars"],
                record["forks"],
                record["subscribers"],
                record["open_issues"],
                record["closed_issues"],
                record["total_issues"],
                record["pull_requests_merged"],
                record["pull_request_contributors"],
                record["commits_4_weeks"],
                record["code_additions_4_weeks"],
                record["code_deletions_4_weeks"],
                record["active_developers_4_weeks"],
                record["last_commit_at"],
                record["source_last_synced_at"],
            ),
        )
        affected_rows = cursor.rowcount
    elapsed = time.monotonic() - write_started_at
    _emit_log(
        f"Finished DB upsert for coin_id={record['coin_id']} affected_rows={affected_rows} "
        f"in {elapsed:.2f}s"
    )
    return affected_rows


def _update_github_metrics(conn, table_name: str, record: Dict[str, Any]) -> int:
    sql = f"""
        UPDATE {table_name}
        SET stars = %s,
            forks = %s,
            open_issues = %s,
            subscribers = %s
        WHERE coin_id = %s
          AND metric_date = %s
    """
    _emit_log(
        f"Updating GitHub metrics in {table_name} for coin_id={record['coin_id']} "
        f"metric_date={record['metric_date']} stars={record['stars']} "
        f"forks={record['forks']} open_issues={record['open_issues']} "
        f"subscribers={record['subscribers']}"
    )
    write_started_at = time.monotonic()
    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                record["stars"],
                record["forks"],
                record["open_issues"],
                record["subscribers"],
                record["coin_id"],
                record["metric_date"],
            ),
        )
        affected_rows = cursor.rowcount
    elapsed = time.monotonic() - write_started_at
    _emit_log(
        f"Finished GitHub metrics update for coin_id={record['coin_id']} "
        f"affected_rows={affected_rows} in {elapsed:.2f}s"
    )
    return affected_rows


with DAG(
    dag_id="sync_crypto_github_metrics_dag",
    description="Sync GitHub repo URLs from CoinMarketCap and refresh GitHub repo metrics into cryptocurrency_github_metrics_daily",
    default_args={
        "owner": "crypto-data",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "cmc", "github", "metadata"],
) as dag:

    @task
    def sync_github_metrics() -> None:
        if not API_KEY:
            raise ValueError("Missing API key: configure Variable cmc_api_key")

        _emit_log(
            f"Starting DAG sync_crypto_github_metrics_dag source_table={DB_CFG['source_table']} "
            f"target_table={DB_CFG['target_table']}"
        )
        _emit_log(
            f"API config timeout={API_CFG.get('timeout', 60)}s retries={API_CFG.get('retries', 3)} "
            f"pause_seconds={API_CFG.get('request_pause_seconds', 0)} "
            f"failure_pause_seconds={API_CFG.get('failure_pause_seconds', 0)}"
        )
        if not GITHUB_API_KEY:
            _emit_log(
                "GitHub API token is empty. Requests may be rate-limited. "
                f"Configure Airflow Variable {GITHUB_API_KEY_VAR or 'github_api_token'}.",
                logging.WARNING,
            )
        _emit_log(
            f"GitHub API timeout={GITHUB_API_CFG.get('timeout', 60)}s "
            f"retries={GITHUB_API_CFG.get('retries', 3)}"
        )

        hook = PostgresHook(postgres_conn_id=DB_CFG["postgres_conn_id"])
        _emit_log(f"Opening Postgres connection {DB_CFG['postgres_conn_id']}")
        conn = hook.get_conn()
        _emit_log("Postgres connection opened")

        inserted_rows = 0
        success_count = 0
        failed_ids: List[int] = []
        skipped_without_repo: List[Dict[str, Any]] = []
        sample_updates: List[Dict[str, Any]] = []
        metrics_success_count = 0
        metrics_failed_ids: List[int] = []
        metrics_sample_updates: List[Dict[str, Any]] = []
        run_metric_date = datetime.now(timezone.utc).date()

        try:
            target_rows = _load_target_rows(conn, DB_CFG["source_table"])
            if not target_rows:
                _emit_log(f"No rows found in {DB_CFG['source_table']}", logging.WARNING)
                return

            pause_seconds = float(API_CFG.get("request_pause_seconds", 0) or 0)
            failure_pause_seconds = float(API_CFG.get("failure_pause_seconds", pause_seconds) or 0)
            _emit_log(
                f"Begin sequential scan for {len(target_rows)} rows from {DB_CFG['source_table']}"
            )

            for index, row in enumerate(target_rows, start=1):
                try:
                    coin_id = int(row["id"])
                    _emit_log(
                        f"[{index}/{len(target_rows)}] Processing coin_id={coin_id} "
                        f"symbol={row['symbol']} name={row['name']}"
                    )
                    payload = _fetch_coin_info(coin_id)
                    if not payload:
                        failed_ids.append(coin_id)
                        _emit_log(
                            f"[{index}/{len(target_rows)}] No CoinMarketCap payload for coin_id={coin_id}",
                            logging.WARNING,
                        )
                        if failure_pause_seconds > 0:
                            _emit_log(
                                f"[{index}/{len(target_rows)}] Sleeping {failure_pause_seconds}s after failure"
                            )
                            time.sleep(failure_pause_seconds)
                        continue

                    record = _transform_payload(coin_id, payload)
                    if not record:
                        if len(skipped_without_repo) < 20:
                            skipped_without_repo.append(
                                {
                                    "coin_id": coin_id,
                                    "symbol": row["symbol"],
                                }
                            )
                        _emit_log(
                            f"[{index}/{len(target_rows)}] Skip coin_id={coin_id} because no GitHub repo URL"
                        )
                        continue

                    run_metric_date = record["metric_date"]
                    inserted_rows += _replace_daily_metric(
                        conn,
                        DB_CFG["target_table"],
                        record,
                    )
                    success_count += 1
                    _emit_log(
                        f"[{index}/{len(target_rows)}] Sync success for coin_id={coin_id} "
                        f"github_repo_url={record['github_repo_url']}"
                    )

                    if len(sample_updates) < 10:
                        sample_updates.append(
                            {
                                "coin_id": record["coin_id"],
                                "metric_date": str(record["metric_date"]),
                                "github_repo_url": record["github_repo_url"],
                            }
                        )
                except Exception as exc:
                    failed_ids.append(int(row["id"]))
                    _emit_log(
                        f"[{index}/{len(target_rows)}] Exception for coin_id={row['id']} "
                        f"symbol={row['symbol']} error={exc}",
                        logging.ERROR,
                    )
                    logging.exception(
                        "Failed to sync GitHub repo for row_id=%s symbol=%s error=%s",
                        row["id"],
                        row["symbol"],
                        exc,
                    )
                    if failure_pause_seconds > 0:
                        _emit_log(
                            f"[{index}/{len(target_rows)}] Sleeping {failure_pause_seconds}s after exception"
                        )
                        time.sleep(failure_pause_seconds)
                finally:
                    if pause_seconds > 0:
                        _emit_log(
                            f"[{index}/{len(target_rows)}] Sleeping {pause_seconds}s before next coin"
                        )
                        time.sleep(pause_seconds)

            _emit_log("Committing CoinMarketCap repo URL phase to Postgres")
            conn.commit()
            _emit_log("CoinMarketCap repo URL phase committed")

            github_rows = _load_github_source_rows(
                conn,
                DB_CFG["target_table"],
                run_metric_date,
            )
            if not github_rows:
                _emit_log(
                    f"No GitHub source rows found in {DB_CFG['target_table']} for metric_date={run_metric_date}",
                    logging.WARNING,
                )
            else:
                _emit_log(
                    f"Begin GitHub metrics refresh for {len(github_rows)} rows "
                    f"from {DB_CFG['target_table']} metric_date={run_metric_date}"
                )

            github_pause_seconds = float(
                GITHUB_API_CFG.get("request_pause_seconds", pause_seconds) or 0
            )
            github_failure_pause_seconds = float(
                GITHUB_API_CFG.get("failure_pause_seconds", failure_pause_seconds) or 0
            )

            for index, row in enumerate(github_rows, start=1):
                try:
                    coin_id = int(row["coin_id"])
                    github_repo_url = str(row["github_repo_url"])
                    _emit_log(
                        f"[GitHub {index}/{len(github_rows)}] Processing coin_id={coin_id} "
                        f"github_repo_url={github_repo_url}"
                    )
                    payload = _fetch_github_repo_metrics(github_repo_url)
                    if not payload:
                        metrics_failed_ids.append(coin_id)
                        _emit_log(
                            f"[GitHub {index}/{len(github_rows)}] No GitHub payload for coin_id={coin_id}",
                            logging.WARNING,
                        )
                        if github_failure_pause_seconds > 0:
                            _emit_log(
                                f"[GitHub {index}/{len(github_rows)}] Sleeping {github_failure_pause_seconds}s after failure"
                            )
                            time.sleep(github_failure_pause_seconds)
                        continue

                    metric_record = _transform_github_metrics(
                        coin_id,
                        row["metric_date"],
                        github_repo_url,
                        payload,
                    )
                    _update_github_metrics(
                        conn,
                        DB_CFG["target_table"],
                        metric_record,
                    )
                    metrics_success_count += 1
                    _emit_log(
                        f"[GitHub {index}/{len(github_rows)}] Sync success for coin_id={coin_id} "
                        f"stars={metric_record['stars']} forks={metric_record['forks']} "
                        f"open_issues={metric_record['open_issues']} subscribers={metric_record['subscribers']}"
                    )

                    if len(metrics_sample_updates) < 10:
                        metrics_sample_updates.append(
                            {
                                "coin_id": metric_record["coin_id"],
                                "stars": metric_record["stars"],
                                "forks": metric_record["forks"],
                                "open_issues": metric_record["open_issues"],
                                "subscribers": metric_record["subscribers"],
                            }
                        )
                except Exception as exc:
                    metrics_failed_ids.append(int(row["coin_id"]))
                    _emit_log(
                        f"[GitHub {index}/{len(github_rows)}] Exception for coin_id={row['coin_id']} "
                        f"github_repo_url={row['github_repo_url']} error={exc}",
                        logging.ERROR,
                    )
                    logging.exception(
                        "Failed to sync GitHub metrics for coin_id=%s github_repo_url=%s error=%s",
                        row["coin_id"],
                        row["github_repo_url"],
                        exc,
                    )
                    if github_failure_pause_seconds > 0:
                        _emit_log(
                            f"[GitHub {index}/{len(github_rows)}] Sleeping {github_failure_pause_seconds}s after exception"
                        )
                        time.sleep(github_failure_pause_seconds)
                finally:
                    if github_pause_seconds > 0:
                        _emit_log(
                            f"[GitHub {index}/{len(github_rows)}] Sleeping {github_pause_seconds}s before next repo"
                        )
                        time.sleep(github_pause_seconds)

            _emit_log("Committing GitHub metrics phase to Postgres")
            conn.commit()
            _emit_log("GitHub metrics phase committed")

            _emit_log(
                "Crypto GitHub repo sync completed for "
                f"{DB_CFG['target_table']}: full_scan_rows={len(target_rows)} "
                f"successful_payloads={success_count} failed_payloads={len(failed_ids)} "
                f"skipped_without_repo={len(skipped_without_repo)} inserted_rows={inserted_rows} "
                f"metrics_success={metrics_success_count} metrics_failed={len(metrics_failed_ids)} "
                f"sample_repo_updates={sample_updates} metrics_sample_updates={metrics_sample_updates}"
            )

            if failed_ids:
                _emit_log(
                    f"Crypto GitHub repo sync failed for {len(failed_ids)} ids: {failed_ids[:20]}",
                    logging.WARNING,
                )
            if metrics_failed_ids:
                _emit_log(
                    f"GitHub metrics sync failed for {len(metrics_failed_ids)} ids: {metrics_failed_ids[:20]}",
                    logging.WARNING,
                )
        finally:
            _emit_log("Closing Postgres connection")
            conn.close()
            _emit_log("Postgres connection closed")

    sync_github_metrics()
