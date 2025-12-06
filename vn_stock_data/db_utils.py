# vn_stock_data/db_utils.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from psycopg2.extensions import connection as PGConnection

from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_all_stock_codes(
    postgres_conn_id: str,
    stock_list_table: str,
    code_column: str = "code",
) -> List[str]:
    """
    Lấy danh sách mã cổ phiếu từ bảng stock_list.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT {code_column} FROM {stock_list_table}")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()


def insert_dynamic_records(
    postgres_conn_id: str,
    table: str,
    records: List[Dict[str, Any]],
    columns_map: List[Dict[str, str]],
    conflict_keys: List[str],
    on_conflict_do_update: bool = False,
    conn: Optional[PGConnection] = None,
) -> None:
    """
    Insert records vào Postgres theo mapping JSON->column trong YAML.

    columns_map: list dict {"json_key": ..., "column": ...}
    conflict_keys: danh sách cột dùng cho ON CONFLICT
    on_conflict_do_update:
        - False: ON CONFLICT DO NOTHING
        - True:  ON CONFLICT DO UPDATE SET col = EXCLUDED.col
    """
    if not records:
        return

    # Build danh sách cột & json key tương ứng
    db_columns = [c["column"] for c in columns_map]
    json_keys = [c["json_key"] for c in columns_map]

    columns_sql = ", ".join(db_columns)
    placeholders_sql = ", ".join(["%s"] * len(db_columns))
    conflict_sql = ", ".join(conflict_keys)

    if on_conflict_do_update:
        set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in db_columns])
        conflict_part = f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {set_clause}"
    else:
        conflict_part = f"ON CONFLICT ({conflict_sql}) DO NOTHING"

    insert_sql = f"""
        INSERT INTO {table} ({columns_sql})
        VALUES ({placeholders_sql})
        {conflict_part}
    """

    managed_conn = False
    if conn is None:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = hook.get_conn()
        managed_conn = True

    cursor = conn.cursor()
    try:
        for rec in records:
            values = [rec.get(k) for k in json_keys]
            cursor.execute(insert_sql, values)
        conn.commit()
    finally:
        cursor.close()
        if managed_conn:
            conn.close()
