# plugins/operators/stock_list_operator.py
from __future__ import annotations

from typing import Any, Dict, List, Sequence

from airflow.models import BaseOperator
from airflow.utils.context import Context

from plugins.utils.etl_blocks import PostgresUpsertLoader


class StockListUpsertOperator(BaseOperator):
    """
    Upsert stock list records vào Postgres.

    records lấy từ XCom (output của task fetch_stock_list), default từ task_id="fetch_stock_list".
    """

    template_fields = ("target_table", "postgres_conn_id", "source_task_id")

    def __init__(
        self,
        *,
        postgres_conn_id: str,
        target_table: str,
        source_task_id: str = "fetch_stock_list",
        columns: Sequence[str] | None = None,
        conflict_keys: Sequence[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.target_table = target_table
        self.source_task_id = source_task_id
        self.columns = tuple(columns or ("code", "fullname_vi", "loaidn", "san"))
        self.conflict_keys = tuple(conflict_keys or ("code",))
        self.loader = PostgresUpsertLoader(
            postgres_conn_id=self.postgres_conn_id,
            target_table=self.target_table,
            columns=self.columns,
            conflict_keys=self.conflict_keys,
        )

    def execute(self, context: Context) -> None:
        ti = context["ti"]
        records: List[Dict[str, Any]] = ti.xcom_pull(task_ids=self.source_task_id)

        if not records:
            raise ValueError("No stock list data to upsert")

        self.loader.run(records)
