import os
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor


class StateStore:
    # тут храню время последнего успешного incremental-запуска

    def __init__(self, conn_uri: Optional[str] = None) -> None:
        self.conn_uri = conn_uri or os.environ.get(
            "MARKETING_STATE_PG_URI",
            "postgresql://marketing:marketing@localhost:5432/marketing_state",
        )
        self._ensure_schema()

    def _connect(self):
        return psycopg2.connect(self.conn_uri)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS marketing_export_state (
                        dag_id VARCHAR(250) NOT NULL,
                        task_id VARCHAR(250) NOT NULL,
                        last_successful_ts TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (dag_id, task_id)
                    )
                    """
                )
            conn.commit()

    def get_last_successful_ts(self, dag_id: str, task_id: str) -> Optional[str]:
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT last_successful_ts
                    FROM marketing_export_state
                    WHERE dag_id = %s AND task_id = %s
                    """,
                    (dag_id, task_id),
                )
                row = cur.fetchone()
        if not row:
            return None
        ts = row["last_successful_ts"]
        return ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

    def set_last_successful_ts(
        self,
        dag_id: str,
        task_id: str,
        ts: Optional[str] = None,
    ) -> None:
        value = ts or datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO marketing_export_state (dag_id, task_id, last_successful_ts, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (dag_id, task_id)
                    DO UPDATE SET
                        last_successful_ts = EXCLUDED.last_successful_ts,
                        updated_at = NOW()
                    """,
                    (dag_id, task_id, value),
                )
            conn.commit()
