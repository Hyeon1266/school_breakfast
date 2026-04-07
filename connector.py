from __future__ import annotations

import mysql.connector
from mysql.connector import Error
from typing import Any

from config import DB_CONFIG, BATCH_SIZE, get_logger

logger = get_logger(__name__)


class DBConnector:
    def __init__(self):
        self._conn = None
        self._cursor = None

    def __enter__(self) -> DBConnector:
        try:
            self._conn = mysql.connector.connect(**DB_CONFIG)
            self._cursor = self._conn.cursor(dictionary=True)  # dict로 받아야 r["col"] 접근 가능
            return self
        except Error as e:
            logger.error(f"DB 연결 실패: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type and self._conn and self._conn.is_connected():
            self._conn.rollback()
            logger.warning(f"롤백: {exc_val}")

        if self._cursor:
            self._cursor.close()
        if self._conn and self._conn.is_connected():
            self._conn.close()
        return False

    def fetch(self, query: str, params: tuple = ()) -> list[dict]:
        self._cursor.execute(query, params)
        return self._cursor.fetchall()

    def fetch_one(self, query: str, params: tuple = ()) -> dict | None:
        self._cursor.execute(query, params)
        return self._cursor.fetchone()

    def execute(self, query: str, params: tuple = ()) -> int:
        self._cursor.execute(query, params)
        self._conn.commit()
        return self._cursor.rowcount

    def executemany_batch(
        self,
        query: str,
        data: list[tuple],
        batch_size: int = BATCH_SIZE,
    ) -> int:
        if not data:
            return 0

        total = 0
        for i in range(0, len(data), batch_size):
            chunk = data[i : i + batch_size]
            self._cursor.executemany(query, chunk)
            self._conn.commit()
            total += self._cursor.rowcount
        return total

    def table_exists(self, table_name: str) -> bool:
        row = self.fetch_one(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (DB_CONFIG["database"], table_name),
        )
        return row["cnt"] > 0 if row else False

    def get_id(self, table: str, id_col: str, name_col: str, val: Any) -> int | None:
        row = self.fetch_one(
            f"SELECT {id_col} AS id FROM {table} WHERE {name_col} = %s",
            (val,),
        )
        return row["id"] if row else None
