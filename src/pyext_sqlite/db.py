import sqlite3
from typing import Any, Optional, Union

from pydantic import BaseModel


class SqliteDatabase:
    def __init__(self, path: str = "db.sqlite") -> None:
        self._path = path
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._in_transaction = False

    def _ensure_open(self) -> None:
        if self._conn is None:
            raise sqlite3.ProgrammingError("database connection is closed")

    def wal_on(self) -> None:
        self._ensure_open()
        self._conn.execute("PRAGMA journal_mode=WAL")

    def wal_off(self) -> None:
        self._ensure_open()
        self._conn.execute("PRAGMA journal_mode=DELETE")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def begin(self) -> None:
        self._ensure_open()
        self._conn.execute("BEGIN")
        self._in_transaction = True

    def commit(self) -> None:
        self._ensure_open()
        self._conn.commit()
        self._in_transaction = False

    def rollback(self) -> None:
        self._ensure_open()
        self._conn.rollback()
        self._in_transaction = False

    def execute(self, sql: str, params: Union[tuple, list] = ()) -> int:
        self._ensure_open()
        cur = self._conn.execute(sql, params)
        if not self._in_transaction:
            self._conn.commit()
        return cur.rowcount

    def executemany(self, sql: str, params: Union[tuple, list] = ()) -> int:
        self._ensure_open()
        cur = self._conn.executemany(sql, params)
        if not self._in_transaction:
            self._conn.commit()
        return cur.rowcount

    def _extract_fields(self, record: Union[dict, BaseModel]) -> dict:
        if isinstance(record, BaseModel):
            return record.model_dump()
        if isinstance(record, dict):
            return record
        raise TypeError("record must be dict or pydantic BaseModel")

    def _build_insert(self, tb_name: str, fields: tuple) -> str:
        columns = ", ".join(f"[{f}]" for f in fields)
        placeholders = ", ".join("?" * len(fields))
        return f"INSERT INTO {tb_name} ({columns}) VALUES ({placeholders})"

    def _build_update(self, tb_name: str, fields: tuple) -> str:
        sets = ", ".join(f"[{f}]=?" for f in fields)
        return f"UPDATE {tb_name} SET {sets} WHERE [id]=?"

    def _build_upsert(self, tb_name: str, fields: tuple) -> str:
        columns = ", ".join(f"[{f}]" for f in fields)
        placeholders = ", ".join("?" * len(fields))
        updates = ", ".join(f"[{f}]=excluded.[{f}]" for f in fields if f != "id")
        return f"INSERT INTO {tb_name} ({columns}) VALUES ({placeholders}) ON CONFLICT([id]) DO UPDATE SET {updates}"

    def insert(self, tb_name: str, record: Union[dict, BaseModel]) -> int:
        data = self._extract_fields(record)
        if not data:
            raise ValueError("record must not be empty")
        fields = tuple(data.keys())
        sql = self._build_insert(tb_name, fields)
        values = tuple(data[f] for f in fields)
        return self.execute(sql, values)

    def insert_all(self, tb_name: str, records: list) -> int:
        if not records:
            return 0
        datas = [self._extract_fields(r) for r in records]
        fields = tuple(datas[0].keys())
        sql = self._build_insert(tb_name, fields)
        params = [tuple(d[f] for f in fields) for d in datas]
        return self.executemany(sql, params)

    def update(self, tb_name: str, record: Union[dict, BaseModel]) -> int:
        data = self._extract_fields(record)
        if "id" not in data:
            raise ValueError("record must contain 'id' field")
        fields = tuple(k for k in data if k != "id")
        if not fields:
            raise ValueError("record must contain fields beyond 'id' to update")
        sql = self._build_update(tb_name, fields)
        values = tuple(data[f] for f in fields) + (data["id"],)
        return self.execute(sql, values)

    def update_all(self, tb_name: str, records: list) -> int:
        if not records:
            return 0
        total = 0
        for record in records:
            total += self.update(tb_name, record)
        return total

    def upsert(self, tb_name: str, record: Union[dict, BaseModel]) -> int:
        data = self._extract_fields(record)
        if not data:
            raise ValueError("record must not be empty")
        non_id_fields = tuple(k for k in data if k != "id")
        if not non_id_fields:
            raise ValueError("record must contain fields beyond 'id' to upsert")
        fields = tuple(data.keys())
        sql = self._build_upsert(tb_name, fields)
        values = tuple(data[f] for f in fields)
        return self.execute(sql, values)

    def upsert_all(self, tb_name: str, records: list) -> int:
        if not records:
            return 0
        total = 0
        for record in records:
            total += self.upsert(tb_name, record)
        return total

    def delete_by_id(self, tb_name: str, id: Union[int, str]) -> int:
        return self.execute(f"DELETE FROM {tb_name} WHERE [id]=?", (id,))

    def query_value(self, sql: str, params: Union[tuple, list] = ()) -> Optional[Any]:
        self._ensure_open()
        cur = self._conn.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    def query(self, sql: str, params: Union[tuple, list] = ()) -> list[dict]:
        self._ensure_open()
        cur = self._conn.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def find_all(self, tb_name: str, where: str, params: Union[tuple, list] = ()) -> list[dict]:
        return self.query(f"SELECT * FROM {tb_name} WHERE {where}", params)

    def find_one(self, tb_name: str, where: str, params: Union[tuple, list] = ()) -> Optional[dict]:
        self._ensure_open()
        cur = self._conn.execute(f"SELECT * FROM {tb_name} WHERE {where}", params)
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)

    def find_by_id(self, tb_name: str, id: Union[int, str]) -> Optional[dict]:
        self._ensure_open()
        cur = self._conn.execute(f"SELECT * FROM {tb_name} WHERE [id]=?", (id,))
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)

    def id_exists(self, tb_name: str, id: Union[int, str]) -> bool:
        return self.find_by_id(tb_name, id) is not None

    def list_ids(self, tb_name: str) -> list[Union[int, str]]:
        self._ensure_open()
        cur = self._conn.execute(f"SELECT [id] FROM {tb_name}")
        return [row[0] for row in cur.fetchall()]

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
