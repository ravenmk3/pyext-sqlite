import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Optional, Union

from pydantic import BaseModel


class SqliteDatabase:

    def __init__(self, path: str = "db.sqlite", thread_safe: bool = True) -> None:
        self._path = path
        self._lock = threading.RLock() if thread_safe else None
        self._conn = sqlite3.connect(path, check_same_thread=not thread_safe)
        self._conn.row_factory = sqlite3.Row
        self._in_transaction = False

    def _ensure_open(self) -> None:
        if self._conn is None:
            raise sqlite3.ProgrammingError("database connection is closed")

    def _pragma(self, name: str, value: Union[str, int]) -> "SqliteDatabase":
        """执行 PRAGMA 命令"""
        self._execute(f"PRAGMA {name}={value}")
        return self

    def wal_on(self) -> "SqliteDatabase":
        return self._pragma("journal_mode", "WAL")

    def wal_off(self) -> "SqliteDatabase":
        return self._pragma("journal_mode", "DELETE")

    def sync_mode(self, mode: str = "NORMAL") -> "SqliteDatabase":
        """设置同步模式: OFF, NORMAL, FULL"""
        return self._pragma("synchronous", mode)

    def cache_size(self, pages: int = -32000) -> "SqliteDatabase":
        """设置页缓存大小，负数表示 KB (如 -32000 = 32MB)"""
        return self._pragma("cache_size", pages)

    def mmap_size(self, size: int = 268435456) -> "SqliteDatabase":
        """设置内存映射大小 (字节)，0 表示禁用"""
        return self._pragma("mmap_size", size)

    def fast_write(self) -> "SqliteDatabase":
        """优化写入性能: WAL + NORMAL同步 + 32MB缓存"""
        return self.wal_on().sync_mode("NORMAL").cache_size(-32000)

    def fast_read(self) -> "SqliteDatabase":
        """优化读取性能: 256MB mmap + 256MB缓存"""
        return self.mmap_size(268435456).cache_size(-256000)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def _execute_impl(self, sql: str, params: dict | None = None):
        """实际执行 SQL，返回 cursor"""
        self._ensure_open()
        return self._conn.execute(sql, params or {})

    def _executemany_impl(self, sql: str, params: list[dict]):
        """实际批量执行 SQL，返回 cursor"""
        self._ensure_open()
        return self._conn.executemany(sql, params)

    def _execute(self, sql: str, params: dict | None = None):
        """带锁保护的执行，返回 cursor"""
        if self._lock and not self._in_transaction:
            # 非事务模式下：获取锁，执行，释放锁
            with self._lock:
                return self._execute_impl(sql, params)
        # 事务模式下：已经在 begin() 中持有锁，直接执行
        return self._execute_impl(sql, params)

    def _executemany(self, sql: str, params: list[dict]):
        """带锁保护的批量执行，返回 cursor"""
        if self._lock and not self._in_transaction:
            with self._lock:
                return self._executemany_impl(sql, params)
        return self._executemany_impl(sql, params)

    def _auto_commit(self, cur) -> None:
        """非事务模式下自动提交"""
        if not self._in_transaction:
            self._conn.commit()

    def begin(self) -> None:
        if self._lock:
            self._lock.acquire()
        self._execute_impl("BEGIN")
        self._in_transaction = True

    def commit(self) -> None:
        self._execute_impl("COMMIT")
        self._in_transaction = False
        if self._lock:
            self._lock.release()

    def rollback(self) -> None:
        self._execute_impl("ROLLBACK")
        self._in_transaction = False
        if self._lock:
            self._lock.release()

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        if self._in_transaction:
            raise RuntimeError("Nested transaction not supported")
        self.begin()
        try:
            yield self
            self.commit()
        except Exception:
            self.rollback()
            raise

    def execute(self, sql: str, params: dict | None = None) -> int:
        cur = self._execute(sql, params)
        self._auto_commit(cur)
        return cur.rowcount

    def executemany(self, sql: str, params: list[dict] | None = None) -> int:
        cur = self._executemany(sql, params or [])
        self._auto_commit(cur)
        return cur.rowcount

    def _extract_fields(self, record: Union[dict, BaseModel]) -> dict:
        if isinstance(record, BaseModel):
            return record.model_dump()
        if isinstance(record, dict):
            return record
        raise TypeError("record must be dict or pydantic BaseModel")

    def _build_insert(self, tb_name: str, fields: tuple) -> str:
        columns = ", ".join(f"[{f}]" for f in fields)
        placeholders = ", ".join(f":{f}" for f in fields)
        return f"INSERT INTO {tb_name} ({columns}) VALUES ({placeholders})"

    def _build_update(self, tb_name: str, fields: tuple) -> str:
        sets = ", ".join(f"[{f}]=:{f}" for f in fields)
        return f"UPDATE {tb_name} SET {sets} WHERE [id]=:id"

    def _build_upsert(self, tb_name: str, fields: tuple) -> str:
        columns = ", ".join(f"[{f}]" for f in fields)
        placeholders = ", ".join(f":{f}" for f in fields)
        updates = ", ".join(f"[{f}]=excluded.[{f}]" for f in fields if f != "id")
        return f"INSERT INTO {tb_name} ({columns}) VALUES ({placeholders}) ON CONFLICT([id]) DO UPDATE SET {updates}"

    def insert(self, tb_name: str, record: Union[dict, BaseModel]) -> int:
        data = self._extract_fields(record)
        if not data:
            raise ValueError("record must not be empty")
        fields = tuple(data.keys())
        sql = self._build_insert(tb_name, fields)
        return self.execute(sql, data)

    def insert_all(self, tb_name: str, records: list) -> int:
        if not records:
            return 0
        datas = [self._extract_fields(r) for r in records]
        fields = tuple(datas[0].keys())
        sql = self._build_insert(tb_name, fields)
        params = [d for d in datas]
        return self.executemany(sql, params)

    def update(self, tb_name: str, record: Union[dict, BaseModel]) -> int:
        data = self._extract_fields(record)
        if "id" not in data:
            raise ValueError("record must contain 'id' field")
        fields = tuple(k for k in data if k != "id")
        if not fields:
            raise ValueError("record must contain fields beyond 'id' to update")
        sql = self._build_update(tb_name, fields)
        return self.execute(sql, data)

    def update_all(self, tb_name: str, records: list) -> int:
        if not records:
            return 0
        datas = [self._extract_fields(r) for r in records]
        for data in datas:
            if "id" not in data:
                raise ValueError("record must contain 'id' field")
            if not any(k != "id" for k in data.keys()):
                raise ValueError("record must contain fields beyond 'id' to update")
        fields = tuple(k for k in datas[0] if k != "id")
        sql = self._build_update(tb_name, fields)
        params = [d for d in datas]
        return self.executemany(sql, params)

    def upsert(self, tb_name: str, record: Union[dict, BaseModel]) -> int:
        data = self._extract_fields(record)
        if not data:
            raise ValueError("record must not be empty")
        non_id_fields = tuple(k for k in data if k != "id")
        if not non_id_fields:
            raise ValueError("record must contain fields beyond 'id' to upsert")
        fields = tuple(data.keys())
        sql = self._build_upsert(tb_name, fields)
        return self.execute(sql, data)

    def upsert_all(self, tb_name: str, records: list) -> int:
        if not records:
            return 0
        datas = [self._extract_fields(r) for r in records]
        for data in datas:
            if not data:
                raise ValueError("record must not be empty")
            if not any(k != "id" for k in data.keys()):
                raise ValueError("record must contain fields beyond 'id' to upsert")
        fields = tuple(datas[0].keys())
        sql = self._build_upsert(tb_name, fields)
        params = [d for d in datas]
        return self.executemany(sql, params)

    def delete_by_id(self, tb_name: str, id: Union[int, str]) -> int:
        return self.execute(f"DELETE FROM {tb_name} WHERE [id]=:id", {"id": id})

    def query_value(self, sql: str, params: dict | None = None) -> Optional[Any]:
        cur = self._execute(sql, params)
        row = cur.fetchone()
        if row is None:
            return None
        return row[0]

    def query(self, sql: str, params: dict | None = None) -> list[dict]:
        cur = self._execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def find_all(self, tb_name: str, where: str, params: dict | None = None) -> list[dict]:
        return self.query(f"SELECT * FROM {tb_name} WHERE {where}", params)

    def find_one(self, tb_name: str, where: str, params: dict | None = None) -> Optional[dict]:
        cur = self._execute(f"SELECT * FROM {tb_name} WHERE {where}", params)
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)

    def find_by_id(self, tb_name: str, id: Union[int, str]) -> Optional[dict]:
        cur = self._execute(f"SELECT * FROM {tb_name} WHERE [id]=:id", {"id": id})
        row = cur.fetchone()
        if row is None:
            return None
        return dict(row)

    def id_exists(self, tb_name: str, id: Union[int, str]) -> bool:
        return self.find_by_id(tb_name, id) is not None

    def list_ids(self, tb_name: str) -> list[Union[int, str]]:
        cur = self._execute(f"SELECT [id] FROM {tb_name}")
        return [row[0] for row in cur.fetchall()]

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
