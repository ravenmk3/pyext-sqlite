# pyext-sqlite

Python SQLite 扩展库，封装常用 CRUD 操作，支持 dict 和 pydantic BaseModel。

## 安装

```bash
uv add /path/to/pyext-sqlite
# 或
pip install /path/to/pyext-sqlite
```

## 快速开始

```python
from pyext_sqlite import SqliteDatabase

db = SqliteDatabase("app.db")

# 建表
db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

# 插入
db.insert("users", {"id": 1, "name": "Alice", "age": 30})

# 查询
user = db.find_by_id("users", 1)  # {"id": 1, "name": "Alice", "age": 30}

# 更新
db.update("users", {"id": 1, "name": "Alice", "age": 31})

# 删除
db.delete_by_id("users", 1)

db.close()
```

## API

| 方法 | 说明 |
|------|------|
| `execute(sql, params)` | 执行 SQL，返回影响行数 |
| `executemany(sql, params)` | 批量执行 SQL |
| `insert(tb, record)` | 插入记录，支持 dict / BaseModel |
| `insert_all(tb, records)` | 批量插入 |
| `update(tb, record)` | 按 id 更新 |
| `update_all(tb, records)` | 批量更新 |
| `upsert(tb, record)` | 插入或更新 (ON CONFLICT) |
| `upsert_all(tb, records)` | 批量 upsert |
| `delete_by_id(tb, id)` | 按 id 删除 |
| `query_value(sql, params)` | 查询单个值 |
| `query(sql, params)` | 执行查询，返回 list[dict] |
| `find_by_id(tb, id)` | 按 id 查询，返回 dict 或 None |
| `find_one(tb, where, params)` | 按条件查询单条，返回 dict 或 None |
| `find_all(tb, where, params)` | 按条件查询多条，返回 list[dict] |
| `id_exists(tb, id)` | 判断 id 是否存在 |
| `list_ids(tb)` | 列出所有 id |
| `wal_on()` / `wal_off()` | 开启/关闭 WAL 模式 |
| `begin()` / `commit()` / `rollback()` | 事务管理 |

支持上下文管理器：`with SqliteDatabase("app.db") as db: ...`

## 开发

```bash
uv sync --group dev
uv run pytest
```

## 依赖

- Python >= 3.14
- pydantic >= 2.0 (可选，仅使用 dict 时非必需)
