# AGENTS.md

Python SQLite 扩展库，封装 CRUD，支持 dict 和 pydantic BaseModel。

## 技术栈

- uv
- Python >= 3.14
- pydantic >= 2.0 (可选，仅使用 dict 时非必需)

## Conventions

- 优先使用 dict 作为 SQL 执行参数

## 命令

开发:

```bash
uv sync --group dev
uv run pytest
```

安装:

```bash
pip install -e .
```
