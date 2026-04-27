import sqlite3
import pytest
from pydantic import BaseModel

from pyext_sqlite import SqliteDatabase


def setup_table(db):
    db.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

def setup_table_str_id(db):
    db.execute("CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, name TEXT, price REAL)")


class UserModel(BaseModel):
    id: int
    name: str
    age: int


class ItemModel(BaseModel):
    id: str
    name: str
    price: float


class TestInitAndLifecycle:
    def test_default_path(self, db):
        assert db._path.endswith(".sqlite")

    def test_memory_database(self):
        mem = SqliteDatabase(":memory:")
        mem.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        mem.execute("INSERT INTO t VALUES (1)")
        assert mem.query_value("SELECT id FROM t") == 1
        mem.close()

    def test_close_idempotent(self, db):
        db.close()
        db.close()

    def test_close_then_operation_raises(self, db):
        db.close()
        with pytest.raises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")


class TestWAL:
    def test_wal_on(self, db):
        db.wal_on()
        result = db.query_value("PRAGMA journal_mode")
        assert result.lower() == "wal"

    def test_wal_off(self, db):
        db.wal_on()
        db.wal_off()
        result = db.query_value("PRAGMA journal_mode")
        assert result.lower() == "delete"


class TestPerformanceOptions:
    def test_sync_mode_full(self, db):
        db.sync_mode("FULL")
        result = db.query_value("PRAGMA synchronous")
        assert result == 2  # FULL = 2

    def test_sync_mode_normal(self, db):
        db.sync_mode("NORMAL")
        result = db.query_value("PRAGMA synchronous")
        assert result == 1  # NORMAL = 1

    def test_sync_mode_off(self, db):
        db.sync_mode("OFF")
        result = db.query_value("PRAGMA synchronous")
        assert result == 0  # OFF = 0

    def test_cache_size(self, db):
        db.cache_size(-32000)
        result = db.query_value("PRAGMA cache_size")
        assert result == -32000

    def test_mmap_size(self, db):
        db.mmap_size(268435456)
        result = db.query_value("PRAGMA mmap_size")
        assert result == 268435456

    def test_fast_write(self, db):
        db.fast_write()
        journal = db.query_value("PRAGMA journal_mode")
        sync = db.query_value("PRAGMA synchronous")
        cache = db.query_value("PRAGMA cache_size")
        assert journal.lower() == "wal"
        assert sync == 1  # NORMAL = 1
        assert cache == -32000

    def test_fast_read(self, db):
        db.fast_read()
        mmap = db.query_value("PRAGMA mmap_size")
        cache = db.query_value("PRAGMA cache_size")
        assert mmap == 268435456
        assert cache == -256000


class TestTransactions:
    def test_commit_persists(self, db):
        setup_table(db)
        db.begin()
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)")
        db.commit()
        assert db.id_exists("users", 1)

    def test_rollback_reverts(self, db):
        setup_table(db)
        db.begin()
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)")
        db.rollback()
        assert not db.id_exists("users", 1)

    def test_begin_twice_raises(self, db):
        db.begin()
        with pytest.raises(sqlite3.OperationalError):
            db.begin()
        db.rollback()


class TestTransactionContext:
    def test_transaction_success_commits(self, db):
        setup_table(db)
        with db.transaction():
            db.insert("users", {"id": 1, "name": "a", "age": 10})
        assert db.id_exists("users", 1)

    def test_transaction_exception_rolls_back(self, db):
        setup_table(db)
        try:
            with db.transaction():
                db.insert("users", {"id": 1, "name": "a", "age": 10})
                raise ValueError("oops")
        except ValueError:
            pass
        assert not db.id_exists("users", 1)

    def test_nested_transaction_raises(self, db):
        setup_table(db)
        with db.transaction():
            with pytest.raises(RuntimeError):
                with db.transaction():
                    pass


class TestExecute:
    def test_execute_insert_returns_rowcount(self, db):
        setup_table(db)
        count = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'bob', 25)")
        assert count == 1

    def test_execute_update_returns_rowcount(self, db):
        setup_table(db)
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'bob', 25)")
        count = db.execute("UPDATE users SET age=26 WHERE id=1")
        assert count == 1

    def test_execute_delete_returns_rowcount(self, db):
        setup_table(db)
        db.execute("INSERT INTO users (id, name, age) VALUES (1, 'bob', 25)")
        count = db.execute("DELETE FROM users WHERE id=1")
        assert count == 1


class TestExecuteMany:
    def test_insert_multiple(self, db):
        setup_table(db)
        count = db.executemany(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            [{"id": 1, "name": "a", "age": 10}, {"id": 2, "name": "b", "age": 20}, {"id": 3, "name": "c", "age": 30}],
        )
        assert count == 3

    def test_empty_params(self, db):
        setup_table(db)
        count = db.executemany(
            "INSERT INTO users (id, name, age) VALUES (:id, :name, :age)",
            [],
        )
        assert count == 0


class TestInsert:
    def test_insert_dict(self, db):
        setup_table(db)
        count = db.insert("users", {"id": 1, "name": "eve", "age": 40})
        assert count == 1
        assert db.find_by_id("users", 1) == {"id": 1, "name": "eve", "age": 40}

    def test_insert_basemodel(self, db):
        setup_table(db)
        user = UserModel(id=2, name="frank", age=50)
        count = db.insert("users", user)
        assert count == 1
        assert db.find_by_id("users", 2) == {"id": 2, "name": "frank", "age": 50}

    def test_insert_empty_dict_raises(self, db):
        setup_table(db)
        with pytest.raises(ValueError):
            db.insert("users", {})

    def test_insert_wrong_type_raises(self, db):
        setup_table(db)
        with pytest.raises(TypeError):
            db.insert("users", 123)

    def test_insert_str_id(self, db):
        setup_table_str_id(db)
        count = db.insert("items", {"id": "a1", "name": "pen", "price": 9.9})
        assert count == 1
        assert db.find_by_id("items", "a1") == {"id": "a1", "name": "pen", "price": 9.9}

    def test_insert_str_id_basemodel(self, db):
        setup_table_str_id(db)
        item = ItemModel(id="b2", name="book", price=29.5)
        count = db.insert("items", item)
        assert count == 1
        assert db.find_by_id("items", "b2")["name"] == "book"


class TestInsertAll:
    def test_insert_multiple_dicts(self, db):
        setup_table(db)
        count = db.insert_all("users", [
            {"id": 1, "name": "a", "age": 10},
            {"id": 2, "name": "b", "age": 20},
            {"id": 3, "name": "c", "age": 30},
        ])
        assert count == 3
        assert db.list_ids("users") == [1, 2, 3]

    def test_insert_multiple_basemodels(self, db):
        setup_table(db)
        users = [
            UserModel(id=4, name="d", age=40),
            UserModel(id=5, name="e", age=50),
        ]
        count = db.insert_all("users", users)
        assert count == 2

    def test_insert_all_empty_list(self, db):
        setup_table(db)
        count = db.insert_all("users", [])
        assert count == 0

    def test_insert_all_str_ids(self, db):
        setup_table_str_id(db)
        count = db.insert_all("items", [
            {"id": "x1", "name": "a", "price": 1.0},
            {"id": "x2", "name": "b", "price": 2.0},
        ])
        assert count == 2
        assert db.list_ids("items") == ["x1", "x2"]


class TestUpdate:
    def test_update_dict(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "old", "age": 20})
        count = db.update("users", {"id": 1, "name": "new", "age": 21})
        assert count == 1
        assert db.find_by_id("users", 1) == {"id": 1, "name": "new", "age": 21}

    def test_update_basemodel(self, db):
        setup_table(db)
        db.insert("users", {"id": 2, "name": "x", "age": 5})
        user = UserModel(id=2, name="y", age=6)
        count = db.update("users", user)
        assert count == 1

    def test_update_missing_id_raises(self, db):
        setup_table(db)
        with pytest.raises(ValueError, match="must contain 'id'"):
            db.update("users", {"name": "no_id"})

    def test_update_not_exists_returns_zero(self, db):
        setup_table(db)
        count = db.update("users", {"id": 99, "name": "ghost", "age": 0})
        assert count == 0

    def test_update_str_id(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "u1", "name": "old", "price": 5.0})
        count = db.update("items", {"id": "u1", "name": "new", "price": 10.0})
        assert count == 1
        assert db.find_by_id("items", "u1") == {"id": "u1", "name": "new", "price": 10.0}


class TestUpdateAll:
    def test_update_multiple(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 1, "name": "a", "age": 1},
            {"id": 2, "name": "b", "age": 2},
        ])
        count = db.update_all("users", [
            {"id": 1, "name": "aa"},
            {"id": 2, "name": "bb"},
        ])
        assert count == 2

    def test_update_all_empty_list(self, db):
        setup_table(db)
        assert db.update_all("users", []) == 0

    def test_update_all_str_ids(self, db):
        setup_table_str_id(db)
        db.insert_all("items", [
            {"id": "v1", "name": "a", "price": 1.0},
            {"id": "v2", "name": "b", "price": 2.0},
        ])
        count = db.update_all("items", [
            {"id": "v1", "name": "aa"},
            {"id": "v2", "name": "bb"},
        ])
        assert count == 2

    def test_update_all_missing_id_raises(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "a", "age": 10})
        with pytest.raises(ValueError, match="must contain 'id'"):
            db.update_all("users", [
                {"id": 1, "name": "aa"},
                {"name": "no_id"},
            ])

    def test_update_all_only_id_raises(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "a", "age": 10})
        with pytest.raises(ValueError, match="must contain fields beyond 'id'"):
            db.update_all("users", [
                {"id": 1, "name": "aa"},
                {"id": 2},
            ])

    def test_update_all_basemodel(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 1, "name": "a", "age": 1},
            {"id": 2, "name": "b", "age": 2},
        ])
        users = [
            UserModel(id=1, name="aa", age=10),
            UserModel(id=2, name="bb", age=20),
        ]
        count = db.update_all("users", users)
        assert count == 2
        assert db.find_by_id("users", 1) == {"id": 1, "name": "aa", "age": 10}
        assert db.find_by_id("users", 2) == {"id": 2, "name": "bb", "age": 20}


class TestUpsert:
    def test_upsert_inserts_new(self, db):
        setup_table(db)
        count = db.upsert("users", {"id": 1, "name": "new", "age": 10})
        assert count == 1
        assert db.find_by_id("users", 1) == {"id": 1, "name": "new", "age": 10}

    def test_upsert_updates_existing(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "old", "age": 10})
        count = db.upsert("users", {"id": 1, "name": "updated", "age": 20})
        assert count == 1
        assert db.find_by_id("users", 1) == {"id": 1, "name": "updated", "age": 20}

    def test_upsert_basemodel(self, db):
        setup_table(db)
        user = UserModel(id=3, name="pm", age=99)
        count = db.upsert("users", user)
        assert count == 1

    def test_upsert_empty_record_raises(self, db):
        setup_table(db)
        with pytest.raises(ValueError):
            db.upsert("users", {})

    def test_upsert_only_id_raises(self, db):
        setup_table(db)
        with pytest.raises(ValueError, match="must contain fields beyond 'id'"):
            db.upsert("users", {"id": 1})

    def test_upsert_str_id_insert(self, db):
        setup_table_str_id(db)
        count = db.upsert("items", {"id": "s1", "name": "cup", "price": 15.0})
        assert count == 1
        assert db.find_by_id("items", "s1")["name"] == "cup"

    def test_upsert_str_id_update(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "s2", "name": "old", "price": 1.0})
        count = db.upsert("items", {"id": "s2", "name": "new", "price": 2.0})
        assert count == 1
        assert db.find_by_id("items", "s2") == {"id": "s2", "name": "new", "price": 2.0}


class TestUpsertAll:
    def test_upsert_all_mixed(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "orig", "age": 1})
        count = db.upsert_all("users", [
            {"id": 1, "name": "updated", "age": 10},
            {"id": 2, "name": "new", "age": 20},
        ])
        assert count == 2
        assert db.find_by_id("users", 1)["name"] == "updated"
        assert db.find_by_id("users", 2)["name"] == "new"

    def test_upsert_all_empty_list(self, db):
        setup_table(db)
        assert db.upsert_all("users", []) == 0

    def test_upsert_all_str_ids(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "w1", "name": "orig", "price": 1.0})
        count = db.upsert_all("items", [
            {"id": "w1", "name": "updated", "price": 10.0},
            {"id": "w2", "name": "new", "price": 20.0},
        ])
        assert count == 2
        assert db.find_by_id("items", "w1")["name"] == "updated"
        assert db.find_by_id("items", "w2")["name"] == "new"

    def test_upsert_all_empty_record_raises(self, db):
        setup_table(db)
        with pytest.raises(ValueError, match="must not be empty"):
            db.upsert_all("users", [
                {"id": 1, "name": "valid", "age": 10},
                {},
            ])

    def test_upsert_all_only_id_raises(self, db):
        setup_table(db)
        with pytest.raises(ValueError, match="must contain fields beyond 'id'"):
            db.upsert_all("users", [
                {"id": 1, "name": "valid", "age": 10},
                {"id": 2},
            ])

    def test_upsert_all_basemodel(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "orig", "age": 1})
        users = [
            UserModel(id=1, name="updated", age=10),
            UserModel(id=2, name="new", age=20),
        ]
        count = db.upsert_all("users", users)
        assert count == 2
        assert db.find_by_id("users", 1) == {"id": 1, "name": "updated", "age": 10}
        assert db.find_by_id("users", 2) == {"id": 2, "name": "new", "age": 20}


class TestDeleteById:
    def test_delete_existing(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "d", "age": 1})
        count = db.delete_by_id("users", 1)
        assert count == 1
        assert not db.id_exists("users", 1)

    def test_delete_not_exists_returns_zero(self, db):
        setup_table(db)
        count = db.delete_by_id("users", 999)
        assert count == 0

    def test_delete_str_id(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "d1", "name": "eraser", "price": 1.0})
        count = db.delete_by_id("items", "d1")
        assert count == 1
        assert not db.id_exists("items", "d1")


class TestQueryValue:
    def test_returns_value(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "q", "age": 42})
        val = db.query_value("SELECT age FROM users WHERE id=:id", {"id": 1})
        assert val == 42

    def test_returns_none_when_no_row(self, db):
        setup_table(db)
        val = db.query_value("SELECT age FROM users WHERE id=:id", {"id": 99})
        assert val is None


class TestQuery:
    def test_returns_all_rows(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 1, "name": "a", "age": 10},
            {"id": 2, "name": "b", "age": 20},
        ])
        rows = db.query("SELECT * FROM users ORDER BY id")
        assert rows == [
            {"id": 1, "name": "a", "age": 10},
            {"id": 2, "name": "b", "age": 20},
        ]

    def test_returns_empty_list(self, db):
        setup_table(db)
        rows = db.query("SELECT * FROM users")
        assert rows == []

    def test_with_params(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 1, "name": "alice", "age": 30},
            {"id": 2, "name": "bob", "age": 20},
        ])
        rows = db.query("SELECT * FROM users WHERE age > :age", {"age": 25})
        assert len(rows) == 1
        assert rows[0]["name"] == "alice"


class TestFindAll:
    def test_find_matching_rows(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 1, "name": "alice", "age": 30},
            {"id": 2, "name": "bob", "age": 20},
            {"id": 3, "name": "charlie", "age": 30},
        ])
        rows = db.find_all("users", "age = :age", {"age": 30})
        assert len(rows) == 2
        assert rows[0]["name"] == "alice"
        assert rows[1]["name"] == "charlie"

    def test_find_all_no_match(self, db):
        setup_table(db)
        rows = db.find_all("users", "age > :age", {"age": 100})
        assert rows == []


class TestFindOne:
    def test_find_one_match(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "alice", "age": 30})
        row = db.find_one("users", "name = :name", {"name": "alice"})
        assert row == {"id": 1, "name": "alice", "age": 30}

    def test_find_one_no_match(self, db):
        setup_table(db)
        row = db.find_one("users", "name = :name", {"name": "nobody"})
        assert row is None

    def test_find_one_multiple_matches_returns_first(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 1, "name": "a", "age": 10},
            {"id": 2, "name": "a", "age": 20},
        ])
        row = db.find_one("users", "name = :name", {"name": "a"})
        assert row is not None
        assert row["name"] == "a"


class TestFindById:
    def test_find_existing(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "f", "age": 7})
        row = db.find_by_id("users", 1)
        assert row == {"id": 1, "name": "f", "age": 7}

    def test_find_missing_returns_none(self, db):
        setup_table(db)
        row = db.find_by_id("users", 999)
        assert row is None

    def test_find_str_id(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "f1", "name": "ruler", "price": 3.0})
        row = db.find_by_id("items", "f1")
        assert row == {"id": "f1", "name": "ruler", "price": 3.0}


class TestIdExists:
    def test_exists_true(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "e", "age": 1})
        assert db.id_exists("users", 1)

    def test_exists_false(self, db):
        setup_table(db)
        assert not db.id_exists("users", 999)

    def test_exists_str_id(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "e1", "name": "stapler", "price": 25.0})
        assert db.id_exists("items", "e1")
        assert not db.id_exists("items", "missing")


class TestListIds:
    def test_empty_table(self, db):
        setup_table(db)
        assert db.list_ids("users") == []

    def test_multiple_ids(self, db):
        setup_table(db)
        db.insert_all("users", [
            {"id": 3, "name": "c", "age": 30},
            {"id": 1, "name": "a", "age": 10},
            {"id": 2, "name": "b", "age": 20},
        ])
        assert db.list_ids("users") == [1, 2, 3]

    def test_list_ids_str(self, db):
        setup_table_str_id(db)
        db.insert_all("items", [
            {"id": "zzz", "name": "c", "price": 3.0},
            {"id": "aaa", "name": "a", "price": 1.0},
            {"id": "mmm", "name": "b", "price": 2.0},
        ])
        assert db.list_ids("items") == ["aaa", "mmm", "zzz"]


class TestContextManager:
    def test_with_statement_closes(self):
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        with SqliteDatabase(tmp.name) as db:
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            db.insert("t", {"id": 1})
        with pytest.raises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")


class TestDel:
    def test_del_closes(self):
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
        tmp.close()
        db = SqliteDatabase(tmp.name)
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        db.__del__()
        with pytest.raises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")


class TestErrorHandling:
    def test_execute_invalid_sql_raises(self, db):
        with pytest.raises(sqlite3.OperationalError):
            db.execute("INVALID SQL")

    def test_insert_into_nonexistent_table_raises(self, db):
        with pytest.raises(sqlite3.OperationalError):
            db.insert("nonexistent", {"id": 1, "name": "a"})

    def test_find_by_id_missing_returns_none(self, db):
        setup_table(db)
        result = db.find_by_id("users", 999)
        assert result is None

    def test_find_one_no_match_returns_none(self, db):
        setup_table(db)
        result = db.find_one("users", "id = :id", {"id": 999})
        assert result is None


class TestEdgeCases:
    def test_zero_id(self, db):
        setup_table(db)
        db.insert("users", {"id": 0, "name": "zero", "age": 1})
        assert db.id_exists("users", 0)
        row = db.find_by_id("users", 0)
        assert row["name"] == "zero"

    def test_negative_id(self, db):
        setup_table(db)
        db.insert("users", {"id": -1, "name": "neg", "age": 1})
        assert db.id_exists("users", -1)
        row = db.find_by_id("users", -1)
        assert row["name"] == "neg"

    def test_very_long_string(self, db):
        setup_table(db)
        long_name = "a" * 10000
        db.insert("users", {"id": 1, "name": long_name, "age": 1})
        row = db.find_by_id("users", 1)
        assert row["name"] == long_name

    def test_unicode(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "你好世界 🌍", "age": 1})
        row = db.find_by_id("users", 1)
        assert row["name"] == "你好世界 🌍"

    def test_special_characters_sql_injection_safe(self, db):
        setup_table(db)
        special_name = "O'Reilly"
        db.insert("users", {"id": 1, "name": special_name, "age": 30})
        row = db.find_by_id("users", 1)
        assert row["name"] == special_name


class TestExtractFieldsTypes:
    def test_extract_fields_list_raises(self, db):
        setup_table(db)
        with pytest.raises(TypeError):
            db.insert("users", [1, 2, 3])  # list 应该报错

    def test_extract_fields_none_raises(self, db):
        setup_table(db)
        with pytest.raises(TypeError):
            db.insert("users", None)

    def test_update_list_raises(self, db):
        setup_table(db)
        db.insert("users", {"id": 1, "name": "a", "age": 10})
        with pytest.raises(TypeError):
            db.update("users", [1, 2, 3])  # list 应该报错

    def test_upsert_list_raises(self, db):
        setup_table(db)
        with pytest.raises(TypeError):
            db.upsert("users", [1, 2, 3])  # list 应该报错


class TestDataTypes:
    def test_null_value(self, db):
        setup_table(db)
        db.execute("INSERT INTO users (id, name) VALUES (1, 'alice')")  # age 为 NULL
        row = db.find_by_id("users", 1)
        assert row["age"] is None

    def test_datetime_string(self, db):
        db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, created_at TEXT)")
        db.execute("INSERT INTO events VALUES (1, '2024-01-01 10:00:00')")
        row = db.query("SELECT * FROM events")[0]
        assert row["created_at"] == "2024-01-01 10:00:00"

    def test_blob(self, db):
        db.execute("CREATE TABLE files (id INTEGER PRIMARY KEY, data BLOB)")
        blob_data = b"\x00\x01\x02\x03"
        db.insert("files", {"id": 1, "data": blob_data})
        row = db.find_by_id("files", 1)
        assert row["data"] == blob_data

    def test_float_precision(self, db):
        setup_table_str_id(db)
        db.insert("items", {"id": "f1", "name": "pi", "price": 3.14159265359})
        row = db.find_by_id("items", "f1")
        assert abs(row["price"] - 3.14159265359) < 0.0001

    def test_boolean_as_integer(self, db):
        db.execute("CREATE TABLE flags (id INTEGER PRIMARY KEY, active INTEGER)")
        db.insert("flags", {"id": 1, "active": True})   # True -> 1
        db.insert("flags", {"id": 2, "active": False})  # False -> 0
        row1 = db.find_by_id("flags", 1)
        row2 = db.find_by_id("flags", 2)
        assert row1["active"] == 1
        assert row2["active"] == 0


class TestExecScript:
    def test_execute_create_tables_and_insert(self, db):
        script = """
            CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT);
            INSERT INTO log (id, msg) VALUES (1, 'hello');
            INSERT INTO log (id, msg) VALUES (2, 'world');
        """
        db.executescript(script)
        assert db.id_exists("log", 1)
        assert db.id_exists("log", 2)
        assert db.find_by_id("log", 1)["msg"] == "hello"

    def test_execute_empty_script(self, db):
        db.executescript("")

    def test_execute_whitespace_only(self, db):
        db.executescript("   ;   ;   ")

    def test_execute_invalid_sql_raises(self, db):
        with pytest.raises(sqlite3.OperationalError):
            db.executescript("INVALID SQL")

    def test_execute_inside_transaction_raises(self, db):
        db.begin()
        try:
            with pytest.raises(RuntimeError, match="Cannot call executescript inside a transaction"):
                db.executescript("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        finally:
            db.rollback()

    def test_execute_after_close_raises(self, db):
        db.close()
        with pytest.raises(sqlite3.ProgrammingError):
            db.executescript("CREATE TABLE t (id INTEGER PRIMARY KEY)")
