import tempfile

import pytest

from pyext_sqlite import SqliteDatabase


@pytest.fixture
def db():
    """Create a fresh SqliteDatabase backed by a temporary file."""
    tmp = tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False)
    tmp.close()
    database = SqliteDatabase(tmp.name)
    yield database
    database.close()
