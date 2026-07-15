from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path
from types import ModuleType

from alembic import command
from alembic.config import Config
from pytest import MonkeyPatch

from graphrag.config import get_settings
from graphrag.infrastructure.database import Base


def _load_migration(filename: str) -> ModuleType:
    path = Path("alembic/versions") / filename
    spec = importlib.util.spec_from_file_location(filename.removesuffix(".py"), path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }


def test_initial_migration_upgrade_downgrade_and_upgrade_again(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    database = tmp_path / "migration.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{database}")
    get_settings.cache_clear()
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    expected = set(Base.metadata.tables)
    assert _tables(database) == expected | {"alembic_version"}
    command.check(config)
    command.downgrade(config, "base")
    assert _tables(database) == {"alembic_version"}
    command.upgrade(config, "head")
    assert _tables(database) == expected | {"alembic_version"}
    get_settings.cache_clear()


def test_context_memory_migration_backfills_text_before_enforcing_not_null(
    monkeypatch: MonkeyPatch,
) -> None:
    migration = _load_migration("20260714_0003_context_memory.py")
    added_columns: dict[str, object] = {}
    altered_columns: list[tuple[str, dict[str, object]]] = []

    class EmptyResult:
        def all(self) -> list[object]:
            return []

    class Connection:
        def execute(self, _statement: object) -> EmptyResult:
            return EmptyResult()

    class BatchOperations:
        def __enter__(self) -> BatchOperations:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def alter_column(self, name: str, **kwargs: object) -> None:
            altered_columns.append((name, kwargs))

    monkeypatch.setattr(
        migration.op,
        "add_column",
        lambda _table, column: added_columns.setdefault(column.name, column),
    )
    monkeypatch.setattr(migration.op, "get_bind", Connection)
    monkeypatch.setattr(migration.op, "create_table", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(migration.op, "create_index", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(migration.op, "batch_alter_table", lambda _table: BatchOperations())

    migration.upgrade()

    summary = added_columns["summary"]
    assert summary.nullable is True
    assert summary.server_default is None
    assert any(
        name == "summary" and kwargs.get("nullable") is False for name, kwargs in altered_columns
    )
