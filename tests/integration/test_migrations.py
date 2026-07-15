from __future__ import annotations

import sqlite3
from pathlib import Path

from alembic import command
from alembic.config import Config
from pytest import MonkeyPatch

from graphrag.config import get_settings
from graphrag.infrastructure.database import Base


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
