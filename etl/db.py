"""Инфраструктура: настройки из окружения, соединения PostgreSQL, SQL-блоки, COPY, блокировка запусков."""
from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

import psycopg

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = REPO_ROOT / "sql"
ARTIFACTS_DIR = Path(os.getenv("ARTIFACTS_DIR", REPO_ROOT / "artifacts"))
LOCK_NAME = "smart_logistics_pipeline"


# ----------------------------------------------------------------------------- настройки
def _opt_date(value: str | None) -> date | None:
    value = (value or "").strip()
    return date.fromisoformat(value) if value else None


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    as_of: date                  # дата снимка; не системная дата
    start_date: date | None      # окно ETL по prices.loading_date (None = без ограничения)
    end_date: date | None
    random_seed: int
    ml_models: tuple[str, ...]   # скор по первой модели

    @property
    def conninfo(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"


def load_settings() -> Settings:
    models = os.getenv("ML_MODELS", "catboost,ridge,tree")
    return Settings(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "deals_bigdata"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        as_of=date.fromisoformat(os.getenv("AS_OF", "2026-10-26")),
        start_date=_opt_date(os.getenv("START_DATE")),
        end_date=_opt_date(os.getenv("END_DATE")),
        random_seed=int(os.getenv("RANDOM_SEED", "42")),
        ml_models=tuple(m.strip() for m in models.split(",") if m.strip()),
    )


# ----------------------------------------------------------------------------- коннекты и SQL
def connect(settings: Settings) -> psycopg.Connection:
    """Соединение с клиентской подстановкой параметров.
    """
    return psycopg.connect(settings.conninfo, cursor_factory=psycopg.ClientCursor)


_MARKER = re.compile(r"^-- @@ (\w+)[ \t]*$", re.MULTILINE)


def parse_blocks(text: str) -> dict[str, str]:
    """Разбор файла на именованные блоки по маркеру `-- @@ имя` (до следующего маркера)."""
    marks = list(_MARKER.finditer(text))
    return {
        m.group(1): text[m.end(): marks[i + 1].start() if i + 1 < len(marks) else len(text)].strip("\n") + "\n"
        for i, m in enumerate(marks)
    }


@lru_cache(maxsize=None)
def _file_blocks(filename: str) -> dict[str, str]:
    return parse_blocks((SQL_DIR / filename).read_text(encoding="utf-8"))


def sql_block(name: str, filename: str = "dwh.sql") -> str:
    try:
        return _file_blocks(filename)[name]
    except KeyError as exc:
        raise KeyError(f"в {filename} нет блока {name!r}; есть: {sorted(_file_blocks(filename))}") from exc


def run_block(cur: psycopg.Cursor, name: str, params: dict[str, Any] | None = None, filename: str = "dwh.sql") -> None:
    """Выполнить блок SQL. Без params символ процента в тексте не интерпретируется."""
    cur.execute(sql_block(name, filename), params)


def run_file(cur: psycopg.Cursor, filename: str, params: dict[str, Any] | None = None) -> None:
    cur.execute((SQL_DIR / filename).read_text(encoding="utf-8"), params)


def copy_rows(cur: psycopg.Cursor, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
    """Пакетная загрузка через COPY (без построчных INSERT). Значения должны быть нативными типами Python."""
    n = 0
    with cur.copy(f"COPY {table} ({', '.join(columns)}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            n += 1
    return n


# ----------------------------------------------------------------------------- блокировка запусков
class PipelineBusy(RuntimeError):
    """Другой запуск пайплайна уже держит блокировку."""


@contextmanager
def pipeline_lock(settings: Settings, name: str = LOCK_NAME) -> Iterator[None]:
    """Взаимное исключение запусков через сессионный advisory lock PostgreSQL.
    """
    conn = psycopg.connect(settings.conninfo, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (name,))
            if not cur.fetchone()[0]:
                raise PipelineBusy("уже выполняется другой запуск пайплайна")
        log.info("pipeline lock acquired name=%s", name)
        yield
    finally:
        conn.close()
        log.info("pipeline lock released name=%s", name)
