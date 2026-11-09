"""Extra: перенос аналитического слоя из PostgreSQL в ClickHouse (MergeTree-семейство).

PostgreSQL остаётся источником истины. Витрины mart.mart_route_card и mart.ml_price_predictions выгружаются
через COPY ... TO STDOUT (CSV) и заливаются по HTTP-интерфейсу ClickHouse, без дополнительных зависимостей.

Идемпотентность: данные льются в соседнюю таблицу `<имя>__new`, проверяется число строк, затем атомарный
EXCHANGE TABLES. Читатель видит либо старую, либо новую версию; повтор даёт тот же результат.

Запуск:  python -m extra.clickhouse_sync
"""
from __future__ import annotations

import http.client
import io
import logging
import os
import sys
import urllib.parse
from dataclasses import dataclass

from etl.db import PipelineBusy, Settings, connect, load_settings, pipeline_lock, sql_block

log = logging.getLogger("extra.clickhouse_sync")


@dataclass(frozen=True)
class ClickHouseSettings:
    url: str
    user: str
    password: str
    database: str


def load_clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings(
        url=os.getenv("CLICKHOUSE_URL", "http://localhost:8123"),
        user=os.getenv("CLICKHOUSE_USER", "analytics"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "analytics"),
        database=os.getenv("CLICKHOUSE_DB", "analytics"),
    )


class ClickHouseError(RuntimeError):
    pass


class ClickHouseHTTP:
    """Минимальный клиент HTTP-интерфейса ClickHouse (порт 8123)."""

    def __init__(self, cfg: ClickHouseSettings):
        parsed = urllib.parse.urlparse(cfg.url)
        self.secure = parsed.scheme == "https"
        self.host = parsed.hostname or "localhost"
        self.port = parsed.port or (443 if self.secure else 8123)
        self.cfg = cfg

    def _post(self, body: bytes, params: dict[str, str]) -> str:
        cls = http.client.HTTPSConnection if self.secure else http.client.HTTPConnection
        conn = cls(self.host, self.port, timeout=600)
        headers = {"X-ClickHouse-User": self.cfg.user, "X-ClickHouse-Key": self.cfg.password}
        try:
            conn.request("POST", "/?" + urllib.parse.urlencode(params), body=body, headers=headers)
            resp = conn.getresponse()
            text = resp.read().decode("utf-8", errors="replace")
        except OSError as exc:        # DNS, отказ в соединении, таймаут
            raise ClickHouseError(
                f"ClickHouse недоступен по адресу {self.host}:{self.port} ({exc}). "
                f"Проверьте, что контейнер запущен: docker compose --profile clickhouse up -d --wait clickhouse") from exc
        finally:
            conn.close()
        if resp.status != 200:
            raise ClickHouseError(f"ClickHouse HTTP {resp.status}: {text[:500]}")
        return text

    def command(self, sql: str) -> str:
        return self._post(sql.encode("utf-8"), {})

    def scalar(self, sql: str) -> str:
        return self.command(sql).strip()

    def insert_csv(self, table: str, data: bytes) -> None:
        # best_effort: разбор дат с разными форматами; пустые поля Nullable читаются как NULL (в CSV мы пишем \N)
        self._post(data, {"query": f"INSERT INTO {table} FORMAT CSV", "date_time_input_format": "best_effort"})


@dataclass(frozen=True)
class TableSpec:
    name: str
    pg_select: str      # порядок колонок обязан совпадать с DDL
    pg_count: str


TABLES = [
    TableSpec(
        name="mart_route_card",
        pg_select=(
            "SELECT price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category, "
            "loading_city, unloading_city, trips_count, carriers_count, p25_trip, median_trip, p75_trip, "
            "avg_trip, p25_km, median_km, p75_km, cv_rate, mom_pct, yoy_pct, mom_km_pct, yoy_km_pct, "
            "last_price_date, reliability_level, as_of, "
            "to_char(refreshed_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') "
            "FROM mart.mart_route_card"
        ),
        pg_count="SELECT count(*) FROM mart.mart_route_card",
    ),
    TableSpec(
        name="ml_price_predictions",
        pg_select=(
            "SELECT price_id, model_name, split, loading_date, loading_city_gcid, unloading_city_gcid, "
            "body_group, actual_price_rub, predicted_price_rub, abs_error_rub, ape_pct, outlier_mark::int, "
            "is_fact::int, "
            "as_of, to_char(scored_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') "
            "FROM mart.ml_price_predictions"
        ),
        pg_count="SELECT count(*) FROM mart.ml_price_predictions",
    ),
]


def read_ddl(spec: TableSpec, table: str) -> str:
    """DDL таблицы из блока sql/clickhouse.sql (имя блока = имя таблицы)."""
    return sql_block(spec.name, "clickhouse.sql").format(table=table)


def sync_table(client: ClickHouseHTTP, db: str, spec: TableSpec, csv_data: bytes, expected_rows: int) -> int:
    """Залить CSV в `<table>__new`, сверить число строк и атомарно подменить живую таблицу."""
    live, tmp = f"{db}.{spec.name}", f"{db}.{spec.name}__new"
    client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    client.command(read_ddl(spec, live))
    client.command(f"DROP TABLE IF EXISTS {tmp}")
    client.command(f"CREATE TABLE {tmp} AS {live}")
    try:
        if expected_rows:
            client.insert_csv(tmp, csv_data)
        loaded = int(client.scalar(f"SELECT count() FROM {tmp}"))
        if loaded != expected_rows:
            raise ClickHouseError(f"{spec.name}: загружено {loaded} строк, ожидалось {expected_rows}; подмена отменена")
        client.command(f"EXCHANGE TABLES {tmp} AND {live}")
    finally:
        client.command(f"DROP TABLE IF EXISTS {tmp}")
    return loaded


def export_csv(cur, select: str) -> bytes:
    """Выгрузка запроса из PostgreSQL одним COPY (NULL как \\N, формат понятен ClickHouse)."""
    buf = io.BytesIO()
    with cur.copy(f"COPY ({select}) TO STDOUT WITH (FORMAT csv, NULL '\\N')") as copy:
        for chunk in copy:
            buf.write(bytes(chunk))
    return buf.getvalue()


def run(settings: Settings, ch: ClickHouseSettings) -> dict[str, int]:
    client = ClickHouseHTTP(ch)
    result: dict[str, int] = {}
    with connect(settings) as conn, conn.cursor() as cur:
        for spec in TABLES:
            cur.execute(spec.pg_count)
            expected = int(cur.fetchone()[0])
            data = export_csv(cur, spec.pg_select)
            result[spec.name] = sync_table(client, ch.database, spec, data, expected)
            log.info("clickhouse table synced table=%s.%s rows=%d", ch.database, spec.name, result[spec.name])
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings, ch = load_settings(), load_clickhouse_settings()
    try:
        with pipeline_lock(settings):
            run(settings, ch)
    except PipelineBusy as exc:
        log.error("%s", exc)
        return 2
    except ClickHouseError as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
