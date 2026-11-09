"""ETL-пайплайн: источник (public.*) -> Core DWH (core.*) -> витрины (mart.*).

Запуск:
    python -m etl.pipeline init                                   # DDL core/mart (идемпотентно)
    python -m etl.pipeline core --start-date 2026-09-01 --end-date 2026-09-30   # backfill окна
    python -m etl.pipeline marts                                  # пересчёт витрин
    python -m etl.pipeline all                                    # init + core + marts

Окно --start-date/--end-date относится к prices.loading_date. Без окна загружается весь источник.
Повторный запуск за то же окно даёт тот же результат (DELETE + INSERT окна в одной транзакции).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from typing import Any

from etl.db import PipelineBusy, Settings, connect, load_settings, pipeline_lock, run_block, run_file

log = logging.getLogger("etl.pipeline")

DDL_BLOCKS = ("schemas", "ddl_core", "ddl_mart", "ddl_ml")


def apply_ddl(settings: Settings) -> None:
    with connect(settings) as conn, conn.cursor() as cur:
        for name in DDL_BLOCKS:
            run_block(cur, name)
            log.info("ddl applied block=%s", name)


def _window_label(start: date | None, end: date | None) -> str:
    return f"[{start or '-inf'} .. {end or '+inf'}]"


def load_core(settings: Settings, start: date | None, end: date | None) -> dict[str, Any]:
    """Валидация, очистка, конвертация валют и идемпотентная загрузка окна в Core."""
    params = {"start_date": start, "end_date": end}
    started = datetime.now(timezone.utc)
    t0 = time.monotonic()
    log.info("core load started window=%s", _window_label(start, end))

    with connect(settings) as conn, conn.cursor() as cur:
        run_block(cur, "core_dims")
        run_block(cur, "core_stage", params)

        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE cardinality(reasons) = 0), "
            "count(*) FILTER (WHERE cardinality(reasons) > 0) FROM stg_price"
        )
        rows_read, rows_valid, rows_bad = cur.fetchone()

        cur.execute(
            "SELECT r.reason_code, count(*) FROM stg_price s, unnest(s.reasons) AS r(reason_code) "
            "GROUP BY 1 ORDER BY 2 DESC"
        )
        by_reason = {code: int(n) for code, n in cur.fetchall()}

        run_block(cur, "core_load", params)

        cur.execute(
            "SELECT count(*) FROM core.fact_price f "
            "WHERE f.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date) "
            "AND COALESCE(%(end_date)s::date, 'infinity'::date) "
            "AND NOT EXISTS (SELECT 1 FROM core.fact_price_date d WHERE d.price_id = f.price_id)",
            params,
        )
        without_dates = cur.fetchone()[0]

        cur.execute("ANALYZE core.fact_price; ANALYZE core.fact_price_date; ANALYZE core.fact_price_body_group")

        stats = {
            "rows_read": int(rows_read),
            "rows_loaded": int(rows_valid),
            "rows_quarantined": int(rows_bad),
            "quarantine_by_reason": by_reason,
            "loaded_without_price_dates": int(without_dates),
        }
        cur.execute(
            "INSERT INTO core.etl_run_log (step, window_start, window_end, rows_read, rows_loaded, "
            "rows_quarantined, details, started_at, finished_at) "
            "VALUES ('core_load', %(start_date)s::date, %(end_date)s::date, %(r)s, %(l)s, %(q)s, "
            "%(d)s::jsonb, %(s)s, now())",
            {**params, "r": stats["rows_read"], "l": stats["rows_loaded"], "q": stats["rows_quarantined"],
             "d": json.dumps(stats, ensure_ascii=False), "s": started},
        )

    log.info(
        "core load finished window=%s read=%d loaded=%d quarantined=%d without_price_dates=%d seconds=%.1f",
        _window_label(start, end), stats["rows_read"], stats["rows_loaded"], stats["rows_quarantined"],
        stats["loaded_without_price_dates"], time.monotonic() - t0,
    )
    for code, n in by_reason.items():
        log.info("dq reason=%s rows=%d", code, n)
    return stats


def refresh_marts(settings: Settings, as_of: date) -> dict[str, Any]:
    """Полная перестройка витрины маршрутов от Core (одна транзакция)."""
    t0 = time.monotonic()
    log.info("mart refresh started as_of=%s", as_of)
    with connect(settings) as conn, conn.cursor() as cur:
        run_file(cur, "route_card.sql", {"as_of": as_of})
        cur.execute("SELECT count(*), count(*) FILTER (WHERE reliability_level = 'HIGH') FROM mart.mart_route_card")
        total, high = cur.fetchone()
    log.info("mart refresh finished rows=%d high_reliability=%d seconds=%.1f", total, high, time.monotonic() - t0)
    return {"mart_route_card_rows": int(total), "high_reliability_rows": int(high)}


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["init", "core", "marts", "all"])
    parser.add_argument("--start-date", type=_parse_date, help="нижняя граница prices.loading_date (включительно)")
    parser.add_argument("--end-date", type=_parse_date, help="верхняя граница prices.loading_date (включительно)")
    parser.add_argument("--as-of", type=_parse_date, help="дата снимка (по умолчанию AS_OF из окружения)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    start = args.start_date or settings.start_date
    end = args.end_date or settings.end_date
    as_of = args.as_of or settings.as_of
    if start and end and start > end:
        parser.error("--start-date позже --end-date")

    try:
        with pipeline_lock(settings):
            if args.command in ("init", "all"):
                apply_ddl(settings)
            if args.command in ("core", "all"):
                load_core(settings, start, end)
            if args.command in ("marts", "all"):
                refresh_marts(settings, as_of)
    except PipelineBusy as exc:
        log.error("%s", exc)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
