"""Тесты DQ, Core, витрины и прогнозов.

Часть 1: эталонная pandas-реализация витрины и её проверка на вручную посчитанных данных (без БД).
Часть 2: интеграционные проверки на реальных данных; нужен PostgreSQL с отработавшим пайплайном:
    docker-compose run --rm --no-deps pipeline pytest -m db
Без доступной БД интеграционные тесты пропускаются.
"""
from datetime import date

import numpy as np
import pandas as pd
import pytest

# ----------------------------------------------------------------------------- часть 1: эталон витрины
KEYS = ["price_month", "loading_city_gcid", "unloading_city_gcid", "body_group", "weight_category"]
ROUTE_KEYS = KEYS[1:]


def reference_route_card(facts: pd.DataFrame, groups: pd.DataFrame, dates: pd.DataFrame, as_of: date) -> pd.DataFrame:
    f = facts[
        facts["is_fact"].astype(bool)
        & facts["price_type"].isin([1, 2, 3, 4, 5, 9])
        & ~facts["complex"].astype(bool)
        & ~facts["outlier_mark"].astype(bool)
        & (facts["price_rub"] > 0)
        & (facts["distance"] > 0)
    ]
    d = dates.copy()
    d["price_date"] = pd.to_datetime(d["price_date"])
    d = d[d["price_date"] <= pd.Timestamp(as_of)]
    d["price_month"] = d["price_date"].dt.to_period("M").dt.to_timestamp()
    pm = d.groupby(["price_id", "price_month"], as_index=False)["price_date"].max().rename(columns={"price_date": "last_price_date"})

    base = f.merge(pm, on="price_id").merge(groups.drop_duplicates(), on="price_id")
    base["price_km"] = base["price_rub"] / base["distance"]

    g = base.groupby(KEYS)
    out = g.agg(
        trips_count=("price_id", "nunique"),
        avg_trip=("price_rub", "mean"),
        sd_trip=("price_rub", "std"),
        last_price_date=("last_price_date", "max"),
    )
    q = g["price_rub"].quantile([0.25, 0.5, 0.75]).unstack()
    q.columns = ["p25_trip", "median_trip", "p75_trip"]
    qk = g["price_km"].quantile([0.25, 0.5, 0.75]).unstack()
    qk.columns = ["p25_km", "median_km", "p75_km"]
    carriers = base[base["price_type"].isin([3, 9])].groupby(KEYS)["organization_id"].nunique().rename("carriers_count")
    out = out.join(q).join(qk).join(carriers).reset_index()
    out["carriers_count"] = out["carriers_count"].fillna(0).astype(int)
    out["cv_rate"] = np.where((out["trips_count"] >= 2) & (out["avg_trip"] != 0), out["sd_trip"] / out["avg_trip"], np.nan)

    def shifted(offset: pd.DateOffset, suffix: str) -> pd.DataFrame:
        s = out[KEYS + ["median_trip", "median_km"]].copy()
        s["price_month"] = s["price_month"] + offset      # ячейка месяца M становится «предыдущей» для M + offset
        return s.rename(columns={"median_trip": f"median_trip_{suffix}", "median_km": f"median_km_{suffix}"})

    out = out.merge(shifted(pd.DateOffset(months=1), "prev"), on=KEYS, how="left")
    out = out.merge(shifted(pd.DateOffset(years=1), "yago"), on=KEYS, how="left")

    def pct(cur, prev):
        return np.where(prev.isna() | (prev == 0), np.nan, (cur / prev - 1) * 100)

    out["mom_pct"] = pct(out["median_trip"], out["median_trip_prev"])
    out["yoy_pct"] = pct(out["median_trip"], out["median_trip_yago"])
    out["mom_km_pct"] = pct(out["median_km"], out["median_km_prev"])
    out["yoy_km_pct"] = pct(out["median_km"], out["median_km_yago"])

    fresh_days = (pd.Timestamp(as_of) - out["last_price_date"]).dt.days
    out["reliability_level"] = np.select(
        [(out["trips_count"] >= 30) & (out["carriers_count"] >= 5) & (fresh_days <= 30), out["trips_count"] >= 10],
        ["HIGH", "MEDIUM"],
        default="LOW_DATA",
    )
    cols = KEYS + [
        "trips_count", "carriers_count", "p25_trip", "median_trip", "p75_trip", "avg_trip",
        "p25_km", "median_km", "p75_km", "cv_rate", "mom_pct", "yoy_pct", "mom_km_pct", "yoy_km_pct",
        "reliability_level",
    ]
    return out[cols].sort_values(KEYS).reset_index(drop=True)


AS_OF = date(2026, 10, 26)




def _frames(rows):
    """rows: (price_id, price_rub, price_type, org, month_dates, extra)"""
    facts, groups, dates = [], [], []
    for pid, price, ptype, org, ds, extra in rows:
        rec = dict(price_id=pid, loading_city_gcid=1, unloading_city_gcid=2, weight_category="FTL", price_rub=price,
                   distance=10.0, price_type=ptype, organization_id=org, complex=False, is_fact=True, outlier_mark=False)
        rec.update(extra)
        facts.append(rec)
        groups.append(dict(price_id=pid, body_group="Тент"))
        dates += [dict(price_id=pid, price_date=x) for x in ds]
    return pd.DataFrame(facts), pd.DataFrame(groups), pd.DataFrame(dates)


def _cell(res, month):
    return res[res["price_month"] == pd.Timestamp(month)].iloc[0]


def test_corridor_trust_mom_and_filters():
    rows = [
        (1, 100.0, 3, 1, ["2026-08-10"], {}),
        (2, 200.0, 3, 2, ["2026-08-11"], {}),
        (3, 300.0, 9, 3, ["2026-08-12", "2026-09-12"], {}),          # долгосрочная: попадает в 2 месяца
        (4, 999.0, 3, 4, ["2026-08-13"], {"outlier_mark": True}),    # выброс исключён
        (5, 999.0, 3, 5, ["2026-08-14"], {"complex": True}),         # составной маршрут исключён
        (6, 999.0, 6, 6, ["2026-08-15"], {"is_fact": False}),        # ставки торгов исключены
        (7, 999.0, 3, 7, ["2026-11-05"], {}),                        # плановый месяц после as_of исключён
    ]
    res = reference_route_card(*_frames(rows), AS_OF)
    aug, sep = _cell(res, "2026-08-01"), _cell(res, "2026-09-01")
    assert aug["trips_count"] == 3 and aug["carriers_count"] == 3
    assert (aug["p25_trip"], aug["median_trip"], aug["p75_trip"], aug["avg_trip"]) == (150.0, 200.0, 250.0, 200.0)
    assert aug["median_km"] == pytest.approx(20.0)
    assert aug["cv_rate"] == pytest.approx(0.5)               # std([100,200,300]) = 100, mean = 200
    assert np.isnan(aug["mom_pct"]) and np.isnan(aug["yoy_pct"])
    assert sep["trips_count"] == 1 and sep["median_trip"] == 300.0
    assert np.isnan(sep["cv_rate"])                           # одно наблюдение
    assert sep["mom_pct"] == pytest.approx(50.0)              # 300 / 200 - 1
    assert "2026-11-01" not in set(res["price_month"].dt.strftime("%Y-%m-%d"))
    assert len(res) == 2


def test_mom_is_null_when_previous_calendar_month_missing():
    rows = [(1, 100.0, 3, 1, ["2026-06-10"], {}), (2, 150.0, 3, 2, ["2026-08-10"], {})]   # июль отсутствует
    res = reference_route_card(*_frames(rows), AS_OF)
    assert np.isnan(_cell(res, "2026-08-01")["mom_pct"])      # не подставляем «последний доступный» месяц


def test_yoy_uses_same_month_previous_year():
    rows = [(1, 100.0, 3, 1, ["2025-09-10"], {}), (2, 130.0, 3, 2, ["2026-09-10"], {})]
    res = reference_route_card(*_frames(rows), AS_OF)
    assert _cell(res, "2026-09-01")["yoy_pct"] == pytest.approx(30.0)


def test_reliability_levels():
    def many(start_id, n, orgs, day):
        return [(start_id + i, 100.0 + i, 3, 1 + i % orgs, [day], {}) for i in range(n)]

    cases = {
        "2026-10-20": (30, 5, "HIGH"),
        "2026-07-31": (30, 5, "MEDIUM"),
        "2026-09-15": (12, 2, "MEDIUM"),
        "2026-06-15": (3, 3, "LOW_DATA"),
        "2026-05-21": (30, 4, "MEDIUM"),
    }
    rows, pid = [], 1
    for day, (n, orgs, _) in cases.items():
        rows += many(pid, n, orgs, day)
        pid += 1000
    res = reference_route_card(*_frames(rows), AS_OF)
    for day, (_, _, expected) in cases.items():
        assert _cell(res, day[:7] + "-01")["reliability_level"] == expected, day



def test_sql_blocks_are_present_and_safe_for_client_side_parameters():
    """Файл dwh.sql склеен из блоков: все на месте, а в параметризованных нет лишнего символа процента."""
    pytest.importorskip("psycopg")
    import re

    from etl.db import parse_blocks, sql_block

    names = ["schemas", "ddl_core", "ddl_mart", "ddl_ml", "core_dims", "core_stage", "core_load", "ml_extract", "checks"]
    for n in names:
        assert sql_block(n).strip(), n
    blocks = parse_blocks("-- @@ a\nSELECT 1;\n-- @@ b\nSELECT 2;\n")
    assert blocks == {"a": "SELECT 1;\n", "b": "SELECT 2;\n"}
    for n in ["core_stage", "core_load", "ml_extract"]:
        stray = re.sub(r"%\((start_date|end_date|train_start)\)s", "", sql_block(n))
        assert "%" not in stray, n
    from etl.db import SQL_DIR

    route_card = (SQL_DIR / "route_card.sql").read_text(encoding="utf-8")
    assert "%" not in re.sub(r"%\(as_of\)s", "", route_card)

# ----------------------------------------------------------------------------- часть 2: интеграция с БД
try:
    from etl.db import PipelineBusy, connect, load_settings, pipeline_lock
    from etl.pipeline import load_core, refresh_marts
except ImportError:          # нет драйвера БД: интеграционные тесты ниже будут пропущены, эталон выше работает
    PipelineBusy = connect = load_settings = pipeline_lock = load_core = refresh_marts = None


@pytest.fixture(scope="module")
def settings():
    if load_settings is None:
        pytest.skip("не установлен драйвер psycopg")
    return load_settings()


@pytest.fixture(scope="module")
def conn(settings):
    try:
        c = connect(settings)
    except Exception as exc:  # нет БД - не наша зона ответственности в unit-прогоне
        pytest.skip(f"PostgreSQL недоступен: {exc}")
    c.autocommit = True
    yield c
    c.close()


def fetch(conn, sql, params=None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


# ------------------------------------------------------------------ Core / DQ
def test_every_source_row_is_loaded_or_quarantined(conn):
    src = scalar(conn, "SELECT count(*) FROM public.prices")
    loaded = scalar(conn, "SELECT count(*) FROM core.fact_price")
    bad = scalar(conn, "SELECT count(DISTINCT price_id) FROM core.dq_quarantine_prices")
    assert src == loaded + bad


def test_core_matches_independent_dq_in_pandas(conn):
    """Те же правила DQ, реализованные заново на pandas, должны дать то же множество строк и те же price_rub."""
    p = fetch(conn, "SELECT id, price, currency_code, nearest_rate_date, distance, total_weight, total_volume, "
                    "loading_city_gcid, unloading_city_gcid FROM public.prices")
    cities = set(fetch(conn, "SELECT DISTINCT gc_id FROM public.cities")["gc_id"])
    fx = fetch(conn, "SELECT num_code AS currency_code, setting_date AS nearest_rate_date, max(value) AS rate "
                     "FROM public.exchange_rates GROUP BY 1, 2")
    p = p.merge(fx, on=["currency_code", "nearest_rate_date"], how="left")
    p["rate"] = np.where(p["currency_code"] == "643", 1.0, p["rate"])

    ok = (
        (p["price"] > 0)
        & ~((p["currency_code"] == "643") & (p["price"] <= 1000))
        & (p["distance"] > 0) & (p["total_weight"] >= 0) & (p["total_volume"] >= 0)
        & np.isfinite(p[["price", "distance", "total_weight", "total_volume"]].astype(float)).all(axis=1)
        & p["loading_city_gcid"].isin(cities) & p["unloading_city_gcid"].isin(cities)
        & p["currency_code"].isin(["643", "840", "978"])
        & p["rate"].notna() & (p["rate"] > 0)
    )
    expected = p[ok].set_index("id")
    got = fetch(conn, "SELECT price_id, price_rub FROM core.fact_price").set_index("price_id")
    assert set(expected.index) == set(got.index)
    exp_rub = (expected["price"] * expected["rate"]).round(2)
    joined = got.join(exp_rub.rename("exp"))
    assert np.allclose(joined["price_rub"], joined["exp"], atol=0.011)


def test_no_invalid_rows_in_core(conn):
    assert scalar(conn, "SELECT count(*) FROM core.fact_price WHERE price_rub <= 0 OR distance <= 0 "
                        "OR total_weight < 0 OR total_volume < 0") == 0
    assert scalar(conn, "SELECT count(*) FROM core.fact_price WHERE currency_code = '643' AND price_orig <= 1000") == 0


def test_foreign_currency_uses_exact_rate_on_nearest_rate_date(conn):
    bad = scalar(conn, """
        SELECT count(*) FROM core.fact_price f
        JOIN public.exchange_rates r ON r.num_code = f.currency_code AND r.setting_date = f.nearest_rate_date
        WHERE f.currency_code <> '643' AND abs(f.price_rub - round((f.price_orig * r.value)::numeric, 2)) > 0.011""")
    assert bad == 0


def test_categories_are_filled(conn):
    assert scalar(conn, "SELECT count(*) FROM core.fact_price WHERE weight_category IS NULL OR volume_category IS NULL") == 0


def test_city_dimension_is_deduplicated_by_min_id(conn):
    assert scalar(conn, "SELECT count(*) - count(DISTINCT gc_id) FROM core.dim_city") == 0
    wrong = scalar(conn, "SELECT count(*) FROM core.dim_city d WHERE d.city_id <> "
                         "(SELECT min(id) FROM public.cities c WHERE c.gc_id = d.gc_id)")
    assert wrong == 0


def test_each_price_has_body_group_and_no_duplicates(conn):
    assert scalar(conn, "SELECT count(*) FROM core.fact_price f WHERE NOT EXISTS "
                        "(SELECT 1 FROM core.fact_price_body_group g WHERE g.price_id = f.price_id)") == 0
    assert scalar(conn, "SELECT count(*) FROM (SELECT price_id, body_group FROM core.fact_price_body_group "
                        "GROUP BY 1, 2 HAVING count(*) > 1) t") == 0


# Контрольные суммы слоёв: содержимое в детерминированном порядке, без служебных меток времени.
CHECKSUMS = {
    "core.fact_price": "SELECT count(*), md5(string_agg(concat_ws('|', price_id, loading_date, price_rub, outlier_mark, "
                       "weight_category, volume_category, currency_code), ';' ORDER BY price_id)) FROM core.fact_price",
    "core.fact_price_body_group": "SELECT count(*), md5(string_agg(price_id || '|' || body_group, ';' "
                                  "ORDER BY price_id, body_group)) FROM core.fact_price_body_group",
    "core.fact_price_date": "SELECT count(*), md5(string_agg(price_id || '|' || price_date, ';' "
                            "ORDER BY price_id, price_date)) FROM core.fact_price_date",
    "core.dq_quarantine_prices": "SELECT count(*), md5(string_agg(price_id || '|' || reason_code, ';' "
                                 "ORDER BY price_id, reason_code)) FROM core.dq_quarantine_prices",
    "mart.mart_route_card": "SELECT count(*), md5(string_agg(concat_ws('|', price_month, loading_city_gcid, "
                            "unloading_city_gcid, body_group, weight_category, trips_count, carriers_count, median_trip, "
                            "reliability_level), ';' ORDER BY price_month, loading_city_gcid, unloading_city_gcid, "
                            "body_group, weight_category)) FROM mart.mart_route_card",
}


def checksums(conn, tables) -> dict:
    return {t: tuple(fetch(conn, CHECKSUMS[t]).iloc[0].tolist()) for t in tables}


CORE_TABLES = [t for t in CHECKSUMS if t.startswith("core.")]


def test_core_rerun_for_window_is_idempotent(settings, conn):
    before = checksums(conn, CORE_TABLES)
    for _ in range(2):
        load_core(settings, date(2026, 9, 1), date(2026, 9, 30))
        assert checksums(conn, CORE_TABLES) == before


def test_moved_loading_date_does_not_leave_stale_copy(settings, conn):
    """Сделка переехала из одного окна в другое: после загрузки нового окна старой копии быть не должно."""
    pid = scalar(conn, "SELECT price_id FROM core.fact_price WHERE loading_date = %(d)s::date "
                       "AND currency_code = '643' ORDER BY price_id LIMIT 1", {"d": date(2026, 9, 10)})
    new_date = date(2026, 8, 12)
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE public.prices SET loading_date = %(n)s::date WHERE id = %(i)s", {"n": new_date, "i": pid})
        load_core(settings, new_date, new_date)             # загружаем ТОЛЬКО новое окно
        rows = fetch(conn, "SELECT loading_date FROM core.fact_price WHERE price_id = %(i)s", {"i": pid})
        assert len(rows) == 1 and rows.iloc[0, 0] == new_date
    finally:
        with conn.cursor() as cur:
            cur.execute("UPDATE public.prices SET loading_date = %(o)s::date WHERE id = %(i)s",
                        {"o": date(2026, 9, 10), "i": pid})
        load_core(settings, date(2026, 8, 12), date(2026, 8, 12))
        load_core(settings, date(2026, 9, 10), date(2026, 9, 10))


def test_second_runner_is_rejected_while_lock_is_held(settings, conn):
    with pipeline_lock(settings):
        with pytest.raises(PipelineBusy):
            with pipeline_lock(settings):
                pass
    with pipeline_lock(settings):       # после освобождения блокировка снова доступна
        pass


# ------------------------------------------------------------------ Mart
def test_mart_grain_is_unique_and_has_no_future_months(conn, settings):
    assert scalar(conn, "SELECT count(*) - (SELECT count(*) FROM (SELECT DISTINCT price_month, loading_city_gcid, "
                        "unloading_city_gcid, body_group, weight_category FROM mart.mart_route_card) t) "
                        "FROM mart.mart_route_card") == 0
    assert scalar(conn, "SELECT count(*) FROM mart.mart_route_card WHERE price_month > %(d)s::date",
                  {"d": settings.as_of}) == 0


def test_mart_quartiles_are_ordered(conn):
    assert scalar(conn, "SELECT count(*) FROM mart.mart_route_card "
                        "WHERE NOT (p25_trip <= median_trip AND median_trip <= p75_trip "
                        "AND p25_km <= median_km AND median_km <= p75_km)") == 0


def test_mart_matches_pandas_reference(conn, settings):
    facts = fetch(conn, "SELECT price_id, loading_city_gcid, unloading_city_gcid, weight_category, price_rub, distance, "
                        "price_type, organization_id, complex, is_fact, outlier_mark FROM core.fact_price")
    groups = fetch(conn, "SELECT price_id, body_group FROM core.fact_price_body_group")
    dates = fetch(conn, "SELECT price_id, price_date FROM core.fact_price_date")
    ref = reference_route_card(facts, groups, dates, settings.as_of)

    mart = fetch(conn, "SELECT * FROM mart.mart_route_card")
    mart["price_month"] = pd.to_datetime(mart["price_month"])
    mart = mart[ref.columns].sort_values(KEYS).reset_index(drop=True)

    assert len(mart) == len(ref)
    assert (mart[KEYS].to_numpy() == ref[KEYS].to_numpy()).all()
    for col in ["trips_count", "carriers_count", "reliability_level"]:
        assert (mart[col].to_numpy() == ref[col].to_numpy()).all(), col
    for col in ["p25_trip", "median_trip", "p75_trip", "avg_trip", "p25_km", "median_km", "p75_km",
                "cv_rate", "mom_pct", "yoy_pct", "mom_km_pct", "yoy_km_pct"]:
        assert np.allclose(mart[col].astype(float), ref[col].astype(float), rtol=1e-9, atol=1e-9, equal_nan=True), col


def test_mart_refresh_is_idempotent(settings, conn):
    before = checksums(conn, ["mart.mart_route_card"])
    refresh_marts(settings, settings.as_of)
    refresh_marts(settings, settings.as_of)
    assert checksums(conn, ["mart.mart_route_card"]) == before


# ------------------------------------------------------------------ ML
def test_predictions_contract(conn, settings):
    n = scalar(conn, "SELECT count(*) FROM mart.ml_price_predictions")
    if n == 0:
        pytest.skip("прогнозы ещё не посчитаны (python -m ml.train_score)")
    assert scalar(conn, "SELECT count(*) FROM (SELECT price_id, model_name FROM mart.ml_price_predictions "
                        "GROUP BY 1, 2 HAVING count(*) > 1) t") == 0
    assert scalar(conn, "SELECT count(*) FROM mart.ml_price_predictions WHERE split = 'future' AND "
                        "(actual_price_rub IS NOT NULL OR abs_error_rub IS NOT NULL OR ape_pct IS NOT NULL)") == 0
    assert scalar(conn, "SELECT count(*) FROM mart.ml_price_predictions WHERE split = 'test' AND "
                        "(loading_date < %(t)s::date OR loading_date > %(a)s::date)",
                  {"t": date(2026, 10, 5), "a": settings.as_of}) == 0
    # плановые заявки скорятся независимо от is_fact (включая ставки торгов)
    expected_future = scalar(conn, "SELECT count(*) FROM core.fact_price WHERE NOT complex AND "
                                   "loading_date > %(a)s::date", {"a": settings.as_of})
    assert scalar(conn, "SELECT count(*) FROM mart.ml_price_predictions WHERE split = 'future'") == expected_future


def test_check_constraint_blocks_actuals_for_future_rows(conn):
    """Гарантия на уровне БД: даже ошибочный скоринг не сможет записать факт для плановой заявки."""
    import psycopg

    with pytest.raises(psycopg.errors.CheckViolation):
        with conn.transaction():          # откат при любом исходе
            conn.execute(
                "INSERT INTO mart.ml_price_predictions (price_id, model_name, split, loading_date, loading_city_gcid, "
                "unloading_city_gcid, body_group, actual_price_rub, predicted_price_rub, outlier_mark, as_of, is_fact) "
                "VALUES (-1, 'probe', 'future', '2027-01-01', 1, 1, 'x', 100, 100, false, '2026-10-26', true)")





def test_model_metrics_store_params_and_feature_importance(conn):
    if scalar(conn, "SELECT count(*) FROM mart.ml_model_metrics") == 0:
        pytest.skip("метрики ещё не посчитаны (python -m ml.train_score)")
    assert scalar(conn, "SELECT count(*) FROM mart.ml_model_metrics WHERE params IS NULL") == 0
    assert scalar(conn, "SELECT count(*) FROM mart.ml_model_metrics WHERE model_name = 'catboost' "
                        "AND feature_importance IS NULL") == 0


# интеграционные тесты (используют фикстуру conn) получают маркер db: `pytest -m "not db"` их пропускает
import inspect  # noqa: E402

for _name, _obj in list(globals().items()):
    if _name.startswith("test_") and callable(_obj) and "conn" in inspect.signature(_obj).parameters:
        globals()[_name] = pytest.mark.db(_obj)
