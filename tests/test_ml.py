"""Тесты ML-контура без БД: признаки и защита от утечек, хронологический split, метрики, обучение и скоринг.

Данные для тестов генерируются небольшой pandas-имитацией выборки core (по мотивам sql/synthetic.sql).
CatBoost проверяется, если установлен; в остальных тестах его роль играет sklearn-бустинг (hgb).
"""
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from ml.features import (CAT_FEATURES, FEATURES, FORBIDDEN_FEATURES, assert_no_leakage, build_features, cyclical,
                         make_windows, split_masks)
from ml.train_score import PREDICTION_COLUMNS, fit_and_score, regression_metrics

# ----------------------------------------------------------------------------- фикстура данных
CITIES = [
    (1001, "г. Москва", 55.7558, 37.6173), (1002, "г. Санкт-Петербург", 59.9343, 30.3351),
    (1003, "Республика Татарстан", 55.7961, 49.1064), (1004, "Свердловская обл.", 56.8389, 60.6057),
    (1005, "Новосибирская обл.", 55.0084, 82.9357), (1006, "Нижегородская обл.", 56.2965, 43.9361),
    (1007, "Самарская обл.", 53.1959, 50.1002), (1008, "Ростовская обл.", 47.2357, 39.7015),
    (1009, "Краснодарский край", 45.0355, 38.9753), (1010, "Воронежская обл.", 51.6720, 39.1843),
    (1011, "Челябинская обл.", 55.1644, 61.4368), (1012, "Республика Башкортостан", 54.7388, 55.9721),
]
BODY = {56: ("Тент", 72.0), 4: ("Рефрижератор", 95.0), 2: ("Изотерм", 84.0), 15: ("Борт", 68.0), 50: ("Прочие", 80.0)}


def _distance(a, b) -> float:
    la1, lo1, la2, lo2 = map(np.radians, (a[2], a[3], b[2], b[3]))
    c = np.cos(la1) * np.cos(la2) * np.cos(lo2 - lo1) + np.sin(la1) * np.sin(la2)
    return float(np.round(1.25 * 6371 * np.arccos(np.minimum(1.0, c))))


def weight_category(w: float) -> str:
    w = round(w, 1)
    for thr, cat in ((30, "G20T"), (10, "FTL"), (5, "10T"), (2.5, "5T"), (0.5, "L5T")):
        if w >= thr:
            return cat
    return "OTH"


def make_core_frame(n: int = 12000, seed: int = 7, as_of: date = date(2026, 10, 26)) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    start = as_of - timedelta(days=150)
    rows = []
    for i in range(n):
        a, b = rng.choice(len(CITIES), size=2, replace=False)
        ca, cb = CITIES[a], CITIES[b]
        if rng.random() < 0.03:
            ld = as_of + timedelta(days=int(rng.integers(1, 110)))      # плановая погрузка
        else:
            ld = start + timedelta(days=int(rng.integers(0, 150)))
        dist = _distance(ca, cb)
        r_w = rng.random()
        w = (30 + round(rng.random() * 8, 1) if r_w < 0.05 else 10 + round(rng.random() * 19.9, 1) if r_w < 0.6
             else 5 + round(rng.random() * 4.9, 1) if r_w < 0.72 else 2.5 + round(rng.random() * 2.4, 1) if r_w < 0.82
             else 0.5 + round(rng.random() * 1.9, 1))
        std = int(rng.choice(list(BODY), p=[0.55, 0.2, 0.1, 0.1, 0.05]))
        mult = 1.1 if w >= 30 else 1 if w >= 10 else 0.75 if w >= 5 else 0.55 if w >= 2.5 else 0.4
        season = 1 + 0.08 * np.cos(2 * np.pi * (ld.month - 12) / 12)
        price = (BODY[std][1] * dist + 6000) * season * mult * np.exp(0.16 * rng.standard_normal())
        out = rng.random()
        if out < 0.012:
            price *= 0.25
        elif out > 0.988:
            price *= 4
        marked = (out < 0.010 or out > 0.990) and ld < as_of          # свежие выбросы ещё не размечены
        vol = float(rng.choice([0, 120, 135, 60, 90]))
        rows.append(
            dict(
                price_id=i + 1, loading_date=pd.Timestamp(ld),
                loading_city_gcid=ca[0], unloading_city_gcid=cb[0],
                distance=dist, total_weight=float(w), total_volume=vol,
                weight_category=weight_category(w), volume_category="FTL" if 0 < vol <= 120 else "OTH",
                body_group=BODY[std][0],
                origin_region=ca[1], destination_region=cb[1],
                origin_lat=ca[2], origin_lon=ca[3], dest_lat=cb[2], dest_lon=cb[3],
                actual_price_rub=float(round(price, 2)), outlier_mark=bool(marked),
                is_fact=bool(rng.random() > 0.14),      # ~14 процентов - ставки торгов (price_type 6)
            )
        )
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------------- признаки, split, метрики
def test_cyclical_wraps_around_year_boundary():
    s_dec, c_dec = cyclical([365], 365.25)
    s_jan, c_jan = cyclical([1], 365.25)
    # 31 декабря и 1 января должны быть соседями на единичной окружности
    assert np.hypot(s_dec[0] - s_jan[0], c_dec[0] - c_jan[0]) < 0.05
    s, c = cyclical(np.arange(1, 13), 12)
    assert np.allclose(s**2 + c**2, 1.0)


def test_build_features_has_no_target_or_forbidden_columns():
    df = make_core_frame(300)
    X = build_features(df)
    assert list(X.columns) == FEATURES
    assert not (set(X.columns) & FORBIDDEN_FEATURES)
    assert "actual_price_rub" not in X.columns and "outlier_mark" not in X.columns


@pytest.mark.parametrize(
    "bad", ["price_with_vat", "price_group_id", "avg_price", "min_km_price", "max_km_price", "price_per_km", "profit", "carrier_cost"]
)
def test_leakage_guard_rejects_forbidden(bad):
    with pytest.raises(ValueError):
        assert_no_leakage(["distance", bad])


def test_time_windows_match_task_spec():
    w = make_windows(date(2026, 10, 26))
    assert w.train_start == date(2026, 7, 26)
    assert w.test_start == date(2026, 10, 5)


def test_split_boundaries_are_chronological_and_disjoint():
    w = make_windows(date(2026, 10, 26))
    d = pd.Series(pd.to_datetime(["2026-07-25", "2026-07-26", "2026-10-04", "2026-10-05", "2026-10-26", "2026-10-27"]))
    m = split_masks(d, w)
    assert m["train"].tolist() == [False, True, True, False, False, False]
    # 2026-10-05 входит только в test; as_of входит в test; 2026-10-27 - только future
    assert m["test"].tolist() == [False, False, False, True, True, False]
    assert m["future"].tolist() == [False, False, False, False, False, True]
    assert not (m["train"] & m["test"]).any()
    assert not (m["fit"] & m["val"]).any()


def test_regression_metrics_known_values():
    m = regression_metrics([100.0, 200.0], [110.0, 180.0])
    assert m["mae_rub"] == pytest.approx(15.0)
    assert m["rmse_rub"] == pytest.approx(np.sqrt((100 + 400) / 2))
    assert m["mape_pct"] == pytest.approx((10 / 100 + 20 / 200) / 2 * 100)
    assert m["r2"] == pytest.approx(1 - 500 / 5000)


def test_categorical_columns_are_plain_object_strings():
    X = build_features(make_core_frame(200))
    for col in CAT_FEATURES:
        assert X[col].dtype == object, col
        assert all(isinstance(v, str) for v in X[col])
    assert not X.isna().any().any()


# ----------------------------------------------------------------------------- обучение и скоринг
AS_OF = date(2026, 10, 26)


@pytest.fixture(scope="module")
def frame():
    return make_core_frame(9000)


def test_fit_and_score_contract(frame):
    w = make_windows(AS_OF)
    res = fit_and_score(frame, w, ["hgb", "ridge"], seed=42)
    pred = res["predictions"]
    assert list(pred.columns) == PREDICTION_COLUMNS
    assert pred["price_id"].is_unique
    assert set(pred["split"]) == {"test", "future"}

    d_all = pd.to_datetime(frame["loading_date"]).dt.date
    future_src = frame[d_all > AS_OF]
    fut = pred[pred["split"] == "future"]
    # плановые заявки скорятся ВСЕ, в том числе не-факты (ставки торгов): фактического тарифа у них нет
    assert set(fut["price_id"]) == set(future_src["price_id"])
    assert (~future_src["is_fact"]).any()
    assert fut["actual_price_rub"].isna().all() and fut["abs_error_rub"].isna().all() and fut["ape_pct"].isna().all()

    tst = pred[pred["split"] == "test"]
    assert tst["actual_price_rub"].notna().all()
    assert np.allclose(tst["abs_error_rub"], (tst["actual_price_rub"] - tst["predicted_price_rub"]).abs())
    assert (pred["predicted_price_rub"] >= 0).all()
    # в test-прогнозах только очищенные факты в границах test-периода
    src = frame.set_index("price_id").loc[tst["price_id"]]
    assert src["is_fact"].all() and not src["outlier_mark"].any()
    d = pd.to_datetime(tst["loading_date"]).dt.date
    assert ((d >= w.test_start) & (d <= w.as_of)).all()


def test_gradient_boosting_beats_linear_baseline(frame):
    res = fit_and_score(frame, make_windows(AS_OF), ["hgb", "ridge"], seed=42)
    assert res["metrics"]["hgb"]["mape_pct"] < res["metrics"]["ridge"]["mape_pct"]


def test_pipeline_is_reproducible(frame):
    w = make_windows(AS_OF)
    a = fit_and_score(frame, w, ["hgb"], seed=42)["predictions"]["predicted_price_rub"].to_numpy()
    b = fit_and_score(frame, w, ["hgb"], seed=42)["predictions"]["predicted_price_rub"].to_numpy()
    assert np.allclose(a, b)


def test_train_excludes_marked_outliers(frame):
    res = fit_and_score(frame, make_windows(AS_OF), ["ridge"], seed=1)
    w = make_windows(AS_OF)
    d = pd.to_datetime(frame["loading_date"]).dt.date
    expected = int(((d >= w.train_start) & (d < w.test_start) & ~frame["outlier_mark"] & frame["is_fact"]).sum())
    assert res["counts"]["train_rows_clean"] == expected


def test_catboost_runs_if_installed(frame):
    pytest.importorskip("catboost")
    res = fit_and_score(frame, make_windows(AS_OF), ["catboost"], seed=42)
    assert res["metrics"]["catboost"]["mape_pct"] < 40


def test_tree_baseline_importance_and_is_fact_in_predictions(frame):
    res = fit_and_score(frame, make_windows(AS_OF), ["hgb", "tree", "ridge"], seed=42)
    imp = res["metrics"]["tree"]["feature_importance"]
    assert imp and abs(sum(imp.values()) - 1.0) < 1e-3 and "distance" in imp       # дерево объясняет вклад признаков
    assert res["metrics"]["ridge"]["feature_importance"] is None
    assert res["metrics"]["tree"]["params"]["max_depth"] == 12
    pred = res["predictions"]
    assert pred["is_fact"].dtype == bool
    assert pred.loc[pred["split"] == "test", "is_fact"].all()                    # test-прогнозы только по фактам
    assert (~pred.loc[pred["split"] == "future", "is_fact"]).any()               # ставки торгов в плановых заявках есть
