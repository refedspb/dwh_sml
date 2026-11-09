"""Модели, обучение (хронологический split) и пакетный скоринг тарифов.

Запуск:  python -m ml.train_score
Читает core.*, пишет mart.ml_price_predictions и mart.ml_model_metrics, артефакты artifacts/metrics.json и модель.
Повторный запуск перезаписывает прогнозы модели целиком (DELETE + COPY в одной транзакции).

Что скорится: очищенные фактические сделки test-периода и ВСЕ плановые заявки после as_of (независимо от is_fact:
факта у них нет, колонка is_fact в прогнозах позволяет отфильтровать ставки торгов).
"""
from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from etl.db import (ARTIFACTS_DIR, PipelineBusy, Settings, connect, copy_rows, load_settings, pipeline_lock,
                    sql_block)
from ml.features import (CAT_FEATURES, NUM_FEATURES, TARGET, TimeWindows, build_features,
                         make_windows, split_masks)

log = logging.getLogger("ml.train_score")

PREDICTION_COLUMNS = [
    "price_id", "model_name", "split", "loading_date", "loading_city_gcid", "unloading_city_gcid",
    "body_group", "actual_price_rub", "predicted_price_rub", "abs_error_rub", "ape_pct",
    "outlier_mark", "is_fact", "as_of",
]


# ----------------------------------------------------------------------------- метрики
def regression_metrics(y_true, y_pred) -> dict[str, float]:
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(y_pred, dtype=float)
    err = y - p
    ss_res = float(np.sum(err**2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    return {
        "r2": 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan"),
        "mape_pct": float(np.mean(np.abs(err) / y) * 100.0),
        "rmse_rub": float(np.sqrt(np.mean(err**2))),
        "mae_rub": float(np.mean(np.abs(err))),
    }


# ----------------------------------------------------------------------------- модели
class CatBoostModel:
    """Нативная обработка категорий, без масштабирования. Ранняя остановка по хронологически последним дням train."""

    name = "catboost"

    def __init__(self, seed: int):
        self.seed = seed
        self.model = None
        self.params: dict[str, Any] = {
            "loss_function": "RMSE", "iterations": 2000, "learning_rate": 0.08, "depth": 8,
            "random_seed": seed, "od_type": "Iter", "od_wait": 100,
        }

    def fit(self, X_fit: pd.DataFrame, y_fit, X_val: pd.DataFrame, y_val) -> "CatBoostModel":
        from catboost import CatBoostRegressor, Pool  # импорт здесь: тяжёлая зависимость

        self.model = CatBoostRegressor(**self.params, allow_writing_files=False, verbose=200)
        self.model.fit(
            Pool(X_fit, y_fit, cat_features=CAT_FEATURES),
            eval_set=Pool(X_val, y_val, cat_features=CAT_FEATURES),
            use_best_model=True,
        )
        self.params["best_iteration"] = int(self.model.get_best_iteration())
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return np.asarray(self.model.predict(X), dtype=float)

    def feature_importance(self) -> dict[str, float]:
        pairs = sorted(zip(self.model.feature_names_, self.model.get_feature_importance()), key=lambda kv: -kv[1])
        return {k: round(float(v), 3) for k, v in pairs}


class _SklearnModel:
    name = "sklearn"

    def __init__(self, seed: int):
        self.seed = seed
        self.pipe = None
        self.params: dict[str, Any] = {"random_state": seed}

    def _build(self):
        raise NotImplementedError

    def fit(self, X_fit: pd.DataFrame, y_fit, X_val=None, y_val=None):
        self.pipe = self._build()
        # Все трансформеры (импутер, энкодер, скейлер) обучаются ТОЛЬКО на train-части.
        self.pipe.fit(X_fit, np.asarray(y_fit, dtype=float))
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        return np.asarray(self.pipe.predict(X), dtype=float)

    def feature_importance(self) -> dict[str, float] | None:
        return None


class RidgeModel(_SklearnModel):
    """Линейный бейзлайн: One-Hot + импутация + масштабирование."""

    name = "ridge"

    def _build(self):
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler

        self.params.update(alpha=1.0)
        pre = ColumnTransformer([
            ("cat", OneHotEncoder(handle_unknown="ignore"), CAT_FEATURES),
            ("num", Pipeline([("imp", SimpleImputer(strategy="median")), ("sc", StandardScaler())]), NUM_FEATURES),
        ])
        return Pipeline([("pre", pre), ("reg", Ridge(alpha=1.0, random_state=self.seed))])


class TreeModel(_SklearnModel):
    """Одиночное дерево решений: второй бейзлайн из R&D-отчёта (категории кодируются порядковыми номерами)."""

    name = "tree"

    def _build(self):
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OrdinalEncoder
        from sklearn.tree import DecisionTreeRegressor

        self.params.update(max_depth=12, min_samples_leaf=15)
        pre = ColumnTransformer([
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), CAT_FEATURES),
            ("num", SimpleImputer(strategy="median"), NUM_FEATURES),
        ])
        reg = DecisionTreeRegressor(max_depth=12, min_samples_leaf=15, random_state=self.seed)
        return Pipeline([("pre", pre), ("reg", reg)])

    def feature_importance(self) -> dict[str, float]:
        values = self.pipe.named_steps["reg"].feature_importances_        # порядок: категории, затем числа
        pairs = sorted(zip(CAT_FEATURES + NUM_FEATURES, values), key=lambda kv: -kv[1])
        return {k: round(float(v), 4) for k, v in pairs}


class HistGBModel(_SklearnModel):
    """Градиентный бустинг sklearn: запасной вариант без CatBoost (используется в офлайн-тестах)."""

    name = "hgb"

    def _build(self):
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import HistGradientBoostingRegressor
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OrdinalEncoder

        pre = ColumnTransformer([
            ("cat", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=np.nan), CAT_FEATURES),
            ("num", "passthrough", NUM_FEATURES),
        ])
        mask = [True] * len(CAT_FEATURES) + [False] * len(NUM_FEATURES)
        reg = HistGradientBoostingRegressor(categorical_features=mask, max_iter=400, learning_rate=0.08,
                                            random_state=self.seed)
        return Pipeline([("pre", pre), ("reg", reg)])


REGISTRY = {"catboost": CatBoostModel, "ridge": RidgeModel, "tree": TreeModel, "hgb": HistGBModel}


def make_model(name: str, seed: int):
    try:
        return REGISTRY[name](seed)
    except KeyError as exc:
        raise ValueError(f"Неизвестная модель {name!r}; доступно: {sorted(REGISTRY)}") from exc


# ----------------------------------------------------------------------------- обучение и скоринг (без БД)
def fit_and_score(df: pd.DataFrame, windows: TimeWindows, model_names: list[str], seed: int) -> dict[str, Any]:
    """Обучает модели на train, считает метрики на test, скорит test + плановые заявки основной моделью."""
    df = df.reset_index(drop=True)
    masks = split_masks(df["loading_date"], windows)
    clean = ~df["outlier_mark"].astype(bool)
    is_fact = df["is_fact"].astype(bool)
    # Обучение и оценка: только фактические сделки без размеченных выбросов.
    # Плановые заявки (после as_of) скорятся независимо от is_fact и outlier_mark: факта у них нет.
    eligible = clean & is_fact

    fit_idx = masks["fit"] & eligible
    val_idx = masks["val"] & eligible
    test_eval_idx = masks["test"] & eligible
    for name, idx in (("fit", fit_idx), ("val", val_idx), ("test", test_eval_idx)):
        if int(idx.sum()) == 0:
            raise RuntimeError(f"Пустая выборка {name} для окон {windows.describe()}")

    X = build_features(df)
    y = df[TARGET].astype(float)

    as_of_day = pd.to_datetime(df["loading_date"]).dt.date == windows.as_of
    counts = {
        "train_rows_clean": int((masks["train"] & eligible).sum()),
        "fit_rows": int(fit_idx.sum()),
        "val_rows": int(val_idx.sum()),
        "test_rows_clean": int(test_eval_idx.sum()),
        "test_rows_all": int((masks["test"] & is_fact).sum()),
        # верхняя граница доли test, затронутой неразмеченными выбросами последнего дня (сами выбросы неизвестны)
        "test_rows_on_as_of_day": int((test_eval_idx & as_of_day).sum()),
        "future_rows": int(masks["future"].sum()),
        "future_rows_fact": int((masks["future"] & is_fact).sum()),
    }
    log.info("windows: %s", windows.describe())
    log.info("rows: %s", counts)

    metrics: dict[str, dict[str, Any]] = {}
    models = {}
    for name in model_names:
        model = make_model(name, seed)
        model.fit(X[fit_idx], y[fit_idx], X[val_idx], y[val_idx])
        pred = np.maximum(model.predict(X[test_eval_idx]), 0.0)
        m = regression_metrics(y[test_eval_idx], pred)
        m.update(n_train=counts["train_rows_clean"], n_test=counts["test_rows_clean"],
                 params=model.params, feature_importance=model.feature_importance())
        metrics[name] = m
        models[name] = model
        log.info("model=%s test R2=%.4f MAPE=%.2f%% RMSE=%.0f MAE=%.0f",
                 name, m["r2"], m["mape_pct"], m["rmse_rub"], m["mae_rub"])

    primary = model_names[0]
    score_idx = test_eval_idx | masks["future"]
    scored = df.loc[score_idx].copy()
    scored["predicted_price_rub"] = np.maximum(models[primary].predict(X[score_idx]), 0.0)
    scored["split"] = np.where(test_eval_idx[score_idx], "test", "future")
    is_test = scored["split"] == "test"
    # Для плановых заявок факт и ошибки всегда NULL, даже если price в источнике заполнена.
    scored["actual_price_rub"] = scored[TARGET].where(is_test)
    scored["abs_error_rub"] = (scored["actual_price_rub"] - scored["predicted_price_rub"]).abs()
    scored["ape_pct"] = scored["abs_error_rub"] / scored["actual_price_rub"] * 100.0
    scored["is_fact"] = scored["is_fact"].astype(bool)
    scored["model_name"] = primary
    scored["as_of"] = windows.as_of
    predictions = scored[PREDICTION_COLUMNS].reset_index(drop=True)

    return {"metrics": metrics, "predictions": predictions, "counts": counts, "primary": primary, "models": models}


# ----------------------------------------------------------------------------- работа с БД и артефактами
def load_frame(settings: Settings, train_start: date) -> pd.DataFrame:
    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute(sql_block("ml_extract"), {"train_start": train_start})
        df = pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    df["loading_date"] = pd.to_datetime(df["loading_date"])
    log.info("loaded rows=%d from core", len(df))
    return df


def _native(v: Any) -> Any:
    if v is None or v is pd.NaT or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.date()
    if isinstance(v, np.generic):
        return v.item()
    return v


UPSERT_METRICS = (
    "INSERT INTO mart.ml_model_metrics (model_name, as_of, r2, mape_pct, rmse_rub, mae_rub, n_train, n_test, "
    "train_start, test_start, params, feature_importance) "
    "VALUES (%(name)s, %(as_of)s::date, %(r2)s, %(mape)s, %(rmse)s, %(mae)s, %(ntr)s, %(nte)s, "
    "%(ts)s::date, %(es)s::date, %(params)s::jsonb, %(imp)s::jsonb) "
    "ON CONFLICT (model_name, as_of) DO UPDATE SET r2 = EXCLUDED.r2, mape_pct = EXCLUDED.mape_pct, "
    "rmse_rub = EXCLUDED.rmse_rub, mae_rub = EXCLUDED.mae_rub, n_train = EXCLUDED.n_train, "
    "n_test = EXCLUDED.n_test, train_start = EXCLUDED.train_start, test_start = EXCLUDED.test_start, "
    "params = EXCLUDED.params, feature_importance = EXCLUDED.feature_importance, trained_at = now()"
)


def save_results(settings: Settings, result: dict[str, Any], windows: TimeWindows) -> None:
    preds: pd.DataFrame = result["predictions"].copy()
    preds["loading_date"] = pd.to_datetime(preds["loading_date"]).dt.date
    rows = ([_native(v) for v in rec] for rec in preds.itertuples(index=False, name=None))

    with connect(settings) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM mart.ml_price_predictions WHERE model_name = %(m)s", {"m": result["primary"]})
        n = copy_rows(cur, "mart.ml_price_predictions", PREDICTION_COLUMNS, rows)
        for name, m in result["metrics"].items():
            cur.execute(UPSERT_METRICS, {
                "name": name, "as_of": windows.as_of, "r2": m["r2"], "mape": m["mape_pct"], "rmse": m["rmse_rub"],
                "mae": m["mae_rub"], "ntr": m["n_train"], "nte": m["n_test"],
                "ts": windows.train_start, "es": windows.test_start,
                "params": json.dumps(m["params"], default=str),
                "imp": json.dumps(m["feature_importance"]) if m["feature_importance"] else None,
            })
    log.info("predictions saved rows=%d model=%s", n, result["primary"])


def write_artifacts(result: dict[str, Any], windows: TimeWindows) -> None:
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of": str(windows.as_of),
        "windows": {k: str(v) for k, v in asdict(windows).items()},
        "primary_model": result["primary"],
        "counts": result["counts"],
        "metrics_on_test": result["metrics"],
    }
    path = ARTIFACTS_DIR / "metrics.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    log.info("metrics artifact written path=%s", path)

    inner = getattr(result["models"][result["primary"]], "model", None)
    if inner is not None and hasattr(inner, "save_model"):       # нативный файл CatBoost для воспроизводимости
        models_dir = ARTIFACTS_DIR / "models"
        models_dir.mkdir(parents=True, exist_ok=True)
        inner.save_model(str(models_dir / f"{result['primary']}_{windows.as_of}.cbm"))


def run(settings: Settings) -> dict[str, Any]:
    windows = make_windows(settings.as_of)
    df = load_frame(settings, windows.train_start)
    result = fit_and_score(df, windows, list(settings.ml_models), settings.random_seed)
    save_results(settings, result, windows)
    write_artifacts(result, windows)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = load_settings()
    try:
        with pipeline_lock(settings):
            run(settings)
    except PipelineBusy as exc:
        log.error("%s", exc)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
