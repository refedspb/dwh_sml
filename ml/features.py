"""Подготовка данных ML: признаки без утечек, периодическое кодирование дат, защита от запрещённых колонок
и хронологическое разбиение (границы считаются от as_of, системная дата не используется)."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

CAT_FEATURES = [
    "origin_region",
    "destination_region",
    "body_group",
    "weight_category",
    "volume_category",
]
NUM_FEATURES = [
    "distance",
    "total_weight",
    "total_volume",
    "origin_lat",
    "origin_lon",
    "dest_lat",
    "dest_lon",
    "year",
    "month_sin",
    "month_cos",
    "dow_sin",
    "dow_cos",
    "doy_sin",
    "doy_cos",
]
FEATURES = CAT_FEATURES + NUM_FEATURES
TARGET = "actual_price_rub"

# Периоды цикла: месяц 12, день недели 7, день года 365.25
PERIODS = {"month": 12.0, "dow": 7.0, "doy": 365.25}

# Прямые и косвенные производные таргета, а также групповые агрегаты price_group_stats.
FORBIDDEN_FEATURES = frozenset(
    {
        "price", "price_rub", "price_orig", "actual_price_rub", "price_with_vat",
        "price_per_km", "price_per_ton", "price_km", "price_group_id", "avg_price",
        "min_km_price", "max_km_price", "carrier_cost", "transportation_cost", "customer_cost",
        "profit", "outlier_mark",
    }
)
_FORBIDDEN_PATTERN = re.compile(r"price|profit|cost|outlier|margin|vat", re.IGNORECASE)


def assert_no_leakage(columns) -> None:
    """Падаем, если в признаковом пространстве оказалась производная таргета или price_group_stats."""
    bad = sorted(c for c in columns if c in FORBIDDEN_FEATURES or _FORBIDDEN_PATTERN.search(c))
    if bad:
        raise ValueError(f"Утечка данных: запрещённые признаки {bad}")


def cyclical(values, period: float) -> tuple[np.ndarray, np.ndarray]:
    """sin/cos(2*pi*t/T): соседние точки на стыке периодов остаются близкими."""
    angle = 2.0 * np.pi * np.asarray(values, dtype=float) / period
    return np.sin(angle), np.cos(angle)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Строит матрицу признаков из выборки core. Таргет и служебные колонки в неё не попадают."""
    dates = pd.to_datetime(df["loading_date"])
    out = pd.DataFrame(index=df.index)

    for col in CAT_FEATURES:
        # dtype=object явно: CatBoost не принимает строковые расширенные dtype из pandas 3
        out[col] = df[col].astype("object").where(df[col].notna(), "NA").astype(str).astype(object)
    for col in ["distance", "total_weight", "total_volume", "origin_lat", "origin_lon", "dest_lat", "dest_lon"]:
        out[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    out["year"] = dates.dt.year.astype(float)
    out["month_sin"], out["month_cos"] = cyclical(dates.dt.month, PERIODS["month"])
    out["dow_sin"], out["dow_cos"] = cyclical(dates.dt.dayofweek, PERIODS["dow"])
    out["doy_sin"], out["doy_cos"] = cyclical(dates.dt.dayofyear, PERIODS["doy"])

    out = out[FEATURES]
    assert_no_leakage(out.columns)
    return out


# ----------------------------------------------------------------------------- хронологическое разбиение
@dataclass(frozen=True)
class TimeWindows:
    as_of: date
    train_start: date   # as_of - 3 месяца, включительно
    val_start: date     # последние 14 дней train: ранняя остановка бустинга
    test_start: date    # as_of - 3 недели: входит ТОЛЬКО в test

    def describe(self) -> str:
        return (
            f"train=[{self.train_start}, {self.test_start}) "
            f"val(в train)=[{self.val_start}, {self.test_start}) "
            f"test=[{self.test_start}, {self.as_of}] future=({self.as_of}, +inf)"
        )


def make_windows(as_of: date) -> TimeWindows:
    train_start = (pd.Timestamp(as_of) - pd.DateOffset(months=3)).date()
    test_start = as_of - timedelta(weeks=3)
    val_start = test_start - timedelta(days=14)
    return TimeWindows(as_of=as_of, train_start=train_start, val_start=val_start, test_start=test_start)


def split_masks(loading_date: pd.Series, w: TimeWindows) -> dict[str, pd.Series]:
    d = pd.to_datetime(loading_date).dt.date
    ts, vs, es, ao = w.train_start, w.val_start, w.test_start, w.as_of
    return {
        "train": (d >= ts) & (d < es),
        "fit": (d >= ts) & (d < vs),
        "val": (d >= vs) & (d < es),
        "test": (d >= es) & (d <= ao),
        "future": d > ao,
    }
