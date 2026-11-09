-- DDL аналитических таблиц ClickHouse (extra). Блоки по маркеру `-- @@ <имя таблицы>`; {table} подставляет загрузчик.

-- @@ mart_route_card
-- Карточка маршрута в ClickHouse. {table} подставляется загрузчиком (etl/clickhouse_sync.py).
-- ReplacingMergeTree по ключу grain: повторная вставка тех же ключей схлопывается при слиянии,
-- а читать для точных чисел до слияния нужно с FINAL. Партиция по году месяца карточки.
CREATE TABLE IF NOT EXISTS {table}
(
    price_month         Date,
    loading_city_gcid   Int32,
    unloading_city_gcid Int32,
    body_group          LowCardinality(String),
    weight_category     LowCardinality(String),
    loading_city        String,
    unloading_city      String,
    trips_count         UInt32,
    carriers_count      UInt32,
    p25_trip            Nullable(Float64),
    median_trip         Nullable(Float64),
    p75_trip            Nullable(Float64),
    avg_trip            Nullable(Float64),
    p25_km              Nullable(Float64),
    median_km           Nullable(Float64),
    p75_km              Nullable(Float64),
    cv_rate             Nullable(Float64),
    mom_pct             Nullable(Float64),
    yoy_pct             Nullable(Float64),
    mom_km_pct          Nullable(Float64),
    yoy_km_pct          Nullable(Float64),
    last_price_date     Date,
    reliability_level   LowCardinality(String),
    as_of               Date,
    refreshed_at        DateTime('UTC')
)
ENGINE = ReplacingMergeTree(refreshed_at)
PARTITION BY toYear(price_month)
ORDER BY (loading_city_gcid, unloading_city_gcid, body_group, weight_category, price_month)

-- @@ ml_price_predictions
-- Факт vs Прогноз в ClickHouse. {table} подставляется загрузчиком.
CREATE TABLE IF NOT EXISTS {table}
(
    price_id            Int64,
    model_name          LowCardinality(String),
    split               LowCardinality(String),
    loading_date        Date,
    loading_city_gcid   Int32,
    unloading_city_gcid Int32,
    body_group          LowCardinality(String),
    actual_price_rub    Nullable(Float64),
    predicted_price_rub Float64,
    abs_error_rub       Nullable(Float64),
    ape_pct             Nullable(Float64),
    outlier_mark        UInt8,
    is_fact             UInt8,
    as_of               Date,
    scored_at           DateTime('UTC')
)
ENGINE = ReplacingMergeTree(scored_at)
PARTITION BY toYYYYMM(loading_date)
ORDER BY (model_name, price_id)
