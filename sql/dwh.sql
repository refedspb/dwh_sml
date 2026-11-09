-- Все DDL и трансформации DWH в одном файле. Файл делится на именованные блоки маркером `-- @@ <имя>`;
-- Python (etl.db.sql_block) отдаёт блок целиком, поэтому разбиения по точкам с запятой нет.
--   schemas, ddl_core, ddl_mart, ddl_ml : DDL (идемпотентно, CREATE ... IF NOT EXISTS)
--   core_dims, core_stage, core_load    : загрузка Core (справочники, классификация DQ, перезапись окна)
--   ml_extract                          : выборка для ML
--   checks                              : примеры проверочных запросов для psql (не выполняются пайплайном)
-- Мартовая витрина маршрута лежит отдельно: sql/route_card.sql.
-- ВАЖНО: в блоках, которые выполняются с параметрами (core_stage, core_load, ml_extract), нельзя использовать
-- символ процента вне плейсхолдеров: подстановка параметров идёт на клиенте.

-- @@ schemas
-- Слои хранилища. Источник (операционная БД) остаётся в схеме public и не изменяется.
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

-- @@ ddl_core
-- Core DWH: очищенные факты и дедуплицированные справочники.

-- Города: gc_id в источнике не уникален, берём запись с минимальным cities.id.
CREATE TABLE IF NOT EXISTS core.dim_city (
    gc_id           integer PRIMARY KEY,
    city_id         bigint           NOT NULL,
    city_name       varchar(255)     NOT NULL,
    full_name_local varchar(255)     NOT NULL,
    region          varchar(255),
    latitude        double precision,
    longitude       double precision
);

-- Кузова: код 1С -> стандартная бизнес-группа.
CREATE TABLE IF NOT EXISTS core.dim_body_type (
    body_type_id integer PRIMARY KEY,
    std_id       integer,
    body_group   varchar(32) NOT NULL
);

-- Очищенные сделки. Содержит все валидные строки (в т.ч. is_fact = false, complex, outlier_mark),
-- фильтры рыночной выборки применяются в витринах и ML, а не здесь.
CREATE TABLE IF NOT EXISTS core.fact_price (
    price_id            bigint PRIMARY KEY,
    created_at          timestamp(0),
    loading_date        date             NOT NULL,
    end_loading_date    date,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    distance            double precision NOT NULL,
    total_weight        double precision NOT NULL,
    total_volume        double precision NOT NULL,
    body_type           integer,
    currency_code       varchar(3)       NOT NULL,
    price_orig          double precision NOT NULL,
    fx_rate             double precision NOT NULL,
    nearest_rate_date   date,
    price_rub           double precision NOT NULL,
    price_type          smallint         NOT NULL,
    organization_id     integer,
    complex             boolean          NOT NULL,
    is_fact             boolean          NOT NULL,
    outlier_mark        boolean          NOT NULL,
    weight_category     varchar(4)       NOT NULL,
    volume_category     varchar(4)       NOT NULL,
    loaded_at           timestamptz      NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fact_price_loading_date_idx ON core.fact_price (loading_date);

-- Одна цена x одна группа кузова (без дублей внутри группы).
CREATE TABLE IF NOT EXISTS core.fact_price_body_group (
    price_id   bigint      NOT NULL REFERENCES core.fact_price (price_id) ON DELETE CASCADE,
    body_group varchar(32) NOT NULL,
    PRIMARY KEY (price_id, body_group)
);

-- Развёртка по датам из price_dates: источник месячной строки витрины.
CREATE TABLE IF NOT EXISTS core.fact_price_date (
    price_id   bigint NOT NULL REFERENCES core.fact_price (price_id) ON DELETE CASCADE,
    price_date date   NOT NULL,
    PRIMARY KEY (price_id, price_date)
);

-- Карантин: одна строка на пару (сделка, причина).
CREATE TABLE IF NOT EXISTS core.dq_quarantine_prices (
    price_id          bigint       NOT NULL,
    reason_code       varchar(64)  NOT NULL,
    loading_date      date         NOT NULL,
    currency_code     varchar(3),
    price             double precision,
    distance          double precision,
    total_weight      double precision,
    total_volume      double precision,
    nearest_rate_date date,
    quarantined_at    timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (price_id, reason_code)
);
CREATE INDEX IF NOT EXISTS dq_quarantine_loading_date_idx ON core.dq_quarantine_prices (loading_date);

-- Журнал запусков ETL.
CREATE TABLE IF NOT EXISTS core.etl_run_log (
    run_id           bigserial PRIMARY KEY,
    step             varchar(64) NOT NULL,
    window_start     date,
    window_end       date,
    rows_read        bigint,
    rows_loaded      bigint,
    rows_quarantined bigint,
    details          jsonb,
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz NOT NULL
);

-- @@ ddl_mart
-- Витрина карточки маршрута и рыночного коридора.
-- Grain: price_month x loading_city_gcid x unloading_city_gcid x body_group x weight_category.
CREATE TABLE IF NOT EXISTS mart.mart_route_card (
    price_month         date         NOT NULL,
    loading_city_gcid   integer      NOT NULL,
    unloading_city_gcid integer      NOT NULL,
    body_group          varchar(32)  NOT NULL,
    weight_category     varchar(4)   NOT NULL,
    loading_city        varchar(255) NOT NULL,
    unloading_city      varchar(255) NOT NULL,
    trips_count         integer      NOT NULL,
    carriers_count      integer      NOT NULL,
    p25_trip            double precision,
    median_trip         double precision,
    p75_trip            double precision,
    avg_trip            double precision,
    p25_km              double precision,
    median_km           double precision,
    p75_km              double precision,
    cv_rate             double precision,
    mom_pct             double precision,
    yoy_pct             double precision,
    mom_km_pct          double precision,
    yoy_km_pct          double precision,
    last_price_date     date         NOT NULL,
    reliability_level   varchar(16)  NOT NULL,
    as_of               date         NOT NULL,
    refreshed_at        timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category)
);
CREATE INDEX IF NOT EXISTS mart_route_card_route_idx
    ON mart.mart_route_card (loading_city_gcid, unloading_city_gcid, body_group, weight_category, price_month);

-- Режим «сводная таблица маршрутной сети»: последний доступный месяц по каждой ячейке.
CREATE OR REPLACE VIEW mart.v_route_latest AS
SELECT DISTINCT ON (loading_city_gcid, unloading_city_gcid, body_group, weight_category) *
FROM mart.mart_route_card
ORDER BY loading_city_gcid, unloading_city_gcid, body_group, weight_category, price_month DESC;

-- @@ ddl_ml
-- Прогнозы: Факт vs Прогноз.
CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id            bigint           NOT NULL,
    model_name          varchar(64)      NOT NULL,
    split               varchar(8)       NOT NULL CHECK (split IN ('test', 'future')),
    loading_date        date             NOT NULL,
    loading_city_gcid   integer          NOT NULL,
    unloading_city_gcid integer          NOT NULL,
    body_group          varchar(128)     NOT NULL,
    actual_price_rub    double precision,
    predicted_price_rub double precision NOT NULL,
    abs_error_rub       double precision,
    ape_pct             double precision,
    outlier_mark        boolean          NOT NULL,
    as_of               date             NOT NULL,
    scored_at           timestamptz      NOT NULL DEFAULT now(),
    is_fact             boolean          NOT NULL DEFAULT true,
    PRIMARY KEY (price_id, model_name)
);
-- Для томов, созданных прежней версией DDL.
ALTER TABLE mart.ml_price_predictions ADD COLUMN IF NOT EXISTS is_fact boolean NOT NULL DEFAULT true;

-- Гарантия на уровне БД: у плановых заявок нет факта и ошибок, что бы ни записал скоринг.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ml_predictions_future_has_no_actual') THEN
        ALTER TABLE mart.ml_price_predictions
            ADD CONSTRAINT ml_predictions_future_has_no_actual
            CHECK (split = 'test' OR (actual_price_rub IS NULL AND abs_error_rub IS NULL AND ape_pct IS NULL));
    END IF;
END
$$;
CREATE INDEX IF NOT EXISTS ml_predictions_date_idx ON mart.ml_price_predictions (loading_date);

-- Метрики на отложенной выборке.
CREATE TABLE IF NOT EXISTS mart.ml_model_metrics (
    model_name  varchar(64)      NOT NULL,
    as_of       date             NOT NULL,
    r2          double precision,
    mape_pct    double precision,
    rmse_rub    double precision,
    mae_rub     double precision,
    n_train     integer,
    n_test      integer,
    train_start date,
    test_start  date,
    trained_at  timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (model_name, as_of)
);
-- Гиперпараметры и важность признаков (воспроизводимость и интерпретация).
ALTER TABLE mart.ml_model_metrics
    ADD COLUMN IF NOT EXISTS params             jsonb,
    ADD COLUMN IF NOT EXISTS feature_importance jsonb;

-- Для томов, созданных прежней версией DDL (там было varchar(32)).
ALTER TABLE mart.ml_price_predictions ALTER COLUMN body_group TYPE varchar(128);

-- @@ core_dims
-- Обновление справочников Core. Идемпотентно (UPSERT). Параметров нет.
-- ВАЖНО: в SQL-файлах, выполняемых с параметрами, нельзя использовать символ процента вне плейсхолдеров.

-- Города: одна строка на gc_id (минимальный cities.id).
-- Регион: часть full_name_local после первой запятой, например 'Казань, Республика Татарстан' -> 'Республика Татарстан'.
INSERT INTO core.dim_city (gc_id, city_id, city_name, full_name_local, region, latitude, longitude)
SELECT DISTINCT ON (c.gc_id)
       c.gc_id,
       c.id,
       c.name,
       c.full_name_local,
       CASE WHEN strpos(c.full_name_local, ',') > 0
            THEN NULLIF(btrim(substr(c.full_name_local, strpos(c.full_name_local, ',') + 1)), '')
       END,
       c.latitude,
       c.longitude
FROM public.cities c
ORDER BY c.gc_id, c.id
ON CONFLICT (gc_id) DO UPDATE
SET city_id         = EXCLUDED.city_id,
    city_name       = EXCLUDED.city_name,
    full_name_local = EXCLUDED.full_name_local,
    region          = EXCLUDED.region,
    latitude        = EXCLUDED.latitude,
    longitude       = EXCLUDED.longitude;

-- Кузова: стандартная группа по std_id.
INSERT INTO core.dim_body_type (body_type_id, std_id, body_group)
SELECT b.id,
       b.std_id,
       CASE b.std_id
            WHEN 56 THEN 'Тент'
            WHEN 4  THEN 'Рефрижератор'
            WHEN 2  THEN 'Изотерм'
            WHEN 15 THEN 'Борт'
            ELSE 'Прочие'
       END
FROM public.body_types b
ON CONFLICT (body_type_id) DO UPDATE
SET std_id     = EXCLUDED.std_id,
    body_group = EXCLUDED.body_group;

-- @@ core_stage
-- Шаг 1 загрузки Core: классификация строк источника по правилам DQ в окне loading_date.
-- Параметры: start_date, end_date (оба могут быть NULL = без ограничения).
-- Таблица живёт до конца транзакции; из неё питаются шаги 22 (карантин и факт) и статистика в логе.
-- ВАЖНО: в этом файле нельзя использовать символ процента вне плейсхолдеров.

DROP TABLE IF EXISTS stg_price;

CREATE TEMP TABLE stg_price ON COMMIT DROP AS
SELECT
    p.id                  AS price_id,
    p.created_at,
    p.loading_date,
    p.end_loading_date,
    p.loading_city_gcid,
    p.unloading_city_gcid,
    p.distance,
    p.total_weight,
    p.total_volume,
    p.body_type,
    p.currency_code,
    p.price               AS price_orig,
    p.nearest_rate_date,
    -- RUB: курс 1. Иностранная валюта: ТОЧНЫЙ курс на nearest_rate_date, без fallback на loading_date.
    CASE WHEN p.currency_code = '643' THEN 1.0::float8 ELSE fx.value END AS fx_rate,
    p.price_type,
    p.organization_id,
    p.complex,
    p.is_fact,
    p.outlier_mark,
    NULLIF(btrim(p.weight_category), '') AS src_weight_category,
    NULLIF(btrim(p.volume_category), '') AS src_volume_category,
    ARRAY_REMOVE(ARRAY[
        -- NaN в PostgreSQL больше любого числа и проходит проверки вида price > 0, поэтому ловим отдельно
        CASE WHEN p.price          IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
               OR p.distance       IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
               OR p.total_weight   IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
               OR p.total_volume   IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)
             THEN 'NON_FINITE_VALUE'::text END,
        CASE WHEN p.price <= 0
             THEN 'PRICE_NON_POSITIVE'::text END,
        -- source-validation: рублёвые ставки до 1000 включительно считаются битыми
        CASE WHEN p.currency_code = '643' AND p.price > 0 AND p.price <= 1000
             THEN 'RUB_PRICE_LE_1000'::text END,
        CASE WHEN p.distance <= 0
             THEN 'DISTANCE_NON_POSITIVE'::text END,
        CASE WHEN p.total_weight < 0
             THEN 'NEGATIVE_WEIGHT'::text END,
        CASE WHEN p.total_volume < 0
             THEN 'NEGATIVE_VOLUME'::text END,
        CASE WHEN lc.gc_id IS NULL
             THEN 'LOADING_CITY_NOT_FOUND'::text END,
        CASE WHEN uc.gc_id IS NULL
             THEN 'UNLOADING_CITY_NOT_FOUND'::text END,
        CASE WHEN p.currency_code NOT IN ('643', '840', '978')
             THEN 'UNSUPPORTED_CURRENCY'::text END,
        CASE WHEN p.currency_code IN ('840', '978') AND p.nearest_rate_date IS NULL
             THEN 'FX_RATE_DATE_MISSING'::text END,
        CASE WHEN p.currency_code IN ('840', '978') AND p.nearest_rate_date IS NOT NULL
                  AND (fx.value IS NULL OR fx.value <= 0 OR fx.value >= 'Infinity'::float8)
             THEN 'FX_RATE_NOT_FOUND'::text END
    ], NULL) AS reasons
FROM public.prices p
LEFT JOIN core.dim_city lc ON lc.gc_id = p.loading_city_gcid
LEFT JOIN core.dim_city uc ON uc.gc_id = p.unloading_city_gcid
LEFT JOIN (
    -- на случай дублей курса на одну дату берём последнюю вставленную запись
    SELECT DISTINCT ON (num_code, setting_date) num_code, setting_date, value
    FROM public.exchange_rates
    ORDER BY num_code, setting_date, id DESC
) fx ON fx.num_code = p.currency_code AND fx.setting_date = p.nearest_rate_date
WHERE p.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date)
                         AND COALESCE(%(end_date)s::date, 'infinity'::date);

ANALYZE stg_price;

-- @@ core_load
-- Шаг 2 загрузки Core: атомарная перезапись окна loading_date (DELETE + INSERT в одной транзакции).
-- Повторный запуск за то же окно даёт тот же результат: ничего не задваивается.
-- Параметры: start_date, end_date. Требует stg_price из шага 21 в той же транзакции.
-- ВАЖНО: в этом файле нельзя использовать символ процента вне плейсхолдеров.

-- 1. Удаляем прежний результат окна (дети fact_price удаляются каскадом).
--    Помимо самого окна чистим и старые версии сделок, которые теперь пришли в окно с другой датой погрузки,
--    иначе после правки loading_date в источнике в Core осталась бы устаревшая копия вне окна.
DELETE FROM core.dq_quarantine_prices q
WHERE q.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date)
                         AND COALESCE(%(end_date)s::date, 'infinity'::date)
   OR q.price_id IN (SELECT s.price_id FROM stg_price s);

DELETE FROM core.fact_price f
WHERE f.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date)
                         AND COALESCE(%(end_date)s::date, 'infinity'::date)
   OR f.price_id IN (SELECT s.price_id FROM stg_price s);

-- 2. Карантин: по строке на каждую нарушенную причину.
INSERT INTO core.dq_quarantine_prices
    (price_id, reason_code, loading_date, currency_code, price, distance,
     total_weight, total_volume, nearest_rate_date)
SELECT s.price_id, r.reason_code, s.loading_date, s.currency_code, s.price_orig, s.distance,
       s.total_weight, s.total_volume, s.nearest_rate_date
FROM stg_price s
CROSS JOIN LATERAL unnest(s.reasons) AS r(reason_code)
WHERE cardinality(s.reasons) > 0;

-- 3. Валидные факты: курс применён, категории дозаполнены.
INSERT INTO core.fact_price
    (price_id, created_at, loading_date, end_loading_date, loading_city_gcid, unloading_city_gcid,
     distance, total_weight, total_volume, body_type, currency_code, price_orig, fx_rate,
     nearest_rate_date, price_rub, price_type, organization_id, complex, is_fact, outlier_mark,
     weight_category, volume_category)
SELECT s.price_id, s.created_at, s.loading_date, s.end_loading_date, s.loading_city_gcid, s.unloading_city_gcid,
       s.distance, s.total_weight, s.total_volume, s.body_type, s.currency_code, s.price_orig, s.fx_rate,
       s.nearest_rate_date,
       ROUND((s.price_orig * s.fx_rate)::numeric, 2)::float8,
       s.price_type, s.organization_id, s.complex, s.is_fact, s.outlier_mark,
       COALESCE(
           s.src_weight_category,
           CASE WHEN ROUND(s.total_weight::numeric, 1) >= 30  THEN 'G20T'
                WHEN ROUND(s.total_weight::numeric, 1) >= 10  THEN 'FTL'
                WHEN ROUND(s.total_weight::numeric, 1) >= 5   THEN '10T'
                WHEN ROUND(s.total_weight::numeric, 1) >= 2.5 THEN '5T'
                WHEN ROUND(s.total_weight::numeric, 1) >= 0.5 THEN 'L5T'
                ELSE 'OTH'
           END),
       COALESCE(
           s.src_volume_category,
           CASE WHEN s.total_volume > 0 AND s.total_volume <= 120 THEN 'FTL' ELSE 'OTH' END)
FROM stg_price s
WHERE cardinality(s.reasons) = 0;

-- 4a. Группы кузова из price_body_types (DISTINCT: одна цена - одна строка на группу).
INSERT INTO core.fact_price_body_group (price_id, body_group)
SELECT DISTINCT f.price_id, COALESCE(db.body_group, 'Прочие')
FROM core.fact_price f
JOIN public.price_body_types pbt ON pbt.price_id = f.price_id
LEFT JOIN core.dim_body_type db ON db.body_type_id = pbt.body_type_id
WHERE f.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date)
                         AND COALESCE(%(end_date)s::date, 'infinity'::date);

-- 4b. Нет связи с price_body_types: fallback на prices.body_type, затем 'Не указан'.
INSERT INTO core.fact_price_body_group (price_id, body_group)
SELECT f.price_id,
       COALESCE(db.body_group,
                CASE WHEN f.body_type IS NULL THEN 'Не указан' ELSE 'Прочие' END)
FROM core.fact_price f
LEFT JOIN core.dim_body_type db ON db.body_type_id = f.body_type
WHERE f.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date)
                         AND COALESCE(%(end_date)s::date, 'infinity'::date)
  AND NOT EXISTS (SELECT 1 FROM core.fact_price_body_group g WHERE g.price_id = f.price_id);

-- 5. Развёртка по датам (источник месячной строки витрины).
INSERT INTO core.fact_price_date (price_id, price_date)
SELECT DISTINCT pd.price_id, pd.price_date
FROM public.price_dates pd
JOIN core.fact_price f ON f.price_id = pd.price_id
WHERE f.loading_date BETWEEN COALESCE(%(start_date)s::date, '-infinity'::date)
                         AND COALESCE(%(end_date)s::date, 'infinity'::date);

-- @@ ml_extract
-- Выборка для ML: одна строка на сделку (прямые маршруты), гео-обогащение по дедуплицированному справочнику городов.
-- Параметр: train_start (нижняя граница по loading_date). Плановые заявки после as_of входят в выборку.
-- Отбор «фактов» и очистку выбросов делает Python (ml/train_score.py): для обучения и метрик нужны is_fact и
-- входной флаг outlier_mark, а плановые заявки скорятся независимо от is_fact (в т.ч. ставки торгов).
-- В выборку НЕ попадают price_with_vat, удельные ставки, маржа и price_group_stats: их нет в Core.
-- Таргет (actual_price_rub) отдаётся отдельной колонкой и в признаки не входит.
-- Группа кузова: у сделки может быть несколько групп, поэтому берём весь набор как одну категорию
-- (сделка не размножается и согласована с витриной, где приоритет у price_body_types).
WITH body_sets AS (
    SELECT price_id,
           array_to_string(array_agg(body_group ORDER BY body_group COLLATE "C"), ' + ') AS body_group
    FROM core.fact_price_body_group
    GROUP BY price_id
)
SELECT f.price_id,
       f.loading_date,
       f.loading_city_gcid,
       f.unloading_city_gcid,
       f.distance,
       f.total_weight,
       f.total_volume,
       f.weight_category,
       f.volume_category,
       COALESCE(b.body_group, 'Не указан')   AS body_group,
       COALESCE(oc.region, 'Не указан')      AS origin_region,
       COALESCE(dc.region, 'Не указан')      AS destination_region,
       oc.latitude  AS origin_lat,
       oc.longitude AS origin_lon,
       dc.latitude  AS dest_lat,
       dc.longitude AS dest_lon,
       f.price_rub  AS actual_price_rub,
       f.is_fact,
       f.outlier_mark
FROM core.fact_price f
JOIN core.dim_city oc ON oc.gc_id = f.loading_city_gcid
JOIN core.dim_city dc ON dc.gc_id = f.unloading_city_gcid
LEFT JOIN body_sets b ON b.price_id = f.price_id
WHERE NOT f.complex
  AND f.price_rub > 0
  AND f.distance > 0
  AND f.loading_date >= %(train_start)s::date
ORDER BY f.loading_date, f.price_id

-- @@ checks
-- Примеры проверочных запросов (выполнять в psql: docker compose exec postgres psql -U postgres -d deals_bigdata).

-- 1. Баланс строк: источник = Core + карантин (по уникальным сделкам).
SELECT (SELECT count(*) FROM public.prices)                                         AS source_rows,
       (SELECT count(*) FROM core.fact_price)                                       AS core_rows,
       (SELECT count(DISTINCT price_id) FROM core.dq_quarantine_prices)             AS quarantined_rows;

-- 2. Отсев по причинам.
SELECT reason_code, count(*) AS rows FROM core.dq_quarantine_prices GROUP BY 1 ORDER BY 2 DESC;

-- 3. Нет дублей зерна витрины (должно вернуть 0 строк).
SELECT price_month, loading_city_gcid, unloading_city_gcid, body_group, weight_category, count(*)
FROM mart.mart_route_card
GROUP BY 1, 2, 3, 4, 5
HAVING count(*) > 1;

-- 4. Карточка маршрута Москва -> Казань, тент, FTL: помесячная история коридора.
SELECT price_month, trips_count, carriers_count,
       round(p25_trip::numeric)    AS p25_trip,
       round(median_trip::numeric) AS median_trip,
       round(p75_trip::numeric)    AS p75_trip,
       round(median_km::numeric, 2) AS median_km,
       round(cv_rate::numeric, 3)  AS cv_rate,
       round(mom_pct::numeric, 1)  AS mom_pct,
       round(yoy_pct::numeric, 1)  AS yoy_pct,
       reliability_level
FROM mart.mart_route_card
WHERE loading_city_gcid = 1001 AND unloading_city_gcid = 1003
  AND body_group = 'Тент' AND weight_category = 'FTL'
ORDER BY price_month DESC
LIMIT 12;

-- 5. Сводная таблица сети: самые плотные направления по последнему месяцу.
SELECT loading_city, unloading_city, body_group, weight_category, price_month,
       trips_count, round(median_trip::numeric) AS median_trip, round(median_km::numeric, 2) AS median_km, reliability_level
FROM mart.v_route_latest
ORDER BY trips_count DESC
LIMIT 20;

-- 6. Распределение надёжности.
SELECT reliability_level, count(*) FROM mart.mart_route_card GROUP BY 1 ORDER BY 1;

-- 7. Факт vs Прогноз: точность по группам кузова на отложенном периоде (без размеченных выбросов).
SELECT body_group, count(*) AS n,
       round(avg(ape_pct)::numeric, 2) AS mape_pct,
       round(avg(abs_error_rub)::numeric) AS mae_rub
FROM mart.ml_price_predictions
WHERE split = 'test' AND NOT outlier_mark
GROUP BY 1 ORDER BY 2 DESC;

-- 8. Рейсы, закрытые существенно выше прогнозного уровня (>30 процентов) на отложенном периоде.
SELECT price_id, loading_date, loading_city_gcid, unloading_city_gcid, body_group,
       actual_price_rub, round(predicted_price_rub::numeric) AS predicted_price_rub, round(ape_pct::numeric, 1) AS ape_pct
FROM mart.ml_price_predictions
WHERE split = 'test' AND actual_price_rub > predicted_price_rub * 1.3
ORDER BY ape_pct DESC
LIMIT 20;

-- 9. Плановые заявки: прогноз есть, факта нет.
SELECT count(*) AS future_rows, count(actual_price_rub) AS future_with_actual_should_be_0
FROM mart.ml_price_predictions WHERE split = 'future';

-- 10. Метрики модели.
SELECT * FROM mart.ml_model_metrics ORDER BY model_name;
